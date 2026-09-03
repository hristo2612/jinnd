//! The string-lane readiness gate (M2-K24; harness FINDINGS #7, #45, #46)
//! — the card's ONE design decision: how a wasm entry's `injects`
//! declaration becomes its fiber's readiness epoch.
//!
//! The typed lane already keeps §3's three promises through the
//! registry's `InjectedReadiness`; a Tier A guest has only the string lane
//! (R3, R7), where `services.resolve` answers from the grant alone and the
//! first CALL meets the provider — or does not. This module gives the
//! declaration the typed lane's semantics without a second registry (R10)
//! and without touching the fiber engine: a [`Gate`] per entry names what
//! it declares; a watcher task recomputes the epoch on the broker's
//! provision edge and the lane's transition edge — never a poll (R1) — and
//! publishes into a [`ReadinessSource`] whose signal is the very
//! [`WatchReadiness`] the lane already hands to `track`. The supervisor's
//! existing epoch planning then does the rest: activate only once every
//! declared provider is `Active` (a), unload → reload as
//! `DependencyChanged` when one is replaced (b), re-arm a `Failed` fiber
//! when one moves and never before (c; ruling 2, 2026-08-25, R9).
//!
//! Epoch identity is by value, in declaration order, exactly the
//! registry's rule: one [`DependencySnapshot`] per declared contract,
//! carrying the providing fiber and the broker generation; a flicker back
//! to the same identity coalesces to no consumer work.

use std::sync::{Arc, Mutex};

use jinnd_api::{
    DependencySnapshot, Epoch, ErrorCode, FiberId, FiberState, Generation, KernelError, Realm,
    ServiceContract, ServiceType,
};
use jinnd_fiber::{ReadinessSignal, ReadinessSource, WatchReadiness};
use tokio::sync::watch;

use crate::broker::Broker;
use crate::broker_state::refusal;
use crate::grants::Grant;

use super::lock;

/// One entry's string-lane dependency declaration (M2-K24): the contracts
/// it injects at activation, in declaration order (part of the epoch's
/// identity), and the elements the decoder could not read as a
/// declaration at all — carried so admission refuses them ON THE RECORD,
/// never dropped silently (R11). A declaration gates; it never grants.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Declaration {
    pub contracts: Vec<String>,
    pub faults: Vec<String>,
}

/// The string lane's one service identity in an epoch: a declared
/// contract is identified by its POSITION in the declaration, the fiber
/// that provides it, and the generation — the name itself rides on the
/// gate, not in the typed snapshot.
struct StringLane;

impl ServiceContract for StringLane {
    type Observation = ();

    const NAME: &'static str = "jinn:services";

    fn observe(&self) {}
}

/// A kernel-supplied provider has no fiber; uids start at 1, so 0 never
/// names a real one.
const KERNEL_PROVIDER: FiberId = FiberId(0);

/// One entry's gate: what it declares, which of those a grant covers by
/// name (what the gate waits on — an ungranted declaration is left to
/// admission to refuse on the record), and what the gate last found
/// unmet, for `jinn:introspect` (Law 2).
pub(crate) struct Gate {
    declared: Mutex<Declaration>,
    gated: Mutex<Vec<String>>,
    unmet: Mutex<Vec<String>>,
    moved: watch::Sender<u64>,
}

impl Gate {
    pub(crate) fn new(declaration: &Declaration, grants: &[Grant]) -> Self {
        Self {
            declared: Mutex::new(declaration.clone()),
            gated: Mutex::new(covered(declaration, grants)),
            unmet: Mutex::new(Vec::new()),
            moved: watch::Sender::new(0),
        }
    }

    /// A config edit restated the entry: the next epoch reads the new
    /// declaration (an edit to `injects` is an epoch input, constitution
    /// 04 §Reconcile).
    pub(crate) fn restate(&self, declaration: &Declaration, grants: &[Grant]) {
        *lock(&self.declared) = declaration.clone();
        *lock(&self.gated) = covered(declaration, grants);
        self.moved.send_modify(|edge| *edge += 1);
    }

    pub(crate) fn declaration(&self) -> Declaration {
        lock(&self.declared).clone()
    }

    pub(crate) fn unmet(&self) -> Vec<String> {
        lock(&self.unmet).clone()
    }

    fn gated(&self) -> Vec<String> {
        lock(&self.gated).clone()
    }

    /// The admission judgment (M2-K24; R11, constitution 01: requests are
    /// not grants): every element that is no declaration, and every
    /// declared contract no ADMITTED grant covers, is a per-entry fault.
    /// Each is returned for the record; any one refuses the entry.
    pub(crate) fn admission(&self, admitted: &[Grant]) -> Vec<KernelError> {
        let declaration = self.declaration();
        let mut refused: Vec<KernelError> = declaration
            .faults
            .iter()
            .map(|fault| {
                refusal(
                    ErrorCode::EffectFailed,
                    format!("injects entry refused: {fault}"),
                )
            })
            .collect();
        for contract in &declaration.contracts {
            if !admitted.iter().any(|grant| grant.contract == *contract) {
                refused.push(refusal(
                    ErrorCode::EffectFailed,
                    format!("injects entry refused: {contract} is declared but not granted"),
                ));
            }
        }
        refused
    }
}

/// The declared contracts a grant covers by NAME, in declaration order.
fn covered(declaration: &Declaration, grants: &[Grant]) -> Vec<String> {
    declaration
        .contracts
        .iter()
        .filter(|contract| grants.iter().any(|grant| grant.contract == **contract))
        .cloned()
        .collect()
}

/// The epoch a gated entry may activate against now: the loader's own
/// dependencies first, then one snapshot per gated contract whose provider
/// is live on the broker AND whose providing fiber is `Active` — a
/// `ServiceProvided` row lands while the provider is still `Loading`
/// (#45), so provision alone is not readiness. A kernel-supplied provider
/// (no fiber) is trivially ready. `None` while anything is unmet; the
/// unmet names are left on the gate for introspection.
pub(crate) fn compute(
    broker: &Broker,
    state_of: impl Fn(FiberId) -> Option<FiberState>,
    gate: &Gate,
    loader: Option<Epoch>,
) -> Option<Epoch> {
    let mut dependencies = Vec::new();
    let mut unmet = Vec::new();
    for contract in gate.gated() {
        match broker.provider_of(&contract) {
            Some((provider, generation))
                if provider.is_none_or(|fiber| state_of(fiber) == Some(FiberState::Active)) =>
            {
                dependencies.push(DependencySnapshot {
                    service: ServiceType::of::<StringLane>(),
                    provider: provider.unwrap_or(KERNEL_PROVIDER),
                    generation: Generation(generation),
                    realm: Realm::Root,
                });
            }
            _ => unmet.push(contract),
        }
    }
    let ready = unmet.is_empty();
    *lock(&gate.unmet) = unmet;
    let mut epoch = loader?;
    epoch.dependencies.append(&mut dependencies);
    ready.then_some(epoch)
}

/// The edges a gate recomputes on, and the source its epoch publishes to.
pub(crate) struct Edges {
    pub(crate) broker: Arc<Broker>,
    pub(crate) provisions: watch::Receiver<u64>,
    pub(crate) transitions: watch::Receiver<u64>,
}

/// Composes the loader's signal with the gate into the ONE signal the
/// fiber consumes for its whole life, published through a
/// [`ReadinessSource`] so the lane's `track` seam keeps its type. One
/// watcher task per entry on the current runtime (R1): it recomputes on
/// the loader's edge, the declaration's edge, and — only while something
/// is gated — the broker's and the lane's; it publishes only when the
/// epoch actually moved; it ends when `stop`'s sender drops with the
/// entry's handle, leaving the fiber holding what it was last told.
pub(crate) fn gated(
    edges: Edges,
    state_of: impl Fn(FiberId) -> Option<FiberState> + Send + 'static,
    gate: Arc<Gate>,
    mut loader: WatchReadiness,
    mut stop: watch::Receiver<()>,
) -> WatchReadiness {
    let Edges {
        broker,
        mut provisions,
        mut transitions,
    } = edges;
    let mut moved = gate.moved.subscribe();
    let mut last = compute(&broker, &state_of, &gate, loader.epoch());
    let source = ReadinessSource::new(last.clone());
    let signal = source.signal();
    tokio::spawn(async move {
        loop {
            let gating = !gate.gated().is_empty();
            tokio::select! {
                stopped = stop.changed() => {
                    if stopped.is_err() {
                        return;
                    }
                }
                () = loader.changed() => {}
                edge = moved.changed() => {
                    if edge.is_err() {
                        return;
                    }
                }
                edge = provisions.changed(), if gating => {
                    if edge.is_err() {
                        return;
                    }
                }
                edge = transitions.changed(), if gating => {
                    if edge.is_err() {
                        return;
                    }
                }
            }
            let next = compute(&broker, &state_of, &gate, loader.epoch());
            if next != last {
                match &next {
                    Some(epoch) => source.ready(epoch.clone()),
                    None => source.withdraw(),
                }
                last = next;
            }
        }
    });
    signal
}

#[cfg(all(test, not(feature = "loom")))]
mod tests;
