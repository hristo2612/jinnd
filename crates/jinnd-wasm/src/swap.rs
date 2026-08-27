//! Mode-1 hot-swap (R8): old instance warm until the new one is healthy,
//! auto-rollback on failure, batched over every entry sharing the artifact
//! hash (decision log 2026-08-25), every phase a ledger event.
//!
//! The interleaving-sensitive part — a swap racing an entry disposal — lives
//! in [`SwapCore`], a sync phase machine modeled under loom (`--features
//! loom`) and driven by [`swap_batch`] itself: the model and the production
//! path are ONE machine (round-2 blocker-3 ruling). The async driver never
//! holds the core's lock across an await (R1).

use jinnd_api::{EntryId, KernelError, KernelFuture, LedgerEventKind, SwapPhaseKind};

use crate::peer::LedgerSink;
use crate::sync::Mutex;
use std::collections::HashMap;

/// One slot's swap phase. `Tombstone` is a disposed entry: a swap must never
/// resurrect it (I1/I4 — disposal wins).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotPhase {
    Steady,
    Preparing,
    Tombstone,
}

/// The phase machine deciding ownership between a swap's commit and a
/// concurrent disposal. Exactly one side ends up owning a prepared
/// instance: a claim that loses to a tombstone reports `false` and the
/// disposer's answer says whether it observed a preparation to discard.
#[derive(Default)]
pub struct SwapCore {
    slots: Mutex<HashMap<u64, SlotPhase>>,
}

impl SwapCore {
    fn with<T>(&self, f: impl FnOnce(&mut HashMap<u64, SlotPhase>) -> T) -> T {
        let mut guard = self
            .slots
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        f(&mut guard)
    }

    /// Steady → Preparing. False for a tombstoned or already-preparing slot.
    pub fn begin(&self, slot: u64) -> bool {
        self.with(
            |slots| match slots.entry(slot).or_insert(SlotPhase::Steady) {
                phase @ SlotPhase::Steady => {
                    *phase = SlotPhase::Preparing;
                    true
                }
                _ => false,
            },
        )
    }

    /// The batch claim: Preparing → Steady for EVERY slot at once, under one
    /// lock — or for none of them. False when any slot left Preparing (a
    /// disposal tombstoned it): the caller must roll the whole batch back,
    /// so a partial commit cannot exist (R8 atomic replacement).
    pub fn commit_all(&self, slots: &[u64]) -> bool {
        self.with(|phases| {
            let all_preparing = slots
                .iter()
                .all(|slot| phases.get(slot) == Some(&SlotPhase::Preparing));
            if all_preparing {
                for slot in slots {
                    phases.insert(*slot, SlotPhase::Steady);
                }
            }
            all_preparing
        })
    }

    /// Preparing → Steady with the old instance kept (rollback).
    pub fn rollback(&self, slot: u64) {
        self.with(|slots| {
            if let Some(phase) = slots.get_mut(&slot)
                && *phase == SlotPhase::Preparing
            {
                *phase = SlotPhase::Steady;
            }
        });
    }

    /// Any phase → Tombstone. Returns the phase the disposer observed: seeing
    /// `Preparing` transfers ownership of the prepared instance to the
    /// disposer's side — the swap's claim will refuse and discard it.
    pub fn dispose(&self, slot: u64) -> SlotPhase {
        self.with(|slots| {
            let previous = slots.insert(slot, SlotPhase::Tombstone);
            previous.unwrap_or(SlotPhase::Steady)
        })
    }

    /// True once `slot` was disposed — the installer's convergence check:
    /// an install that observes the tombstone retires what it installed.
    pub fn is_tombstone(&self, slot: u64) -> bool {
        self.with(|slots| slots.get(&slot) == Some(&SlotPhase::Tombstone))
    }
}

/// The lane seam the swap driver drives (transport-agnostic: the adapter's
/// wasm lane implements it over real instances; unit tests over mocks).
///
/// Contract: `entries_pinned_to` names each live entry with its slot key in
/// the shared [`SwapCore`]; `prepare` builds and health-gates the NEW
/// instance — activate, offer the old snapshot, health check — while the old
/// instance stays warm and untouched; `install` lands a CLAIMED instance
/// (called only after [`SwapCore::commit_all`] succeeded) and must converge
/// with a concurrent disposal rather than fail the batch — its error means
/// "disposal won this entry, the prepared instance is gone"; `discard` drops
/// a prepared instance without ever touching the old one.
pub trait SwapSlots: Send + Sync {
    type Prepared: Send;

    fn entries_pinned_to(&self, hash: &str) -> Vec<(EntryId, u64)>;

    fn prepare(&self, entry: &EntryId) -> KernelFuture<'_, Self::Prepared>;

    fn install(&self, entry: &EntryId, slot: u64, prepared: Self::Prepared)
    -> KernelFuture<'_, ()>;

    fn discard(&self, prepared: Self::Prepared) -> KernelFuture<'_, ()>;
}

/// One batch outcome (mirrored to the facade's `SwapReport` by the harness).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SwapOutcome {
    pub swapped: Vec<EntryId>,
    pub rolled_back: bool,
}

/// Drives one Mode-1 swap over every entry pinned to `old_hash` — the batch
/// is by artifact hash, never per entry. Phases run through `core`, the ONE
/// loom-modeled machine: begin each slot, prepare each instance, then claim
/// every slot atomically with [`SwapCore::commit_all`] before any install.
/// A failed preparation, a refused begin, or a lost claim rolls the WHOLE
/// batch back — old instances stay warm, zero entries commit. Every phase is
/// a ledger event.
///
/// # Errors
///
/// None of its own: every failure path is the ROLLBACK outcome, recorded.
pub async fn swap_batch<S: SwapSlots>(
    slots: &S,
    core: &SwapCore,
    old_hash: &str,
    new_hash: &str,
    ledger: &dyn LedgerSink,
) -> Result<SwapOutcome, KernelError> {
    let entries = slots.entries_pinned_to(old_hash);
    ledger.append(
        LedgerEventKind::SwapPhase {
            artifact: new_hash.to_owned(),
            phase: SwapPhaseKind::Began,
        },
        None,
    );
    let mut prepared: Vec<(EntryId, u64, S::Prepared)> = Vec::new();
    let mut begun: Vec<u64> = Vec::new();
    let mut lost = false;
    for (entry, slot) in &entries {
        if !core.begin(*slot) {
            lost = true;
            break;
        }
        begun.push(*slot);
        match slots.prepare(entry).await {
            Ok(instance) => {
                ledger.append(
                    LedgerEventKind::SwapPhase {
                        artifact: new_hash.to_owned(),
                        phase: SwapPhaseKind::InstanceHealthy,
                    },
                    None,
                );
                prepared.push((entry.clone(), *slot, instance));
            }
            Err(_) => {
                lost = true;
                break;
            }
        }
    }
    if lost || !core.commit_all(&begun) {
        for slot in &begun {
            core.rollback(*slot);
        }
        for (_, _, instance) in prepared {
            let _ = slots.discard(instance).await;
        }
        ledger.append(
            LedgerEventKind::SwapPhase {
                artifact: new_hash.to_owned(),
                phase: SwapPhaseKind::RolledBack,
            },
            None,
        );
        return Ok(SwapOutcome {
            swapped: Vec::new(),
            rolled_back: true,
        });
    }
    let mut swapped = Vec::new();
    for (entry, slot, instance) in prepared {
        // A failed install means disposal won this entry post-claim: the
        // lane converged (the prepared instance is retired), the rest of
        // the batch stands.
        if slots.install(&entry, slot, instance).await.is_ok() {
            swapped.push(entry);
        }
    }
    ledger.append(
        LedgerEventKind::SwapPhase {
            artifact: new_hash.to_owned(),
            phase: SwapPhaseKind::Committed,
        },
        None,
    );
    Ok(SwapOutcome {
        swapped,
        rolled_back: false,
    })
}

#[cfg(all(test, feature = "loom"))]
mod swap_model;

#[cfg(all(test, not(feature = "loom")))]
mod swap_tests;
