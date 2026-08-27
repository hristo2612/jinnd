//! One fiber's instance seat: the level of indirection Mode-1 hot-swap needs
//! (R8). The broker holds the SLOT as the provider face, so committing a
//! swap redirects contract-call routing atomically by installing the new
//! seat — the old instance stays warm and fully routed until that instant.
//!
//! A seat pairs the instance with what IT registered: undo tokens and
//! listener ids never outlive or outtravel the instance that minted them
//! (round-2 blocker-4 ruling; R5, I1). A swap replaces the seat WHOLE — the
//! staged activation's outcome is committed, exactly as an initial
//! activation's is; nothing is ever retargeted through a mutable cell.

use std::sync::{Arc, Mutex, MutexGuard};

use jinnd_api::{ErrorCode, FiberId, KernelError, KernelFuture, LedgerEventKind};

use crate::broker::Broker;
use crate::handle::{ActivationOutcome, InstanceHandle, peer_face};
use crate::peer::{LedgerSink, Peer, PeerId};
use crate::topics::{EventTarget, LocalTopics, Rebind};

/// One instance's committed contribution: the instance PAIRED with what it
/// registered, in registration order.
pub struct SeatState {
    pub instance: InstanceHandle,
    /// Guest effects: (label, undo token) — tokens of THIS instance.
    pub effects: Vec<(String, u64)>,
    /// Contracts provided over the broker, routed through the slot face.
    pub provisions: Vec<String>,
    /// Topic-registry ids, each targeting THIS instance's delivery face.
    pub listens: Vec<u64>,
}

impl SeatState {
    /// The seat of a LIVE activation: its registrations were routed as it
    /// ran, so the listens carry the ids they were minted under.
    #[must_use]
    pub fn live(instance: InstanceHandle, outcome: ActivationOutcome) -> Self {
        Self {
            instance,
            effects: outcome.effects,
            provisions: outcome.provisions,
            listens: outcome
                .listens
                .iter()
                .filter_map(|record| record.id)
                .collect(),
        }
    }

    /// Withdraws exactly this seat's contribution (I1), LIFO: the guest
    /// inverses run against the instance that registered them, then the
    /// listeners withdraw, then the provisions, then the instance disposes
    /// (R7 instant dispose). The first failing inverse is reported after the
    /// remaining withdrawal still ran (R9, R11). With a `ledger`, every
    /// effect and listener withdrawal is appended under its registration
    /// label — the seat is where the labels live, so the dispose trail is
    /// exactly complete (Law 2).
    ///
    /// # Errors
    ///
    /// The first guest inverse failure, with everything else withdrawn.
    pub async fn retire(
        self,
        broker: &Broker,
        topics: &LocalTopics,
        peer: PeerId,
        ledger: Option<(&dyn LedgerSink, FiberId)>,
    ) -> Result<(), KernelError> {
        let mut first = None;
        for (label, token) in self.effects.iter().rev() {
            let outcome = self.instance.undo(*token).await;
            if let Some((sink, fiber)) = ledger {
                sink.append(
                    LedgerEventKind::EffectWithdrawn {
                        label: label.clone(),
                        clean: outcome.is_ok(),
                    },
                    Some(fiber),
                );
            }
            if let Err(error) = outcome {
                first.get_or_insert(error);
            }
        }
        for id in &self.listens {
            let topic = topics.unlisten(*id);
            if let (Some((sink, fiber)), Some(topic)) = (ledger, topic) {
                sink.append(
                    LedgerEventKind::EffectWithdrawn {
                        label: format!("listen {topic}"),
                        clean: true,
                    },
                    Some(fiber),
                );
            }
        }
        for contract in &self.provisions {
            broker.withdraw(peer, contract);
        }
        self.instance.dispose().await;
        match first {
            None => Ok(()),
            Some(error) => Err(error),
        }
    }
}

/// The live seat behind one fiber, swappable whole.
#[derive(Default)]
pub struct SharedSlot {
    current: Mutex<Option<SeatState>>,
}

fn empty() -> KernelError {
    KernelError {
        code: ErrorCode::PluginFailed,
        message: "the fiber has no live instance".to_owned(),
        fiber: None,
    }
}

impl SharedSlot {
    fn lock(&self) -> MutexGuard<'_, Option<SeatState>> {
        self.current
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Installs `seat` as the live one, returning the displaced seat —
    /// the swap-commit primitive. No lock is held across either instance.
    pub fn install(&self, seat: SeatState) -> Option<SeatState> {
        self.lock().replace(seat)
    }

    /// Empties the slot (teardown), returning the seat to retire.
    pub fn take(&self) -> Option<SeatState> {
        self.lock().take()
    }

    /// The live instance, if any.
    pub fn current(&self) -> Option<InstanceHandle> {
        self.lock().as_ref().map(|seat| seat.instance.clone())
    }

    /// The live seat's broker provisions and listener ids.
    pub fn registrations(&self) -> (Vec<String>, Vec<u64>) {
        match self.lock().as_ref() {
            Some(seat) => (seat.provisions.clone(), seat.listens.clone()),
            None => (Vec::new(), Vec::new()),
        }
    }
}

/// Commits a staged activation as the slot's live seat — the Mode-1 commit
/// (R8), run INSIDE the batch claim's critical section: every operation
/// here is infallible sync bookkeeping (round-3 ruling — nothing may fail,
/// block, await, or call guest code). The staged listens register
/// atomically against the NEW instance's own delivery face, the new seat
/// replaces the old for contract-call routing, provisions the predecessor
/// did not hold are provided through the slot face (kept ones never
/// re-provide, so their generation and live handles stand — Mode-1
/// continuity), orphaned ones withdraw. The staged outcome is COMMITTED,
/// exactly as an initial activation registers its own (round-2 blocker-4
/// ruling; R5, I1). The displaced seat is handed back for disposal AFTER
/// the critical section — dispose-only: the handoff transferred its
/// contribution to the successor, whose own activation registered its own
/// inverses (warm until commit, R8).
#[allow(clippy::too_many_arguments)]
#[must_use = "the displaced seat must be disposed after the critical section"]
pub fn commit_staged(
    slot: &Arc<SharedSlot>,
    staged: InstanceHandle,
    outcome: ActivationOutcome,
    broker: &Broker,
    topics: &LocalTopics,
    peer: PeerId,
    fiber: Option<FiberId>,
    context: u64,
    ledger: &dyn LedgerSink,
) -> Option<SeatState> {
    let (old_provisions, old_listens) = slot.registrations();
    let face = peer_face(&staged);
    let registrations: Vec<Rebind> = outcome
        .listens
        .iter()
        .map(|record| Rebind {
            topic: record.topic.clone(),
            context,
            token: record.token,
            target: Arc::clone(&face) as Arc<dyn EventTarget>,
        })
        .collect();
    let ids = topics.rebind(&old_listens, registrations);
    let displaced = slot.install(SeatState {
        instance: staged,
        effects: outcome.effects,
        provisions: outcome.provisions.clone(),
        listens: ids,
    });
    for contract in &outcome.provisions {
        if !old_provisions.contains(contract) {
            let provided =
                broker.provide(peer, contract, Arc::new(Arc::clone(slot)) as Arc<dyn Peer>);
            if let Err(error) = provided {
                // Grant-checked at staging; a residual refusal (an occupied
                // slot) is contained and recorded, never silent (R6).
                ledger.append(LedgerEventKind::ErrorRecorded { error }, fiber);
            }
        }
    }
    for contract in &old_provisions {
        if !outcome.provisions.contains(contract) {
            broker.withdraw(peer, contract);
        }
    }
    displaced
}

impl Peer for Arc<SharedSlot> {
    fn call(
        &self,
        caller: PeerId,
        contract: &str,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let current = self.current();
        let (contract, operation) = (contract.to_owned(), operation.to_owned());
        Box::pin(async move {
            match current {
                Some(instance) => {
                    instance
                        .contract_call(caller, &contract, &operation, payload)
                        .await
                }
                None => Err(empty()),
            }
        })
    }

    fn check(&self, consumer: PeerId) -> KernelFuture<'static, bool> {
        let current = self.current();
        Box::pin(async move {
            match current {
                Some(instance) => instance.check(consumer).await,
                None => Ok(false),
            }
        })
    }
}
