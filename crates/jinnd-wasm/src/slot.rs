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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use jinnd_api::{ErrorCode, FiberId, KernelError, KernelFuture, LedgerEventKind};

use crate::alarms::{Alarms, ArmRequest};
use crate::broker::Broker;
use crate::handle::{ActivationOutcome, InstanceHandle, Registration, peer_face};
use crate::peer::{LedgerSink, Peer, PeerId};
use crate::topics::{EventTarget, LocalTopics, Rebind};

mod teardown;

/// One instance's committed contribution: the instance PAIRED with its
/// registration journal — ONE list, in registration order (R5: teardown has
/// no second list to iterate).
pub struct SeatState {
    pub instance: InstanceHandle,
    /// Everything THIS instance registered, in the order it happened;
    /// listens carry the topic-registry ids they were minted under.
    pub registrations: Vec<Registration>,
}

impl SeatState {
    /// The seat of a LIVE activation: its registrations were routed as it
    /// ran, so the journal already carries minted listener ids.
    #[must_use]
    pub fn live(instance: InstanceHandle, outcome: ActivationOutcome) -> Self {
        Self {
            instance,
            registrations: outcome.registrations,
        }
    }

    /// The host-provider effect ids this seat holds (M2-K4), in order.
    #[must_use]
    pub fn host_effects(&self) -> Vec<u64> {
        self.registrations
            .iter()
            .filter_map(|registration| match registration {
                Registration::Host(record) => Some(record.effect),
                _ => None,
            })
            .collect()
    }

    /// The seat's provided contracts, minted listener ids, and armed alarm
    /// ids (derived per-category views over the one journal).
    fn views(&self) -> (Vec<String>, Vec<u64>, Vec<u64>) {
        let mut provisions = Vec::new();
        let mut listens = Vec::new();
        let mut alarms = Vec::new();
        for registration in &self.registrations {
            match registration {
                Registration::Provision { contract } => provisions.push(contract.clone()),
                Registration::Listen(record) => listens.extend(record.id),
                Registration::Alarm(record) => alarms.extend(record.id),
                Registration::Effect { .. } | Registration::Host(_) => {}
            }
        }
        (provisions, listens, alarms)
    }
}

/// The live seat behind one fiber, swappable whole.
#[derive(Default)]
pub struct SharedSlot {
    current: Mutex<Option<SeatState>>,
    /// Raised once the seat's journal closes for withdrawal or suspension
    /// (M2-K4): every later registration attempt refuses on the record.
    sealed: AtomicBool,
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

    /// Closes the journal (M2-K4, FINDINGS #15): stored `SeqCst` BEFORE the
    /// instance is asked to seal, so any guest entry still in flight sees
    /// its next registration refused, and the entries that follow never
    /// run.
    pub fn seal(&self) {
        self.sealed.store(true, Ordering::SeqCst);
    }

    /// True once the journal closed.
    pub fn sealed(&self) -> bool {
        self.sealed.load(Ordering::SeqCst)
    }

    /// Opens the journal for a fresh incarnation (M2-K4): a restart reuses
    /// the fiber's slot, and its closing sequence has already landed on the
    /// fiber's own task before the next activation begins (single-flight).
    pub fn unseal(&self) {
        self.sealed.store(false, Ordering::SeqCst);
    }

    /// Appends registrations made AFTER activation (a wake or call
    /// handler's effects, M2-K3) to the live seat's journal, so teardown
    /// withdraws them with the rest. Handed back when no seat is live yet
    /// (the instant between activation and install): the caller keeps
    /// them for the next drain.
    pub fn extend(&self, late: Vec<Registration>) -> Option<Vec<Registration>> {
        match self.lock().as_mut() {
            Some(seat) => {
                seat.registrations.extend(late);
                None
            }
            None => Some(late),
        }
    }

    /// The live instance, if any.
    pub fn current(&self) -> Option<InstanceHandle> {
        self.lock().as_ref().map(|seat| seat.instance.clone())
    }

    /// The live seat's broker provisions, listener ids, and alarm ids.
    pub fn registrations(&self) -> (Vec<String>, Vec<u64>, Vec<u64>) {
        match self.lock().as_ref() {
            Some(seat) => seat.views(),
            None => (Vec::new(), Vec::new(), Vec::new()),
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
    alarms: &Arc<Alarms>,
    peer: PeerId,
    fiber: Option<FiberId>,
    context: u64,
    ledger: &dyn LedgerSink,
) -> Option<SeatState> {
    let (old_provisions, old_listens, old_alarms) = slot.registrations();
    let face = peer_face(&staged);
    let rebinds: Vec<Rebind> = outcome
        .listens()
        .map(|record| Rebind {
            topic: record.topic.clone(),
            context,
            token: record.token,
            target: Arc::clone(&face) as Arc<dyn EventTarget>,
        })
        .collect();
    let ids = topics.rebind(&old_listens, rebinds);
    // The staged alarms arm against the NEW instance's own face, the
    // displaced seat's alarms cancel — live alarms survive the swap through
    // the staged outcome, exactly like any effect (M2-K2, R8); their floor
    // was validated at staging, so nothing here can fail.
    let arm_requests: Vec<ArmRequest> = outcome
        .alarms()
        .map(|record| ArmRequest {
            spec: record.spec,
            token: record.token,
            fiber,
            target: Arc::clone(&face) as Arc<dyn EventTarget>,
        })
        .collect();
    let alarm_ids = alarms.rebind(&old_alarms, arm_requests);
    // The minted ids land back in the journal, in order: the committed seat
    // carries ONE registration list, exactly as an initial activation's.
    let mut registrations = outcome.registrations;
    let mut minted = ids.into_iter();
    let mut minted_alarms = alarm_ids.into_iter();
    for registration in &mut registrations {
        match registration {
            Registration::Listen(record) => record.id = minted.next(),
            Registration::Alarm(record) => record.id = minted_alarms.next(),
            _ => {}
        }
    }
    let new_provisions: Vec<String> = registrations
        .iter()
        .filter_map(|registration| match registration {
            Registration::Provision { contract } => Some(contract.clone()),
            _ => None,
        })
        .collect();
    let displaced = slot.install(SeatState {
        instance: staged,
        registrations,
    });
    for contract in &new_provisions {
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
        if !new_provisions.contains(contract) {
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
