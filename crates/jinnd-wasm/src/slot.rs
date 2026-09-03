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

use std::sync::atomic::Ordering;
use std::sync::{Mutex, MutexGuard};

use jinnd_api::{ErrorCode, KernelError};

use crate::handle::{ActivationOutcome, InstanceHandle, Registration};
use jinnd_fiber::FaultSink;

mod commit;
mod gate;
mod peer_face;
#[cfg(all(test, feature = "loom"))]
mod seal_model;
mod summary;
mod teardown;

pub use commit::commit_staged;
pub(crate) use gate::SealGate;
pub use summary::SeatSummary;

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

    /// The host-provider effects this seat holds (M2-K4), in order, as
    /// (contract, effect id) — ids are per provider (M2-K8).
    #[must_use]
    pub fn host_effects(&self) -> Vec<(String, u64)> {
        self.registrations
            .iter()
            .filter_map(|registration| match registration {
                Registration::Host(record) => Some((record.contract.clone(), record.effect)),
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
                Registration::Effect { .. } | Registration::Host(_) | Registration::Kernel(_) => {}
            }
        }
        (provisions, listens, alarms)
    }
}

/// The live seat behind one fiber, swappable whole.
#[derive(Default)]
pub struct SharedSlot {
    current: Mutex<Option<SeatState>>,
    /// The closing gate (M2-K4/K5): door, then drain, then journal.
    gate: SealGate,
    /// True once ANY incarnation has been installed here (M2-K9). This is
    /// what tells a fiber being REPLACED from one arriving for the first
    /// time: the slot outlives every incarnation of its entry, so once it
    /// has held a seat, any transition the fiber later owes is a
    /// replacement of something that was live.
    ever: crate::sync::AtomicBool,
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
        self.ever.store(true, Ordering::SeqCst);
        self.lock().replace(seat)
    }

    /// True once an incarnation has been installed here (M2-K9): the entry
    /// has been live, so a transition it owes now replaces something.
    /// Never lowered — `unseal` reopens the gate, not this.
    pub fn ever_installed(&self) -> bool {
        self.ever.load(Ordering::SeqCst)
    }

    /// Empties the slot (teardown), returning the seat to retire.
    pub fn take(&self) -> Option<SeatState> {
        self.lock().take()
    }

    /// Closes the seat in law order (M2-K5, FINDINGS #16): shuts the door
    /// to guest entries not yet dequeued, awaits `drain` — the in-flight
    /// entry returning under its deadline — and only THEN seals the journal.
    /// A drained handler lands every registration; the seal refusal is the
    /// backstop for a handler that outlived its deadline (I1, R5, R11).
    pub async fn close(&self, drain: impl Future<Output = ()>) {
        self.gate.close(drain).await;
    }

    /// Seals the journal ALONE — the backstop step `close` raises last:
    /// every registration refuses on the record from here on, whatever
    /// still runs (a handler past its deadline).
    pub fn seal(&self) {
        self.gate.seal();
    }

    /// True once the door shut: the supervisor refuses guest entries it
    /// dequeues from here on (the one already running finishes).
    pub fn closing(&self) -> bool {
        self.gate.closing()
    }

    /// True once the journal closed: every registration refuses.
    pub fn sealed(&self) -> bool {
        self.gate.sealed()
    }

    /// Opens the seat for a fresh incarnation (M2-K4): a restart reuses
    /// the fiber's slot, and its closing sequence has already landed on the
    /// fiber's own task before the next activation begins (single-flight).
    pub fn unseal(&self) {
        self.gate.reopen();
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

    /// Reports a retained death only if `instance` is still this slot's
    /// committed seat. The identity check and fault write linearize with
    /// Mode-1 install under this brief kernel lock (R1, M2-K25(c)).
    pub(crate) fn fault_if_current(
        &self,
        instance: &InstanceHandle,
        faults: &FaultSink,
        error: KernelError,
    ) -> bool {
        let current = self.lock();
        current
            .as_ref()
            .is_some_and(|seat| seat.instance.same_instance(instance))
            && faults.fault(error)
    }

    /// The live seat's broker provisions, listener ids, and alarm ids.
    pub fn registrations(&self) -> (Vec<String>, Vec<u64>, Vec<u64>) {
        match self.lock().as_ref() {
            Some(seat) => seat.views(),
            None => (Vec::new(), Vec::new(), Vec::new()),
        }
    }
}
