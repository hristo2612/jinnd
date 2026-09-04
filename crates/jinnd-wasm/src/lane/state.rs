//! The lane's held state: [`LaneCore`] (one broker, topic registry, host,
//! swap machine per assembly), the [`Roster`] row per live entry, and the
//! poison-tolerant `lock`. Split from `lane.rs` by responsibility (R10 file
//! hygiene).

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, MutexGuard};

use jinnd_api::{EntryId, FiberId, FiberState, KernelError, LedgerEventKind};
use jinnd_fiber::FaultSink;
use tokio::sync::watch;

use super::injects::Gate;
use crate::alarms::Alarms;
use crate::broker::Broker;
use crate::handle::HostRecord;
use crate::host::{LoadedComponent, WasmHost};
use crate::peer::{LedgerSink, PeerId};
use crate::slot::{SeatSummary, SharedSlot};
use crate::swap::SwapCore;
use crate::topics::LocalTopics;

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

/// One live wasm entry, addressable by the swap machine.
pub(crate) struct Roster {
    pub(crate) slot: Arc<SharedSlot>,
    /// This activation's [`SwapCore`] key — never reused.
    pub(crate) slot_id: u64,
    pub(crate) peer: PeerId,
    pub(crate) fiber: FiberId,
    pub(crate) context: u64,
    /// The entry's granted `jinn:clock` floor — a staging seat revalidates
    /// against the SAME grant scope its live seat holds (M2-K2, R9).
    pub(crate) clock_floor_ms: u64,
    pub(crate) config: Vec<u8>,
    pub(crate) component: Arc<Mutex<LoadedComponent>>,
    pub(crate) faults: FaultSink,
}

/// Host-held wasm-lane state: ONE broker, one topic registry, one host, one
/// loom-modeled swap phase machine (R7). The assembly owns the ledger sink
/// every broker crossing lands on (R6).
pub struct LaneCore {
    pub broker: Arc<Broker>,
    pub topics: Arc<LocalTopics>,
    /// The `jinn:clock` alarm registry (M2-K2): one per assembly, wakes
    /// ledgered on the same sink.
    pub alarms: Arc<Alarms>,
    pub host: WasmHost,
    pub sink: Arc<dyn LedgerSink>,
    pub packages: Mutex<HashMap<String, Arc<Mutex<LoadedComponent>>>>,
    pub(crate) roster: Mutex<HashMap<EntryId, Roster>>,
    pub swap: SwapCore,
    pub(super) next_slot: AtomicU64,
    /// Per profile entry, the world effects retained across incarnations
    /// (M2-K4): what suspended seats handed back, in registration order,
    /// for the entry's true dispose to withdraw LIFO after the live seat's
    /// own trail — the journal belongs to the ENTRY, never to a fiber.
    pub(super) journals: Mutex<HashMap<EntryId, Vec<HostRecord>>>,
    /// Every spawned wasm fiber's committed state (M2-K24), keyed by
    /// fiber: what a declared consumer's gate reads to know its provider
    /// is `Active`. A row leaves with the fiber's cell.
    pub(super) states: Mutex<HashMap<FiberId, watch::Receiver<FiberState>>>,
    /// The lane's transition edge (M2-K24): moved on every tracked fiber's
    /// state change — a gate's second edge beside the broker's provisions.
    pub(super) transitions: watch::Sender<u64>,
    /// Per entry, its string-lane gate (M2-K24): what it declares and what
    /// it finds unmet, for `jinn:introspect` (Law 2).
    pub(crate) gates: Mutex<HashMap<EntryId, Arc<Gate>>>,
}

impl LaneCore {
    /// Assembles the lane state over the assembly's ledger sink.
    ///
    /// # Errors
    ///
    /// Wasm engine construction failures.
    pub fn new(sink: Arc<dyn LedgerSink>) -> Result<Self, KernelError> {
        Ok(Self {
            broker: Arc::new(Broker::new(Arc::clone(&sink))),
            // The byte-lane tap (M2-K2; Law 2): every emit through this
            // assembly's port lands one DispatchTrace on the same sink.
            topics: Arc::new(LocalTopics::traced(Arc::clone(&sink))),
            alarms: Arc::new(Alarms::new(Arc::clone(&sink))),
            host: WasmHost::new()?,
            sink,
            packages: Mutex::new(HashMap::new()),
            roster: Mutex::new(HashMap::new()),
            swap: SwapCore::default(),
            next_slot: AtomicU64::new(0),
            journals: Mutex::new(HashMap::new()),
            states: Mutex::new(HashMap::new()),
            transitions: watch::Sender::new(0),
            gates: Mutex::new(HashMap::new()),
        })
    }

    /// A tracked fiber's last committed state, when the lane spawned it.
    pub(crate) fn state_of(&self, fiber: FiberId) -> Option<FiberState> {
        lock(&self.states).get(&fiber).map(|state| *state.borrow())
    }

    /// Follows a spawned fiber's states for the gates (M2-K24): one
    /// forwarder task per fiber on the current runtime (R1), moving the
    /// lane's transition edge on every change and ending — its row with
    /// it — when the fiber's cell is gone. The edge moves once at
    /// subscription too, so a gate computed before this fiber was visible
    /// re-reads it. The same forwarder ends the entry's tombstones
    /// (M2-K26 (c)): a fiber that rests `Failed` or `Disposed` has no
    /// successor to commit them away, so they are withdrawn on that rest
    /// — a watch, never a poll (R1) — each an `EffectWithdrawn` row under
    /// the trail, AFTER the transition row the commit already wrote.
    pub(crate) fn track_states(
        self: &Arc<Self>,
        fiber: FiberId,
        mut states: watch::Receiver<FiberState>,
        entry: EntryId,
        trail: bool,
    ) {
        lock(&self.states).insert(fiber, states.clone());
        self.transitions.send_modify(|edge| *edge += 1);
        let lane = Arc::clone(self);
        tokio::spawn(async move {
            while states.changed().await.is_ok() {
                lane.transitions.send_modify(|edge| *edge += 1);
                let rested = matches!(*states.borrow(), FiberState::Failed | FiberState::Disposed);
                if rested {
                    lane.end_tombstones(fiber, &entry, trail);
                }
            }
            lane.end_tombstones(fiber, &entry, trail);
            lock(&lane.states).remove(&fiber);
            lane.transitions.send_modify(|edge| *edge += 1);
        });
    }

    /// Withdraws whatever tombstones `fiber` still holds, on the record
    /// (M2-K26 (c); I4): the quiescent state after a failed replacement
    /// or a disposal is an entry with no listener, as a fresh boot of the
    /// same profile would have it.
    fn end_tombstones(&self, fiber: FiberId, entry: &EntryId, trail: bool) {
        for topic in self.topics.withdraw_tombstones(fiber) {
            if trail {
                self.sink.append_for(
                    LedgerEventKind::EffectWithdrawn {
                        label: format!("listen {topic}"),
                        clean: true,
                    },
                    Some(entry.clone()),
                    Some(fiber),
                );
            }
        }
    }

    /// Watches one committed instance's retained death notice. A staged
    /// instance is armed only after Mode-1 commit; slot identity makes a
    /// displaced instance's late notice stale rather than fatal to its
    /// successor (M2-K25(c), R11).
    pub(crate) fn track_death(
        self: &Arc<Self>,
        slot: Arc<SharedSlot>,
        instance: crate::InstanceHandle,
        faults: FaultSink,
    ) {
        let mut deaths = instance.deaths();
        tokio::spawn(async move {
            if deaths.borrow().is_none() && deaths.changed().await.is_err() {
                return;
            }
            let error = deaths.borrow().clone();
            if let Some(error) = error {
                slot.fault_if_current(&instance, &faults, error);
            }
        });
    }

    /// The declared contracts one entry's gate currently finds unmet, as
    /// `jinn:introspect` reports them (M2-K24, 0.6.0) — `None` for an
    /// entry the lane does not host. The declaration itself is the
    /// document of record's to report, never the gate's (round-1 ruling
    /// 4). A snapshot under brief locks; no guest is called.
    #[must_use]
    pub fn unmet(&self, entry: &EntryId) -> Option<Vec<String>> {
        let gate = Arc::clone(lock(&self.gates).get(entry)?);
        Some(gate.unmet())
    }

    /// One entry's live seat as `jinn:introspect` reports it (M2-K7): the
    /// activation's incarnation (the slot id — never reused in this
    /// process) and its registrations by class. A snapshot of kernel-owned
    /// state under the roster lock; no guest is ever called (R1).
    /// The incarnation a restart refusal concerns (M2-K9): the entry's
    /// current activation id — never reused in this process — which is the
    /// doomed incarnation while its seat is still installed, and the
    /// replacement arriving to take its place once teardown has taken it.
    /// Either way it is the incarnation the refused caller must wait past.
    ///
    /// `None` when NO incarnation has ever been installed — the
    /// load-bearing case: an entry arriving for the first time owes its
    /// first transition too, but nothing is being replaced, so it is never
    /// refused as restarting. Between a suspension and the replacement's
    /// roster row (M2-K26 (c)) the entry's TOMBSTONES answer with the
    /// incarnation they were entombed under, so the oracle never lapses
    /// to "nothing owed" while a refusing row is still selectable. A
    /// snapshot under the roster and slot locks; no guest is called (R1).
    #[must_use]
    pub fn incarnation(&self, entry: &EntryId) -> Option<u64> {
        let rostered = {
            let roster = lock(&self.roster);
            roster
                .get(entry)
                .map(|row| (Arc::clone(&row.slot), row.slot_id))
        };
        match rostered {
            Some((slot, incarnation)) if slot.ever_installed() => Some(incarnation),
            Some(_) => None,
            None => self.topics.entombed_incarnation(entry),
        }
    }

    #[must_use]
    pub fn seat_summary(&self, entry: &EntryId) -> Option<(u64, SeatSummary)> {
        let (slot, incarnation) = {
            let roster = lock(&self.roster);
            let row = roster.get(entry)?;
            (Arc::clone(&row.slot), row.slot_id)
        };
        slot.summary().map(|summary| (incarnation, summary))
    }
}
