//! The lane's held state: [`LaneCore`] (one broker, topic registry, host,
//! swap machine per assembly), the [`Roster`] row per live entry, and the
//! poison-tolerant `lock`. Split from `lane.rs` by responsibility (R10 file
//! hygiene).

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, MutexGuard};

use jinnd_api::{EntryId, FiberId, KernelError};

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
        })
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
    /// `None` when the entry has no roster row, and — the load-bearing
    /// case — when NO incarnation has ever been installed: an entry
    /// arriving for the first time owes its first transition too, but
    /// nothing is being replaced, so it is never refused as restarting. A
    /// snapshot under the roster and slot locks; no guest is called (R1).
    #[must_use]
    pub fn incarnation(&self, entry: &EntryId) -> Option<u64> {
        let (slot, incarnation) = {
            let roster = lock(&self.roster);
            let row = roster.get(entry)?;
            (Arc::clone(&row.slot), row.slot_id)
        };
        slot.ever_installed().then_some(incarnation)
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
