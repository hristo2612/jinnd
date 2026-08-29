//! Shared daemon plumbing (split by responsibility, R10): the ledger sink
//! every broker crossing lands on, the tracked-fiber map behind the
//! transition bridge, and the error/lock helpers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use jinnd_api::{EntryId, ErrorCode, FiberId, KernelError, LedgerEventKind};
use jinnd_fiber::Fiber;
use jinnd_wasm::LedgerSink;

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

pub(crate) fn error(code: ErrorCode, message: String) -> KernelError {
    KernelError {
        code,
        message,
        fiber: None,
    }
}

/// Broker crossings land on the kernel ledger's ordered record lane (R6).
/// The entry column is filled from the fiber → entry mapping the lane
/// knows (M2-K7, harness #19; Law 2): every attributable event names its
/// profile entry, never fiber-only.
pub(crate) struct Sink {
    pub(crate) ledger: jinnd_ledger::Ledger,
    pub(crate) fibers: SharedFibers,
}

impl Sink {
    fn entry_of(&self, fiber: Option<FiberId>) -> Option<EntryId> {
        let fiber = fiber?;
        lock(&self.fibers)
            .get(&fiber)
            .map(|tracked| tracked.entry.clone())
    }
}

impl LedgerSink for Sink {
    fn append(&self, kind: LedgerEventKind, fiber: Option<FiberId>) {
        self.ledger.record(kind, self.entry_of(fiber), fiber);
    }

    fn append_for(&self, kind: LedgerEventKind, entry: Option<EntryId>, fiber: Option<FiberId>) {
        let entry = entry.or_else(|| self.entry_of(fiber));
        self.ledger.record(kind, entry, fiber);
    }
}

/// The daemon's tracked fibers: the transition-ledger bridge's feed (R6).
pub(crate) type SharedFibers = Arc<Mutex<HashMap<FiberId, Tracked>>>;

pub(crate) struct Tracked {
    pub(crate) fiber: Arc<Fiber>,
    /// The profile entry the fiber hosts (the ledger's entry attribution).
    pub(crate) entry: EntryId,
    /// Transitions already emitted to the ledger.
    pub(crate) recorded: usize,
}

/// Emits every committed fiber transition the ledger has not yet seen
/// (R6: transitions are ledger events; ordered, unreceipted lane), each
/// attributed to its entry.
pub(crate) fn sync_transitions(fibers: &SharedFibers, ledger: &jinnd_ledger::Ledger) {
    let mut fibers = lock(fibers);
    for tracked in fibers.values_mut() {
        let transitions = tracked.fiber.record().transitions;
        for transition in transitions.iter().skip(tracked.recorded) {
            ledger.record(
                LedgerEventKind::FiberTransition(transition.clone()),
                Some(tracked.entry.clone()),
                Some(tracked.fiber.id()),
            );
        }
        tracked.recorded = transitions.len();
    }
}
