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
    pub(crate) recorded: Option<usize>,
}

/// Spawns one fiber through `spawn` and records it under `entry` for the
/// transition bridge and the sink's entry attribution — in ONE critical
/// section (M2-K24 round 3; Law 2, M2-K7). The supervisor starts inside
/// `spawn` and may run the body on another worker before this returns;
/// the map lock is taken FIRST, so a row that body appends waits on the
/// insert through the sink's own lookup instead of landing fiber-only.
/// `spawn` runs no plugin code — it only starts the task (R1).
pub(crate) fn tracked(
    fibers: &SharedFibers,
    entry: EntryId,
    recorded: Option<usize>,
    spawn: impl FnOnce() -> Arc<Fiber>,
) -> Arc<Fiber> {
    let mut fibers = lock(fibers);
    let fiber = spawn();
    fibers.insert(
        fiber.id(),
        Tracked {
            fiber: Arc::clone(&fiber),
            entry,
            recorded,
        },
    );
    fiber
}

/// Emits every committed fiber transition the ledger has not yet seen
/// (R6: transitions are ledger events; ordered, unreceipted lane), each
/// attributed to its entry — and hands the same transition to the
/// lifecycle publisher (M2-K13), IN THAT ORDER: the append is sent to the
/// single writer before the publisher is offered anything, so the
/// publisher's own barrier can never resolve ahead of the row.
///
/// Nothing is offered while the fiber map is locked: the publisher reads
/// lane state of its own, and the kernel holds no lock across a call it
/// does not own (R1).
pub(crate) fn sync_transitions(
    fibers: &SharedFibers,
    ledger: &jinnd_ledger::Ledger,
    publisher: Option<&Arc<crate::daemon::Lifecycle>>,
) {
    let mut committed: Vec<(EntryId, jinnd_api::Transition)> = Vec::new();
    {
        let mut fibers = lock(fibers);
        for tracked in fibers.values_mut() {
            let Some(recorded) = tracked.recorded.as_mut() else {
                continue;
            };
            let transitions = tracked.fiber.record().transitions;
            for transition in transitions.iter().skip(*recorded) {
                ledger.record(
                    LedgerEventKind::FiberTransition(transition.clone()),
                    Some(tracked.entry.clone()),
                    Some(tracked.fiber.id()),
                );
                committed.push((tracked.entry.clone(), transition.clone()));
            }
            *recorded = transitions.len();
        }
    }
    if let Some(publisher) = publisher {
        for (entry, transition) in &committed {
            publisher.offer(entry, transition);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use jinnd_api::{FiberState, KernelFuture, LedgerQuery};
    use jinnd_fiber::{FiberBody, ReadinessSource, Setup};

    use super::*;

    /// A body whose first act is an attributable row appended with only
    /// its fiber — the shape of the lane's admission refusal (M2-K24).
    struct Probe(Arc<dyn LedgerSink>);

    impl FiberBody for Probe {
        fn activate<'a>(&'a self, setup: Setup<'a>) -> KernelFuture<'a, ()> {
            Box::pin(async move {
                self.0.append(
                    LedgerEventKind::ErrorRecorded {
                        error: error(ErrorCode::EffectFailed, "probe refused".to_owned()),
                    },
                    Some(setup.fiber()),
                );
                Ok(())
            })
        }
    }

    /// The ordering the Linux gate hit (M2-K24 round 3), FORCED rather
    /// than waited for: the supervisor starts inside `spawn` and the body
    /// runs on another worker before the tracker has returned — the spawn
    /// step dawdles to make that certain. The row must still name the
    /// entry: the attribution insert happens-before the body's first
    /// append, or Law 2 has a fiber-only row (M2-K7).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_row_the_body_appends_before_the_tracker_returns_names_its_entry() {
        let ledger =
            jinnd_ledger::Ledger::open_in_memory().unwrap_or_else(|error| panic!("{error}"));
        let fibers: SharedFibers = Arc::new(Mutex::new(HashMap::new()));
        let sink: Arc<dyn LedgerSink> = Arc::new(Sink {
            ledger: ledger.clone(),
            fibers: Arc::clone(&fibers),
        });
        let body: Arc<dyn FiberBody> = Arc::new(Probe(Arc::clone(&sink)));
        let source = ReadinessSource::independent();
        let signal = source.signal();
        let fiber = tracked(&fibers, EntryId("probe".to_owned()), Some(0), move || {
            let fiber = Arc::new(Fiber::spawn(body, signal));
            std::thread::sleep(Duration::from_millis(100));
            fiber
        });
        let mut states = fiber.states();
        while *states.borrow() != FiberState::Active {
            states
                .changed()
                .await
                .unwrap_or_else(|error| panic!("{error}"));
        }
        let records = ledger
            .events(LedgerQuery::default())
            .await
            .unwrap_or_else(|error| panic!("{error:?}"));
        let rows: Vec<&jinnd_api::LedgerRecord> = records
            .iter()
            .filter(|record| matches!(record.kind, LedgerEventKind::ErrorRecorded { .. }))
            .collect();
        assert_eq!(rows.len(), 1, "one row: {records:?}");
        assert_eq!(rows[0].fiber, Some(fiber.id()));
        assert_eq!(
            rows[0].entry,
            Some(EntryId("probe".to_owned())),
            "the row names its entry, never fiber-only (Law 2, M2-K7)"
        );
    }
}
