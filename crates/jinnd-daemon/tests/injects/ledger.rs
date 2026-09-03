//! What the cases read off the ledger (M2-K24): transitions, loads,
//! crossings, provisions, and errors per entry. Split from the harness by
//! responsibility (R10 file hygiene).

use jinnd_api::{FiberState, LedgerEventKind, LedgerRecord, Transition};
use jinnd_daemon::Daemon;

/// The sibling contract the fixture's `provider` mode provides and the
/// `inject-counter` modes inject at activation.
pub(crate) const COUNTER: &str = "jinn:test/counter";

/// The ledger after the daemon has committed every landed transition.
pub(crate) async fn events(daemon: &Daemon) -> Vec<LedgerRecord> {
    daemon.sync_transitions();
    daemon
        .ledger_events()
        .await
        .unwrap_or_else(|error| panic!("ledger read: {error:?}"))
}

/// `entry`'s committed transitions, in ledger order.
pub(crate) fn transitions<'a>(records: &'a [LedgerRecord], entry: &str) -> Vec<&'a Transition> {
    records
        .iter()
        .filter(|record| record.entry.as_ref().is_some_and(|id| id.0 == entry))
        .filter_map(|record| match &record.kind {
            LedgerEventKind::FiberTransition(transition) => Some(transition),
            _ => None,
        })
        .collect()
}

/// How many times `entry` entered `Loading` — one per activation.
pub(crate) fn loads(records: &[LedgerRecord], entry: &str) -> usize {
    transitions(records, entry)
        .iter()
        .filter(|transition| transition.to == FiberState::Loading)
        .count()
}

/// Whether `entry` ever rested `Failed`.
pub(crate) fn failed(records: &[LedgerRecord], entry: &str) -> bool {
    transitions(records, entry)
        .iter()
        .any(|transition| transition.to == FiberState::Failed)
}

/// `entry`'s contract-call crossings of `operation` on the counter.
pub(crate) fn calls(records: &[LedgerRecord], entry: &str, operation: &str) -> usize {
    records
        .iter()
        .filter(|record| record.entry.as_ref().is_some_and(|id| id.0 == entry))
        .filter(|record| {
            matches!(&record.kind, LedgerEventKind::ContractCall { contract, operation: op } if contract == COUNTER && op == operation)
        })
        .count()
}

/// `entry`'s recorded errors, as their messages.
pub(crate) fn errors(records: &[LedgerRecord], entry: &str) -> Vec<String> {
    records
        .iter()
        .filter(|record| record.entry.as_ref().is_some_and(|id| id.0 == entry))
        .filter_map(|record| match &record.kind {
            LedgerEventKind::ErrorRecorded { error } => Some(error.message.clone()),
            _ => None,
        })
        .collect()
}

/// Whether `entry` has provided anything (a `ServiceProvided` row, which
/// the broker writes synchronously — unlike transition rows, which the
/// daemon syncs per pass in fiber-map order, so ledger sequence is no
/// witness of ordering ACROSS fibers).
pub(crate) fn provided(records: &[LedgerRecord], entry: &str) -> bool {
    records
        .iter()
        .filter(|record| record.entry.as_ref().is_some_and(|id| id.0 == entry))
        .any(|record| matches!(record.kind, LedgerEventKind::ServiceProvided { .. }))
}
