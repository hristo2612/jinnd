use jinnd_api::{FiberState, LedgerEventKind, LedgerRecord, Transition};
use jinnd_daemon::Daemon;

pub(crate) const COUNTER: &str = "jinn:test/counter";

pub(crate) async fn events(daemon: &Daemon) -> Vec<LedgerRecord> {
    daemon.sync_transitions();
    daemon
        .ledger_events()
        .await
        .unwrap_or_else(|error| panic!("ledger read: {error:?}"))
}

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

pub(crate) fn loads(records: &[LedgerRecord], entry: &str) -> usize {
    transitions(records, entry)
        .iter()
        .filter(|transition| transition.to == FiberState::Loading)
        .count()
}

pub(crate) fn failed(records: &[LedgerRecord], entry: &str) -> bool {
    transitions(records, entry)
        .iter()
        .any(|transition| transition.to == FiberState::Failed)
}

pub(crate) fn calls(records: &[LedgerRecord], entry: &str, operation: &str) -> usize {
    records
        .iter()
        .filter(|record| record.entry.as_ref().is_some_and(|id| id.0 == entry))
        .filter(|record| {
            matches!(&record.kind, LedgerEventKind::ContractCall { contract, operation: op } if contract == COUNTER && op == operation)
        })
        .count()
}

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

pub(crate) fn provided(records: &[LedgerRecord], entry: &str) -> bool {
    records
        .iter()
        .filter(|record| record.entry.as_ref().is_some_and(|id| id.0 == entry))
        .any(|record| matches!(record.kind, LedgerEventKind::ServiceProvided { .. }))
}

pub(crate) fn active_sequence(records: &[LedgerRecord], entry: &str) -> u64 {
    records
        .iter()
        .find(|record| {
            record.entry.as_ref().is_some_and(|id| id.0 == entry)
                && matches!(&record.kind, LedgerEventKind::FiberTransition(transition) if transition.to == FiberState::Active)
        })
        .map(|record| record.sequence)
        .unwrap_or_else(|| panic!("{entry} has an Active row"))
}
