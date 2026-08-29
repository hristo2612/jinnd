//! Writer-thread pins: the M2-K2 honesty path for refused unreceipted
//! appends, and the M2-K7 paging / high-water reads.

use jinnd_api::{DispatchMode, ErrorCode, FiberId, LedgerEventKind, LedgerQuery};

use super::{open_memory, serve};
use crate::store::Op;

/// M2-K7 (`jinn:ledger` reader): a page is at most `limit` records
/// from `from`, in order; paging by last+1 walks the stream; the last
/// sequence is the highest committed one, 0 when empty.
#[tokio::test]
async fn pages_walk_the_stream_and_last_sequence_is_the_high_water_mark() {
    let ledger = crate::Ledger::open_in_memory().unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(ledger.last_sequence().await, Ok(0));
    for label in ["a", "b", "c", "d", "e"] {
        ledger.record(
            LedgerEventKind::EffectRegistered {
                label: label.to_owned(),
            },
            None,
            None,
        );
    }
    let first = ledger
        .page(1, 2)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        first
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let next = first.last().map_or(1, |record| record.sequence + 1);
    let second = ledger
        .page(next, 10)
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        second
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        vec![3, 4, 5]
    );
    assert!(
        ledger
            .page(6, 10)
            .await
            .unwrap_or_else(|error| panic!("{error}"))
            .is_empty()
    );
    assert_eq!(ledger.last_sequence().await, Ok(5));
}

/// M2-K2 (R6, R11): an unreceipted append that storage refuses is
/// recorded through the ledger's own honesty path — an `ErrorRecorded`
/// event with the original attribution — never silently dropped.
#[test]
fn a_refused_unreceipted_append_lands_an_error_recorded_event() {
    let connection = open_memory().unwrap_or_else(|error| panic!("open: {error}"));
    connection
        .execute_batch(
            "CREATE TRIGGER refuse_traces BEFORE INSERT ON events
             WHEN new.kind LIKE '%DispatchTrace%'
             BEGIN SELECT RAISE(ABORT, 'trace storage refused'); END",
        )
        .unwrap_or_else(|error| panic!("trigger: {error}"));

    serve(
        &connection,
        Op::Append {
            kind: LedgerEventKind::DispatchTrace {
                topic: "jinn:test/topic".to_owned(),
                mode: DispatchMode::Emit,
                listeners: 0,
                failures: 0,
                emitter: 9,
            },
            entry: None,
            fiber: Some(FiberId(7)),
            ack: None,
        },
    );

    let records = super::select(
        &connection,
        &LedgerQuery {
            entry: None,
            fiber: None,
            from_sequence: None,
        },
        None,
    )
    .unwrap_or_else(|error| panic!("select: {error}"));
    assert_eq!(records.len(), 1, "the honesty event landed: {records:?}");
    match &records[0].kind {
        LedgerEventKind::ErrorRecorded { error } => {
            assert_eq!(error.code, ErrorCode::EffectFailed);
            assert!(
                error.message.contains("unreceipted ledger append failed"),
                "names the failure class: {}",
                error.message
            );
            assert!(
                error.message.contains("trace storage refused"),
                "carries storage's verbatim refusal: {}",
                error.message
            );
        }
        other => panic!("not the honesty event: {other:?}"),
    }
    assert_eq!(
        records[0].fiber,
        Some(FiberId(7)),
        "the failed append's attribution survives onto the honesty event"
    );
}

/// The receipted lane keeps its contract: the caller gets the storage
/// error through its acknowledgement, and no honesty event doubles it.
#[test]
fn a_refused_receipted_append_answers_its_caller_without_doubling() {
    let connection = open_memory().unwrap_or_else(|error| panic!("open: {error}"));
    connection
        .execute_batch(
            "CREATE TRIGGER refuse_traces BEFORE INSERT ON events
             WHEN new.kind LIKE '%DispatchTrace%'
             BEGIN SELECT RAISE(ABORT, 'trace storage refused'); END",
        )
        .unwrap_or_else(|error| panic!("trigger: {error}"));

    let (ack, receipt) = tokio::sync::oneshot::channel();
    serve(
        &connection,
        Op::Append {
            kind: LedgerEventKind::DispatchTrace {
                topic: "jinn:test/topic".to_owned(),
                mode: DispatchMode::Emit,
                listeners: 0,
                failures: 0,
                emitter: 9,
            },
            entry: None,
            fiber: None,
            ack: Some(ack),
        },
    );
    let answered = receipt
        .blocking_recv()
        .unwrap_or_else(|error| panic!("answered: {error}"));
    assert!(answered.is_err(), "the caller holds the refusal");
    let records = super::select(
        &connection,
        &LedgerQuery {
            entry: None,
            fiber: None,
            from_sequence: None,
        },
        None,
    )
    .unwrap_or_else(|error| panic!("select: {error}"));
    assert!(
        records.is_empty(),
        "the receipted lane surfaces to its caller, not the honesty path: {records:?}"
    );
}
