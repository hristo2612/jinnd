//! Store contract: monotonic receipts, replay-identical reopen, attribution.

use jinnd_api::{EntryId, ErrorCode, FiberId, KernelError, LedgerEventKind, LedgerQuery};
use jinnd_ledger::Ledger;

fn open() -> Ledger {
    Ledger::open_in_memory().unwrap_or_else(|error| panic!("open: {error}"))
}

fn entry(id: &str) -> Option<EntryId> {
    Some(EntryId(id.to_owned()))
}

#[tokio::test]
async fn receipts_are_monotonic_and_events_replay_in_order() {
    let ledger = open();
    let mut last = 0;
    for label in ["one", "two", "three"] {
        let receipt = ledger
            .append(
                LedgerEventKind::EffectRegistered {
                    label: label.to_owned(),
                },
                None,
                None,
            )
            .await
            .unwrap_or_else(|error| panic!("append: {error}"));
        assert!(receipt.sequence > last, "sequence must be monotonic");
        last = receipt.sequence;
    }
    let records = ledger
        .events(LedgerQuery::default())
        .await
        .unwrap_or_else(|error| panic!("events: {error}"));
    assert_eq!(records.len(), 3);
    assert!(records.windows(2).all(|w| w[0].sequence < w[1].sequence));
}

#[tokio::test]
async fn a_reopened_ledger_replays_identically() {
    let dir = std::env::temp_dir().join(format!("jinnd-ledger-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap_or_else(|error| panic!("mkdir: {error}"));
    let path = dir.join("ledger.sqlite3");

    let before = {
        let ledger = Ledger::open(&path).unwrap_or_else(|error| panic!("open: {error}"));
        for index in 0..3 {
            // Receipted: durable before the acknowledgement resolves, so the
            // abrupt drop below is the process-death simulation.
            ledger
                .append(
                    LedgerEventKind::WriteBack {
                        detail: format!("commit {index}"),
                    },
                    entry("persisted"),
                    None,
                )
                .await
                .unwrap_or_else(|error| panic!("append: {error}"));
        }
        ledger
            .events(LedgerQuery::default())
            .await
            .unwrap_or_else(|error| panic!("events: {error}"))
        // The ledger drops here with no orderly shutdown.
    };

    let reopened = Ledger::open(&path).unwrap_or_else(|error| panic!("reopen: {error}"));
    let after = reopened
        .events(LedgerQuery::default())
        .await
        .unwrap_or_else(|error| panic!("events after reopen: {error}"));
    assert_eq!(before, after, "a reopened ledger replays identically");
    std::fs::remove_dir_all(&dir).unwrap_or_else(|error| panic!("cleanup: {error}"));
}

#[tokio::test]
async fn queries_attribute_errors_to_their_entry() {
    let ledger = open();
    ledger
        .append(
            LedgerEventKind::ErrorRecorded {
                error: KernelError {
                    code: ErrorCode::PluginFailed,
                    message: "the entry failed".to_owned(),
                    fiber: None,
                },
            },
            entry("failing-entry"),
            Some(FiberId(7)),
        )
        .await
        .unwrap_or_else(|error| panic!("append: {error}"));
    ledger
        .append(
            LedgerEventKind::WriteBack {
                detail: "unrelated".to_owned(),
            },
            entry("other-entry"),
            None,
        )
        .await
        .unwrap_or_else(|error| panic!("append: {error}"));

    let by_entry = ledger
        .events(LedgerQuery {
            entry: entry("failing-entry"),
            ..LedgerQuery::default()
        })
        .await
        .unwrap_or_else(|error| panic!("events: {error}"));
    assert_eq!(by_entry.len(), 1);
    assert!(matches!(
        by_entry[0].kind,
        LedgerEventKind::ErrorRecorded { .. }
    ));

    let by_fiber = ledger
        .events(LedgerQuery {
            fiber: Some(FiberId(7)),
            ..LedgerQuery::default()
        })
        .await
        .unwrap_or_else(|error| panic!("events: {error}"));
    assert_eq!(by_fiber.len(), 1);

    let from = ledger
        .events(LedgerQuery {
            from_sequence: Some(by_entry[0].sequence + 1),
            ..LedgerQuery::default()
        })
        .await
        .unwrap_or_else(|error| panic!("events: {error}"));
    assert_eq!(from.len(), 1, "from_sequence is inclusive");
}

#[tokio::test]
async fn the_unreceipted_lane_is_ordered_with_receipted_appends() {
    let ledger = open();
    ledger.record(
        LedgerEventKind::ServiceProvided {
            service: "jinn.test/first".to_owned(),
        },
        None,
        None,
    );
    ledger
        .append(
            LedgerEventKind::ServiceProvided {
                service: "jinn.test/second".to_owned(),
            },
            None,
            None,
        )
        .await
        .unwrap_or_else(|error| panic!("append: {error}"));
    let records = ledger
        .events(LedgerQuery::default())
        .await
        .unwrap_or_else(|error| panic!("events: {error}"));
    let services: Vec<&str> = records
        .iter()
        .map(|record| match &record.kind {
            LedgerEventKind::ServiceProvided { service } => service.as_str(),
            other => panic!("unexpected kind: {other:?}"),
        })
        .collect();
    assert_eq!(services, ["jinn.test/first", "jinn.test/second"]);
}

#[tokio::test]
async fn the_reserved_dispatch_trace_class_appends_and_replays() {
    let ledger = open();
    ledger
        .append(
            LedgerEventKind::DispatchTrace {
                event: "jinn.test/reserved".to_owned(),
            },
            None,
            None,
        )
        .await
        .unwrap_or_else(|error| panic!("append: {error}"));
    let records = ledger
        .events(LedgerQuery::default())
        .await
        .unwrap_or_else(|error| panic!("events: {error}"));
    assert!(matches!(
        records[0].kind,
        LedgerEventKind::DispatchTrace { .. }
    ));
}
