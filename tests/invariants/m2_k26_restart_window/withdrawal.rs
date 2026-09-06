//! A Failed state publishes before the lane's asynchronous tombstone watcher
//! runs (R1). Observe its attributed withdrawal after that committed failure.

use super::{NOTICE, ledger};
use jinnd_api::{EntryId, FiberId, FiberState, LedgerEventKind, LedgerRecord};
use jinnd_daemon::Daemon;
use std::time::{Duration, Instant};

fn consumer(record: &LedgerRecord, fiber: FiberId) -> bool {
    record
        .entry
        .as_ref()
        .is_some_and(|entry| entry.0 == "consumer")
        && record.fiber == Some(fiber)
}

pub(super) fn failed(records: &[LedgerRecord], fiber: FiberId) -> u64 {
    records
        .iter()
        .find(|record| {
            consumer(record, fiber)
                && matches!(&record.kind,
                    LedgerEventKind::FiberTransition(transition)
                        if transition.fiber == fiber && transition.to == FiberState::Failed
                )
        })
        .map(|record| record.sequence)
        .unwrap_or_else(|| panic!("the consumer's Failed is recorded first: {records:?}"))
}

fn after_failed(
    records: &[LedgerRecord],
    fiber: FiberId,
    failed: u64,
) -> Result<u64, &'static str> {
    records
        .iter()
        .find(|record| {
            consumer(record, fiber)
                && record.sequence > failed
                && matches!(&record.kind, LedgerEventKind::EffectWithdrawn { label, clean: true }
            if label == &format!("listen {NOTICE}"))
        })
        .map(|record| record.sequence)
        .ok_or("the exact clean withdrawal after Failed is missing")
}

pub(super) async fn wait(
    daemon: &Daemon,
    fiber: FiberId,
    failed: u64,
    mut records: Vec<LedgerRecord>,
) -> Vec<LedgerRecord> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(withdrawn) = after_failed(&records, fiber, failed) {
            println!(
                "K26 withdrawal consumer fiber={fiber:?}: Failed={failed}, clean withdrawal={withdrawn}"
            );
            return records;
        }
        assert!(
            Instant::now() < deadline,
            "the consumer's clean tombstone withdrawal follows Failed within 20s: {records:?}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
        records = ledger::events(daemon).await;
    }
}

pub(super) fn controls(records: &[LedgerRecord], fiber: FiberId, failed: u64) {
    let withdrawn = after_failed(records, fiber, failed)
        .unwrap_or_else(|error| panic!("real withdrawal: {error}"));
    // Replay a prefix of the REAL ledger ending at Failed. It is a causal
    // snapshot, not a claim that this execution's first read caught the gap.
    let prefix: Vec<_> = records
        .iter()
        .filter(|record| record.sequence <= failed)
        .cloned()
        .collect();
    assert_eq!(self::failed(&prefix, fiber), failed);
    assert!(after_failed(&prefix, fiber, failed).is_err());

    let mut missing = records.to_vec();
    missing.retain(|record| record.sequence != withdrawn);
    assert!(after_failed(&missing, fiber, failed).is_err());
    let real = records
        .iter()
        .find(|record| record.sequence == withdrawn)
        .unwrap_or_else(|| panic!("real row exists"));
    for defect in ["entry", "fiber", "label", "clean", "ordering"] {
        let mut wrong = real.clone();
        match defect {
            "entry" => wrong.entry = Some(EntryId("another-consumer".to_owned())),
            "fiber" => wrong.fiber = None,
            "label" => {
                wrong.kind = LedgerEventKind::EffectWithdrawn {
                    label: "another effect".to_owned(),
                    clean: true,
                }
            }
            "clean" => {
                wrong.kind = LedgerEventKind::EffectWithdrawn {
                    label: format!("listen {NOTICE}"),
                    clean: false,
                }
            }
            "ordering" => wrong.sequence = failed.saturating_sub(1),
            _ => unreachable!(),
        }
        let mut control = missing.clone();
        control.push(wrong);
        assert!(
            after_failed(&control, fiber, failed).is_err(),
            "wrong {defect} cannot satisfy withdrawal"
        );
    }
    assert_eq!(after_failed(records, fiber, failed), Ok(withdrawn));
    println!(
        "K26 withdrawal controls: real Failed prefix and missing/wrong entry/fiber/label/clean/order all rejected; real later withdrawal accepted"
    );
}
