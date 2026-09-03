//! The admission faults the round-1 verifier surfaced (M2-K24 round 2,
//! rulings 2 and 3): a declaration is gated on ADMITTED grants, never on
//! a grant the admission point refuses, and an `injects` that is present
//! but no list — `null` included — faults the entry on the record (R11,
//! constitution 01: requests are not grants).

use jinnd_api::FiberState;

use crate::harness::{booted, bystander, entry, home, paths, state, until_state};
use crate::ledger::{COUNTER, calls, errors, events, failed};

/// Ruling 2: a declaration whose only grant is REFUSED at admission (a
/// scope on a contract that declares no scope type) is not a gate — it
/// waits on nothing. With no provider anywhere, the entry still reaches
/// activation and faults there ON THE RECORD, naming both the refused
/// grant and the ungranted declaration; it never rests `Pending` on a
/// grant it does not hold. Its sibling is untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declaration_whose_grant_is_refused_faults_at_admission_instead_of_waiting() {
    let home = home("refused-grant");
    let entries = [
        entry(
            "refused",
            serde_json::json!([{ "contract": COUNTER, "scope": 7 }]),
            serde_json::json!([COUNTER]),
            "inject-counter",
        ),
        bystander("bystander", "plain"),
    ];
    let (paths, _) = paths(&home, &entries);
    let daemon = booted(paths).await;
    until_state(&daemon, "refused", FiberState::Failed).await;
    until_state(&daemon, "bystander", FiberState::Active).await;
    let records = events(&daemon).await;
    let recorded = errors(&records, "refused");
    assert!(
        recorded
            .iter()
            .any(|message| message.starts_with("grant refused") && message.contains(COUNTER)),
        "the grant's refusal is on the record: {recorded:?}"
    );
    assert!(
        recorded.iter().any(
            |message| message.starts_with("injects entry refused") && message.contains(COUNTER)
        ),
        "the declaration's refusal is on the record: {recorded:?}"
    );
    assert_eq!(
        calls(&records, "refused", "get"),
        0,
        "the entry loaded nothing"
    );
    assert!(!failed(&records, "bystander"));
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
}

/// Ruling 3: `injects: null` is a present non-list, not an absent list —
/// a per-entry fault refused on the record at admission; the entry rests
/// `Failed`, its sibling loads normally.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_injects_that_is_null_is_a_contained_entry_fault() {
    let home = home("null-injects");
    let entries = [
        entry(
            "nulled",
            serde_json::json!([COUNTER]),
            serde_json::Value::Null,
            "plain",
        ),
        bystander("bystander", "plain"),
    ];
    let (paths, _) = paths(&home, &entries);
    let daemon = booted(paths).await;
    until_state(&daemon, "nulled", FiberState::Failed).await;
    until_state(&daemon, "bystander", FiberState::Active).await;
    let records = events(&daemon).await;
    let recorded = errors(&records, "nulled");
    assert!(
        recorded
            .iter()
            .any(|message| message.contains("injects is not a list")),
        "the refusal names the shape: {recorded:?}"
    );
    assert_eq!(state(&daemon, "bystander"), Some(FiberState::Active));
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
}
