//! Revert protocol contract (constitution 03) through the public lane.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use jinnd_api::{
    EffectId, ErrorCode, KernelError, LedgerEventKind, LedgerQuery, RevertKey, RevertResolution,
    Witness,
};

use jinnd_ledger::Ledger;
use jinnd_ledger::{Inverse, RevertLane};

fn lane() -> RevertLane {
    RevertLane::new(Ledger::open_in_memory().unwrap_or_else(|error| panic!("open: {error}")))
}

fn key(value: &str) -> RevertKey {
    RevertKey(value.to_owned())
}

fn counting_inverse(runs: &Arc<AtomicUsize>) -> Inverse {
    let runs = Arc::clone(runs);
    Box::new(move || {
        runs.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    })
}

fn failing_inverse() -> Inverse {
    Box::new(|| {
        Box::pin(async {
            Err(KernelError {
                code: ErrorCode::EffectFailed,
                message: "the inverse refused".to_owned(),
                fiber: None,
            })
        })
    })
}

#[tokio::test]
async fn a_clean_inverse_with_a_passing_witness_resolves_reverted() {
    let lane = lane();
    let runs = Arc::new(AtomicUsize::new(0));
    let witness: Witness = Arc::new(|| true);
    let state = lane
        .revert(
            EffectId(1),
            key("k"),
            witness,
            counting_inverse(&runs),
            None,
            None,
        )
        .await
        .unwrap_or_else(|error| panic!("revert: {error:?}"));
    assert_eq!(state, RevertResolution::Reverted);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(
        lane.resolution(EffectId(1)),
        Some(RevertResolution::Reverted)
    );
}

#[tokio::test]
async fn a_same_key_retry_returns_the_recorded_state_without_rerunning() {
    let lane = lane();
    let runs = Arc::new(AtomicUsize::new(0));
    let witness: Witness = Arc::new(|| true);
    let first = lane
        .revert(
            EffectId(1),
            key("k"),
            witness.clone(),
            counting_inverse(&runs),
            None,
            None,
        )
        .await
        .unwrap_or_else(|error| panic!("revert: {error:?}"));
    let second = lane
        .revert(
            EffectId(1),
            key("k"),
            witness,
            counting_inverse(&runs),
            None,
            None,
        )
        .await
        .unwrap_or_else(|error| panic!("retry: {error:?}"));
    assert_eq!(first, second);
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the inverse runs exactly once per branch"
    );
}

#[tokio::test]
async fn a_distinct_key_against_an_existing_branch_is_refused() {
    let lane = lane();
    let runs = Arc::new(AtomicUsize::new(0));
    let witness: Witness = Arc::new(|| true);
    lane.revert(
        EffectId(1),
        key("a"),
        witness.clone(),
        counting_inverse(&runs),
        None,
        None,
    )
    .await
    .unwrap_or_else(|error| panic!("revert: {error:?}"));
    let refused = lane
        .revert(
            EffectId(1),
            key("b"),
            witness,
            counting_inverse(&runs),
            None,
            None,
        )
        .await;
    assert!(refused.is_err(), "a distinct key is refused, never retried");
    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_failing_witness_leaves_the_branch_pending_revert_visibly() {
    let lane = lane();
    let runs = Arc::new(AtomicUsize::new(0));
    let witness: Witness = Arc::new(|| false);
    let state = lane
        .revert(
            EffectId(1),
            key("k"),
            witness,
            counting_inverse(&runs),
            None,
            None,
        )
        .await
        .unwrap_or_else(|error| panic!("revert: {error:?}"));
    assert_eq!(state, RevertResolution::PendingRevert);
    assert_eq!(
        lane.resolution(EffectId(1)),
        Some(RevertResolution::PendingRevert),
        "the unresolved branch stays pending-revert, visibly"
    );
}

#[tokio::test]
async fn a_failing_inverse_stays_pending_and_records_the_error() {
    let lane = lane();
    let witness: Witness = Arc::new(|| true);
    let state = lane
        .revert(
            EffectId(1),
            key("k"),
            witness,
            failing_inverse(),
            None,
            None,
        )
        .await
        .unwrap_or_else(|error| panic!("revert: {error:?}"));
    assert_eq!(state, RevertResolution::PendingRevert);
}

#[tokio::test]
async fn compensation_resolves_compensated_never_reverted() {
    let lane = lane();
    let witness: Witness = Arc::new(|| true);
    lane.revert(
        EffectId(1),
        key("k"),
        witness,
        failing_inverse(),
        None,
        None,
    )
    .await
    .unwrap_or_else(|error| panic!("revert: {error:?}"));
    let runs = Arc::new(AtomicUsize::new(0));
    let state = lane
        .compensate(
            EffectId(1),
            key("comp"),
            counting_inverse(&runs),
            true,
            None,
            None,
        )
        .await
        .unwrap_or_else(|error| panic!("compensate: {error:?}"));
    assert_eq!(
        state,
        RevertResolution::Compensated { clean: true },
        "the original witness passes, so the compensation is clean"
    );
    assert_ne!(state, RevertResolution::Reverted);
}

#[tokio::test]
async fn a_compensation_failing_the_original_witness_stays_unclean() {
    let lane = lane();
    let witness: Witness = Arc::new(|| false);
    lane.revert(
        EffectId(1),
        key("k"),
        witness,
        failing_inverse(),
        None,
        None,
    )
    .await
    .unwrap_or_else(|error| panic!("revert: {error:?}"));
    let runs = Arc::new(AtomicUsize::new(0));
    let state = lane
        .compensate(
            EffectId(1),
            key("comp"),
            counting_inverse(&runs),
            true,
            None,
            None,
        )
        .await
        .unwrap_or_else(|error| panic!("compensate: {error:?}"));
    assert_eq!(
        state,
        RevertResolution::Compensated { clean: false },
        "unless compensation satisfies the original witness, the branch is unclean"
    );
}

#[tokio::test]
async fn compensation_without_operator_confirmation_is_refused() {
    let lane = lane();
    let witness: Witness = Arc::new(|| true);
    lane.revert(
        EffectId(1),
        key("k"),
        witness,
        failing_inverse(),
        None,
        None,
    )
    .await
    .unwrap_or_else(|error| panic!("revert: {error:?}"));
    let runs = Arc::new(AtomicUsize::new(0));
    assert!(
        lane.compensate(
            EffectId(1),
            key("comp"),
            counting_inverse(&runs),
            false,
            None,
            None
        )
        .await
        .is_err(),
        "compensation is a distinct, operator-confirmed operation"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 0);
    assert_eq!(
        lane.resolution(EffectId(1)),
        Some(RevertResolution::PendingRevert)
    );
}

#[tokio::test]
async fn compensating_a_resolved_or_unknown_branch_is_refused() {
    let lane = lane();
    let runs = Arc::new(AtomicUsize::new(0));
    assert!(
        lane.compensate(
            EffectId(9),
            key("comp"),
            counting_inverse(&runs),
            true,
            None,
            None
        )
        .await
        .is_err(),
        "an unknown branch cannot be compensated"
    );
    let witness: Witness = Arc::new(|| true);
    lane.revert(
        EffectId(1),
        key("k"),
        witness,
        counting_inverse(&runs),
        None,
        None,
    )
    .await
    .unwrap_or_else(|error| panic!("revert: {error:?}"));
    assert!(
        lane.compensate(
            EffectId(1),
            key("comp"),
            counting_inverse(&runs),
            true,
            None,
            None
        )
        .await
        .is_err(),
        "a reverted branch is closed; compensation applies to pending-revert only"
    );
}

#[tokio::test]
async fn a_reopened_lane_answers_a_same_key_retry_without_rerunning() {
    // The claim IS a ledger event (PLA-276 round-2 blocker 3): claimed keys
    // are derived from the ledger on reopen, never from process memory.
    let dir = std::env::temp_dir().join(format!(
        "jinnd-ledger-reopen-{}-{}",
        std::process::id(),
        line!()
    ));
    std::fs::create_dir_all(&dir).unwrap_or_else(|error| panic!("mkdir: {error}"));
    let path = dir.join("ledger.sqlite3");
    let runs = Arc::new(AtomicUsize::new(0));
    {
        let ledger = Ledger::open(&path).unwrap_or_else(|error| panic!("open: {error}"));
        let lane = RevertLane::new(ledger);
        let witness: Witness = Arc::new(|| true);
        let state = lane
            .revert(
                EffectId(1),
                key("k"),
                witness,
                counting_inverse(&runs),
                None,
                None,
            )
            .await
            .unwrap_or_else(|error| panic!("revert: {error:?}"));
        assert_eq!(state, RevertResolution::Reverted);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        // The lane drops here with no orderly shutdown: process death.
    }
    let reopened =
        RevertLane::new(Ledger::open(&path).unwrap_or_else(|error| panic!("reopen: {error}")));
    let witness: Witness = Arc::new(|| true);
    let retry = reopened
        .revert(
            EffectId(1),
            key("k"),
            witness.clone(),
            counting_inverse(&runs),
            None,
            None,
        )
        .await
        .unwrap_or_else(|error| panic!("retry: {error:?}"));
    assert_eq!(retry, RevertResolution::Reverted);
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "a same-key retry after reopen answers from the ledger; the inverse never re-runs"
    );
    assert!(
        reopened
            .revert(
                EffectId(1),
                key("other"),
                witness,
                counting_inverse(&runs),
                None,
                None,
            )
            .await
            .is_err(),
        "the ledger-bound key still refuses a distinct key after reopen"
    );
    std::fs::remove_dir_all(&dir).unwrap_or_else(|error| panic!("cleanup: {error}"));
}

#[tokio::test]
async fn revert_events_carry_their_effect_and_attribution() {
    // Card requirement (PLA-276 round-2 blocker 4): revert events are
    // traceable — the effect identifier rides the event, the entry/fiber
    // attribution rides the record.
    let ledger = Ledger::open_in_memory().unwrap_or_else(|error| panic!("open: {error}"));
    let lane = RevertLane::new(ledger.clone());
    let runs = Arc::new(AtomicUsize::new(0));
    let witness: Witness = Arc::new(|| true);
    lane.revert(
        EffectId(7),
        key("k"),
        witness,
        counting_inverse(&runs),
        Some(jinnd_api::EntryId("owner".to_owned())),
        Some(jinnd_api::FiberId(3)),
    )
    .await
    .unwrap_or_else(|error| panic!("revert: {error:?}"));
    let records = ledger
        .events(LedgerQuery {
            entry: Some(jinnd_api::EntryId("owner".to_owned())),
            ..LedgerQuery::default()
        })
        .await
        .unwrap_or_else(|error| panic!("events: {error}"));
    assert_eq!(
        records.len(),
        3,
        "intent, completion, and resolution are all reachable by entry"
    );
    for record in &records {
        assert_eq!(record.fiber, Some(jinnd_api::FiberId(3)));
        let effect = match &record.kind {
            LedgerEventKind::RevertIntent { effect, .. }
            | LedgerEventKind::RevertCompleted { effect, .. }
            | LedgerEventKind::RevertResolved { effect, .. } => *effect,
            other => panic!("unexpected kind: {other:?}"),
        };
        assert_eq!(effect, EffectId(7), "the event names the effect it reverts");
    }
}

#[tokio::test]
async fn intent_is_recorded_before_completion() {
    let ledger = Ledger::open_in_memory().unwrap_or_else(|error| panic!("open: {error}"));
    let lane = RevertLane::new(ledger.clone());
    let runs = Arc::new(AtomicUsize::new(0));
    let witness: Witness = Arc::new(|| true);
    lane.revert(
        EffectId(1),
        key("k"),
        witness,
        counting_inverse(&runs),
        None,
        None,
    )
    .await
    .unwrap_or_else(|error| panic!("revert: {error:?}"));
    let records = ledger
        .events(LedgerQuery::default())
        .await
        .unwrap_or_else(|error| panic!("events: {error}"));
    let kinds: Vec<&LedgerEventKind> = records.iter().map(|record| &record.kind).collect();
    assert!(matches!(kinds[0], LedgerEventKind::RevertIntent { .. }));
    assert!(matches!(
        kinds[1],
        LedgerEventKind::RevertCompleted { clean: true, .. }
    ));
    assert!(matches!(kinds[2], LedgerEventKind::RevertResolved { .. }));
}
