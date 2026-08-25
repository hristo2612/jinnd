//! A dropped replay is a pause, not a discharge.
//!
//! Replay is an ordinary future, so it can be dropped at any suspension point — a
//! `select!` losing its race, a timeout, a cancelled task. What was never touched
//! must still be withdrawable afterwards, and the one inverse that was in flight
//! must be reported rather than silently lost (I1, R6, R11).

mod support;

use jinnd_api::ErrorCode;
use jinnd_effects::{EffectScope, UndoOutcome};
use support::{Trace, poll_pending, recorded, registered, stuck, stuck_panicking_destructor};

/// Exactly-once across an interruption: what already ran is not re-run, what never
/// started is still replayable, and the inverse in flight is reported, not dropped.
#[tokio::test]
async fn a_dropped_replay_pauses_the_teardown_instead_of_discharging_it() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    registered(scope.register("bottom", recorded(&trace, "bottom")));
    registered(scope.register("in-flight", stuck()));
    registered(scope.register("top", recorded(&trace, "top")));

    {
        let mut replay = Box::pin(scope.replay());
        poll_pending(replay.as_mut());
    }

    assert_eq!(trace.entries(), vec!["top"]);

    let report = scope.replay().await;

    assert_eq!(
        trace.entries(),
        vec!["top", "bottom"],
        "an inverse runs exactly once, whatever the replay did"
    );
    assert_eq!(
        report
            .effects
            .iter()
            .map(|effect| (effect.label.as_str(), effect.outcome.clone()))
            .collect::<Vec<_>>(),
        vec![
            ("top", UndoOutcome::Done),
            ("in-flight", UndoOutcome::Interrupted { panic: None }),
            ("bottom", UndoOutcome::Done),
        ],
        "the resumed report opens with what the interrupted replay had already done"
    );
    assert!(!report.is_clean());
    assert!(scope.is_empty());
}

/// The effects a paused teardown has not reached are still live: they keep their
/// place in the tree, and the scope does not pretend to be empty.
#[tokio::test]
async fn a_paused_teardown_still_holds_the_effects_it_never_reached() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    let parent = registered(scope.register("parent", recorded(&trace, "parent")));
    registered(scope.register_child(parent, "child", recorded(&trace, "child")));
    registered(scope.register("in-flight", stuck()));

    {
        let mut replay = Box::pin(scope.replay());
        poll_pending(replay.as_mut());
    }

    assert!(!scope.is_empty(), "a paused teardown is not a finished one");
    let tree = scope.tree();
    assert_eq!(
        tree.first().map(|effect| effect.label.as_str()),
        Some("parent")
    );
    assert_eq!(
        tree.first().map(|effect| effect.children.len()),
        Some(1),
        "the nesting the rest of the teardown depends on survives the interruption"
    );
    assert_eq!(
        scope
            .register("late", recorded(&trace, "late"))
            .err()
            .map(|error| error.code),
        Some(ErrorCode::InactiveContext),
        "teardown has begun: nothing new may join it"
    );
    assert!(trace.entries().is_empty());
}

/// The one place "the replay went away" is observable is the in-flight inverse's own
/// destructor. A panic raised there is plugin-authored too: contained and recorded.
#[tokio::test]
async fn a_destructor_panic_while_cancelling_an_inverse_is_contained_and_recorded() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    registered(scope.register("bottom", recorded(&trace, "bottom")));
    registered(scope.register("in-flight", stuck_panicking_destructor(&trace, "in-flight")));

    {
        let mut replay = Box::pin(scope.replay());
        poll_pending(replay.as_mut());
    }

    assert_eq!(
        scope
            .withdrawn()
            .first()
            .map(|effect| effect.outcome.clone()),
        Some(UndoOutcome::Interrupted {
            panic: Some("in-flight left a panicking destructor".to_owned())
        })
    );

    let report = scope.replay().await;

    assert_eq!(trace.entries(), vec!["bottom"]);
    assert_eq!(report.effects.len(), 2);
    assert!(!report.is_clean());
}

/// Nothing is withheld from a replay that ran to completion: the carry-over slot is
/// the interrupted case only.
#[tokio::test]
async fn a_completed_replay_carries_nothing_over() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    registered(scope.register("only", recorded(&trace, "only")));

    let report = scope.replay().await;

    assert_eq!(report.effects.len(), 1);
    assert!(scope.withdrawn().is_empty());
}
