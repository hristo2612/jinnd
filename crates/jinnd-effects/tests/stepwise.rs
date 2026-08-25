//! Stepwise inverses and the cancellation point between their steps.

mod support;

use jinnd_effects::{Disposer, EffectScope, UndoOutcome, step};
use support::{Trace, error, recorded, registered, step_with_panicking_destructor};
use tokio_util::sync::CancellationToken;

fn recording_step(trace: &Trace, label: &'static str) -> jinnd_effects::UndoStep {
    let trace = trace.clone();
    step(move || {
        trace.push(label);
        Ok(())
    })
}

#[tokio::test]
async fn a_stepwise_inverse_runs_its_steps_in_the_order_given() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    registered(scope.register(
        "stepwise",
        Disposer::stepwise(
            vec![
                recording_step(&trace, "one"),
                recording_step(&trace, "two"),
                recording_step(&trace, "three"),
            ],
            CancellationToken::new(),
        ),
    ));

    let report = scope.replay().await;

    assert_eq!(trace.entries(), vec!["one", "two", "three"]);
    assert!(report.is_clean());
}

/// The cancellation point sits between steps: a step that is running is never torn
/// out from under itself, and the steps after the point never start.
#[tokio::test]
async fn cancellation_between_steps_stops_the_sequence_and_reports_the_split() {
    let trace = Trace::new();
    let cancel = CancellationToken::new();
    let mut scope = EffectScope::new();
    let canceller = {
        let trace = trace.clone();
        let cancel = cancel.clone();
        step(move || {
            trace.push("two");
            cancel.cancel();
            Ok(())
        })
    };
    registered(scope.register(
        "stepwise",
        Disposer::stepwise(
            vec![
                recording_step(&trace, "one"),
                canceller,
                recording_step(&trace, "three"),
                recording_step(&trace, "four"),
            ],
            cancel,
        ),
    ));

    let report = scope.replay().await;

    assert_eq!(trace.entries(), vec!["one", "two"]);
    assert_eq!(
        report.effects.first().map(|effect| effect.outcome.clone()),
        Some(UndoOutcome::Cancelled {
            completed: 2,
            remaining: 2
        })
    );
    assert!(!report.is_clean());
}

#[tokio::test]
async fn a_cancelled_stepwise_inverse_does_not_stop_the_replay() {
    let trace = Trace::new();
    let cancel = CancellationToken::new();
    cancel.cancel();
    let mut scope = EffectScope::new();
    registered(scope.register("bottom", recorded(&trace, "bottom")));
    registered(scope.register(
        "cancelled",
        Disposer::stepwise(vec![recording_step(&trace, "never")], cancel),
    ));
    registered(scope.register("top", recorded(&trace, "top")));

    let report = scope.replay().await;

    assert_eq!(trace.entries(), vec!["top", "bottom"]);
    assert_eq!(
        report.effects.get(1).map(|effect| effect.outcome.clone()),
        Some(UndoOutcome::Cancelled {
            completed: 0,
            remaining: 1
        })
    );
    assert_eq!(report.effects.len(), 3);
}

#[tokio::test]
async fn a_failing_step_stops_its_own_sequence_and_leaves_the_replay_running() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    registered(scope.register("bottom", recorded(&trace, "bottom")));
    let failing_step = {
        let trace = trace.clone();
        step(move || {
            trace.push("two");
            Err(error("two"))
        })
    };
    registered(scope.register(
        "stepwise",
        Disposer::stepwise(
            vec![
                recording_step(&trace, "one"),
                failing_step,
                recording_step(&trace, "three"),
            ],
            CancellationToken::new(),
        ),
    ));

    let report = scope.replay().await;

    assert_eq!(trace.entries(), vec!["one", "two", "bottom"]);
    assert_eq!(
        report.effects.first().map(|effect| effect.outcome.clone()),
        Some(UndoOutcome::Failed(error("two")))
    );
    assert_eq!(report.effects.len(), 2);
}

#[tokio::test]
async fn a_panicking_step_stops_its_own_sequence_and_is_contained() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    registered(scope.register("bottom", recorded(&trace, "bottom")));
    let bad_step = {
        let trace = trace.clone();
        step(move || {
            trace.push("two");
            panic!("step two could not be undone");
        })
    };
    registered(scope.register(
        "stepwise",
        Disposer::stepwise(
            vec![
                recording_step(&trace, "one"),
                bad_step,
                recording_step(&trace, "three"),
            ],
            CancellationToken::new(),
        ),
    ));

    let report = scope.replay().await;

    assert_eq!(trace.entries(), vec!["one", "two", "bottom"]);
    assert_eq!(
        report.effects.first().map(|effect| effect.outcome.clone()),
        Some(UndoOutcome::Panicked(
            "step two could not be undone".to_owned()
        ))
    );
}

/// A sequence that stops early drops the steps it never ran, and those closures are
/// plugin-authored too. R11: their destructors cannot unwind past this crate.
#[tokio::test]
async fn a_panicking_destructor_on_an_unrun_step_is_contained() {
    let trace = Trace::new();
    let mut scope = EffectScope::new();
    registered(scope.register("bottom", recorded(&trace, "bottom")));
    let failing_step = {
        let trace = trace.clone();
        step(move || {
            trace.push("one");
            Err(error("one"))
        })
    };
    registered(scope.register(
        "stepwise",
        Disposer::stepwise(
            vec![failing_step, step_with_panicking_destructor("unrun")],
            CancellationToken::new(),
        ),
    ));

    let report = scope.replay().await;

    assert_eq!(trace.entries(), vec!["one", "bottom"]);
    assert_eq!(
        report.effects.first().map(|effect| effect.outcome.clone()),
        Some(UndoOutcome::Panicked(
            "unrun left a panicking destructor".to_owned()
        ))
    );
    assert_eq!(report.effects.len(), 2);
}

/// The same holds at the cancellation point: the steps a cancelled sequence never
/// reaches are discarded inside the containment, not outside it.
#[tokio::test]
async fn a_panicking_destructor_on_a_cancelled_step_is_contained() {
    let trace = Trace::new();
    let cancel = CancellationToken::new();
    cancel.cancel();
    let mut scope = EffectScope::new();
    registered(scope.register("bottom", recorded(&trace, "bottom")));
    registered(scope.register(
        "stepwise",
        Disposer::stepwise(vec![step_with_panicking_destructor("unreached")], cancel),
    ));

    let report = scope.replay().await;

    assert_eq!(trace.entries(), vec!["bottom"]);
    assert_eq!(
        report.effects.first().map(|effect| effect.outcome.clone()),
        Some(UndoOutcome::Panicked(
            "unreached left a panicking destructor".to_owned()
        ))
    );
    assert_eq!(report.effects.len(), 2);
}
