//! Failure is local (R11), it is recorded rather than retried (R9), and whatever a
//! failed activation had already applied is withdrawn exactly (I1's seed).

#![cfg(not(feature = "loom"))]

mod support;

use jinnd_api::{ErrorCode, FiberState, TransitionCause};
use jinnd_effects::{Disposer, UndoOutcome};
use jinnd_fiber::{Fiber, ReadinessSource};
use support::{Gate, Trace, body, epoch, failure, path, ready, recording, undo};

/// A body that fails half-way withdraws exactly the effects it had registered, in
/// reverse, and nothing else.
#[tokio::test]
async fn a_failing_body_unwinds_exactly_the_effects_it_had_registered() {
    let trace = Trace::new();
    let recorded = trace.clone();
    let (fiber, _source) = ready(body(move |mut setup| {
        let recorded = recorded.clone();
        Box::pin(async move {
            setup.effect("first", undo(&recorded, "first"))?;
            setup.effect("second", undo(&recorded, "second"))?;
            Err(failure("the body could not finish"))
        })
    }));

    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Failed);
    assert_eq!(trace.entries(), vec!["undo:second", "undo:first"]);
    assert_eq!(
        path(&fiber.record().transitions),
        vec![
            FiberState::Loading,
            FiberState::Unloading,
            FiberState::Failed
        ]
    );
    let record = fiber.record();
    assert_eq!(record.failures.len(), 1);
    assert_eq!(record.failures[0].fiber, Some(fiber.id()));
    assert!(fiber.effects().is_empty());
}

/// A panic in a plugin body never crosses the kernel boundary (R11); it is converted
/// to a recorded failure of that one fiber.
#[tokio::test]
async fn a_panicking_body_is_contained_and_recorded() {
    let trace = Trace::new();
    let recorded = trace.clone();
    let (fiber, _source) = ready(body(move |mut setup| {
        let recorded = recorded.clone();
        Box::pin(async move {
            setup.effect("applied", undo(&recorded, "applied"))?;
            panic!("the plugin body panicked");
        })
    }));

    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Failed);
    assert_eq!(trace.entries(), vec!["undo:applied"]);
    let record = fiber.record();
    assert_eq!(record.failures[0].code, ErrorCode::PluginFailed);
    assert!(record.failures[0].message.contains("panicked"));
}

/// R9: a failed fiber is never retried against an environment that did not change.
#[tokio::test]
async fn a_failed_fiber_is_not_retried_against_an_unchanged_environment() {
    let trace = Trace::new();
    let recorded = trace.clone();
    let (fiber, _source) = ready(body(move |_setup| {
        let recorded = recorded.clone();
        Box::pin(async move {
            recorded.push("load");
            Err(failure("always fails"))
        })
    }));
    fiber.quiesce().await;
    let settled = fiber.record();

    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Failed);
    assert_eq!(trace.count("load"), 1);
    assert_eq!(fiber.record().transitions, settled.transitions);
}

/// A changed environment is a new attempt, not a retry of the old one.
#[tokio::test]
async fn a_failed_fiber_reloads_when_its_dependencies_change() {
    let trace = Trace::new();
    let recorded = trace.clone();
    let (fiber, source) = ready(body(move |_setup| {
        let recorded = recorded.clone();
        Box::pin(async move {
            recorded.push("load");
            if recorded.count("load") == 1 {
                return Err(failure("the first attempt fails"));
            }
            Ok(())
        })
    }));
    fiber.quiesce().await;
    assert_eq!(fiber.state(), FiberState::Failed);

    source.ready(epoch(2));
    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(trace.count("load"), 2);
}

/// An explicit restart is also a changed environment: the operator asked.
#[tokio::test]
async fn an_explicit_restart_reattempts_a_failed_fiber() {
    let trace = Trace::new();
    let recorded = trace.clone();
    let (fiber, _source) = ready(body(move |_setup| {
        let recorded = recorded.clone();
        Box::pin(async move {
            recorded.push("load");
            if recorded.count("load") == 1 {
                return Err(failure("the first attempt fails"));
            }
            Ok(())
        })
    }));
    fiber.quiesce().await;

    fiber.restart(TransitionCause::ConfigChanged);
    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(trace.count("load"), 2);
}

/// An inverse that fails is contained and reported, the inverses behind it still
/// run, and the fiber is honest about not having withdrawn cleanly.
#[tokio::test]
async fn an_undo_failure_is_contained_and_the_remaining_inverses_still_run() {
    let trace = Trace::new();
    let recorded = trace.clone();
    let (fiber, source) = ready(body(move |mut setup| {
        let recorded = recorded.clone();
        Box::pin(async move {
            setup.effect("first", undo(&recorded, "first"))?;
            let refuses = recorded.clone();
            setup.effect(
                "second",
                Disposer::sync(move || {
                    refuses.push("undo:second");
                    Err(failure("this inverse refuses"))
                }),
            )?;
            Ok(())
        })
    }));
    fiber.quiesce().await;

    source.withdraw();
    fiber.quiesce().await;

    assert_eq!(trace.entries(), vec!["undo:second", "undo:first"]);
    assert_eq!(fiber.state(), FiberState::Failed);
    let record = fiber.record();
    assert!(!record.replays[0].is_clean());
    assert!(matches!(
        record.replays[0].effects[0].outcome,
        UndoOutcome::Failed(_)
    ));
}

/// Disposal is terminal even when a withdrawal was not clean: the residue is
/// reported, never hidden behind a state that says the fiber is still alive.
#[tokio::test]
async fn an_unclean_disposal_still_reaches_disposed_and_reports_the_residue() {
    let trace = Trace::new();
    let recorded = trace.clone();
    let (fiber, _source) = ready(body(move |mut setup| {
        let recorded = recorded.clone();
        Box::pin(async move {
            let refuses = recorded.clone();
            setup.effect(
                "stubborn",
                Disposer::sync(move || {
                    refuses.push("undo:stubborn");
                    Err(failure("this inverse refuses"))
                }),
            )?;
            Ok(())
        })
    }));
    fiber.quiesce().await;

    fiber.dispose().await;

    assert_eq!(fiber.state(), FiberState::Disposed);
    assert_eq!(trace.entries(), vec!["undo:stubborn"]);
    assert!(!fiber.record().replays[0].is_clean());
}

/// A body that stops early on cancellation leaves exactly its partial contribution,
/// and exactly that much is withdrawn.
#[tokio::test]
async fn cancellation_mid_loading_unwinds_exactly_the_partial_effects() {
    let trace = Trace::new();
    let gate = Gate::new();
    let recorded = trace.clone();
    let held = gate.clone();
    let (fiber, source) = ready(body(move |mut setup| {
        let recorded = recorded.clone();
        let held = held.clone();
        Box::pin(async move {
            setup.effect("first", undo(&recorded, "first"))?;
            held.enter().await;
            // Waiting on the token is what makes the hand-off observable: the body
            // resumes exactly when the fiber tells it the target moved.
            setup.cancellation().cancelled().await;
            // Stopping here is the point: the rest of this activation is already
            // owed a withdrawal, so it is never applied in the first place.
            Ok(())
        })
    }));
    gate.entered(1).await;
    gate.release();

    source.withdraw();
    fiber.quiesce().await;

    assert_eq!(trace.entries(), vec!["undo:first"]);
    let record = fiber.record();
    assert_eq!(record.replays[0].effects.len(), 1);
    assert_eq!(record.replays[0].effects[0].label, "first");
}

/// R11: a sibling never observes another fiber's failure.
#[tokio::test]
async fn a_failing_fiber_leaves_its_sibling_untouched() {
    let trace = Trace::new();
    let recorded = trace.clone();
    let failing_source = ReadinessSource::new(Some(epoch(1)));
    let failing = Fiber::spawn(
        body(move |_setup| {
            let recorded = recorded.clone();
            Box::pin(async move {
                recorded.push("load:failing");
                Err(failure("this one fails"))
            })
        }),
        failing_source.signal(),
    );
    let healthy_source = ReadinessSource::new(Some(epoch(1)));
    let healthy = Fiber::spawn(recording(&trace, "healthy"), healthy_source.signal());

    failing.quiesce().await;
    healthy.quiesce().await;

    assert_eq!(failing.state(), FiberState::Failed);
    assert_eq!(healthy.state(), FiberState::Active);
    assert!(healthy.record().failures.is_empty());
    assert_eq!(trace.count("load:healthy"), 1);
}
