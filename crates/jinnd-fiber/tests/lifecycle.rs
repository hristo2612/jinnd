//! Legal transitions, the states nothing may skip, and what disposal is terminal for.

#![cfg(not(feature = "loom"))]

mod support;

use jinnd_api::{FiberState, TransitionCause};
use jinnd_effects::Disposer;
use jinnd_fiber::{Fiber, ReadinessSource};
use support::{Trace, body, epoch, path, ready, recording, undo};

/// A fiber whose dependencies are satisfied walks `Pending → Loading → Active` and
/// runs its body exactly once for that activation.
#[tokio::test]
async fn a_satisfied_fiber_loads_once_and_ends_active() {
    let trace = Trace::new();
    let (fiber, _source) = ready(recording(&trace, "alpha"));

    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(trace.entries(), vec!["load:alpha"]);
    assert_eq!(
        path(&fiber.record().transitions),
        vec![FiberState::Loading, FiberState::Active]
    );
    assert_eq!(
        fiber.record().transitions[0].cause,
        TransitionCause::InitialLoad
    );
}

/// Availability is the kernel's to manage (SOURCE-OF-TRUTH §3): an unsatisfied fiber
/// waits in `Pending` and its body never runs.
#[tokio::test]
async fn an_unsatisfied_fiber_stays_pending_and_never_runs_its_body() {
    let trace = Trace::new();
    let source = ReadinessSource::new(None);
    let fiber = Fiber::spawn(recording(&trace, "alpha"), source.signal());

    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Pending);
    assert!(trace.entries().is_empty());
    assert!(fiber.record().transitions.is_empty());
}

/// The readiness signal is what activates a fiber, and the fiber never polls for it.
#[tokio::test]
async fn readiness_arriving_after_the_spawn_activates_the_fiber() {
    let trace = Trace::new();
    let source = ReadinessSource::new(None);
    let fiber = Fiber::spawn(recording(&trace, "alpha"), source.signal());
    fiber.quiesce().await;

    source.ready(epoch(1));
    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(trace.entries(), vec!["load:alpha"]);
    assert_eq!(
        fiber.record().transitions[0].cause,
        TransitionCause::DependencyChanged
    );
}

/// Withdrawing a dependency drains the fiber back to `Pending` through `Unloading`,
/// replaying its scope on the way (R5).
#[tokio::test]
async fn withdrawing_a_dependency_unloads_the_fiber_and_replays_its_scope() {
    let trace = Trace::new();
    let (fiber, source) = ready(recording(&trace, "alpha"));
    fiber.quiesce().await;

    source.withdraw();
    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Pending);
    assert_eq!(trace.entries(), vec!["load:alpha", "undo:alpha"]);
    assert_eq!(
        path(&fiber.record().transitions),
        vec![
            FiberState::Loading,
            FiberState::Active,
            FiberState::Unloading,
            FiberState::Pending
        ]
    );
    assert!(fiber.effects().is_empty());
}

/// Effects are withdrawn last-registered-first, and each activation starts from an
/// empty scope: teardown order is the effect engine's, adopted per fiber.
#[tokio::test]
async fn an_activation_withdraws_its_own_effects_last_registered_first() {
    let trace = Trace::new();
    let recorded = trace.clone();
    let fiber_body = body(move |mut setup| {
        let recorded = recorded.clone();
        Box::pin(async move {
            for label in ["first", "second", "third"] {
                setup.effect(label, undo(&recorded, label))?;
            }
            Ok(())
        })
    });
    let (fiber, _source) = ready(fiber_body);
    fiber.quiesce().await;

    assert_eq!(
        fiber
            .effects()
            .iter()
            .map(|effect| effect.label.clone())
            .collect::<Vec<_>>(),
        vec!["first", "second", "third"]
    );

    fiber.dispose().await;

    assert_eq!(
        trace.entries(),
        vec!["undo:third", "undo:second", "undo:first"]
    );
}

/// An explicit restart unloads and reloads exactly once and ends active — the body
/// runs once per activation, not once per fiber lifetime.
#[tokio::test]
async fn an_explicit_restart_reactivates_once_and_ends_active() {
    let trace = Trace::new();
    let (fiber, _source) = ready(recording(&trace, "alpha"));
    fiber.quiesce().await;

    fiber.restart(TransitionCause::ExplicitRestart);
    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(
        trace.entries(),
        vec!["load:alpha", "undo:alpha", "load:alpha"]
    );
    assert_eq!(
        path(&fiber.record().transitions),
        vec![
            FiberState::Loading,
            FiberState::Active,
            FiberState::Unloading,
            FiberState::Pending,
            FiberState::Loading,
            FiberState::Active
        ]
    );
}

/// `Disposed` is reached only after the replay completed and reported (I1's seed).
#[tokio::test]
async fn disposal_reaches_disposed_only_after_the_replay_reported() {
    let trace = Trace::new();
    let (fiber, _source) = ready(recording(&trace, "alpha"));
    fiber.quiesce().await;

    fiber.dispose().await;

    assert_eq!(fiber.state(), FiberState::Disposed);
    let record = fiber.record();
    assert_eq!(record.replays.len(), 1);
    assert!(record.replays[0].is_clean());
    assert_eq!(
        record.replays[0]
            .effects
            .iter()
            .map(|effect| effect.label.clone())
            .collect::<Vec<_>>(),
        vec!["alpha"]
    );
    assert_eq!(*path(&record.transitions).last().unwrap_or(&FiberState::Pending), FiberState::Disposed);
}

/// Disposal is idempotent: the second call withdraws nothing and adds no transition.
#[tokio::test]
async fn disposing_twice_withdraws_each_effect_once() {
    let trace = Trace::new();
    let (fiber, _source) = ready(recording(&trace, "alpha"));
    fiber.quiesce().await;

    fiber.dispose().await;
    let after_first = fiber.record();
    fiber.dispose().await;

    assert_eq!(trace.count("undo:alpha"), 1);
    assert_eq!(fiber.record().transitions, after_first.transitions);
}

/// `Disposed` is terminal: a later restart or readiness change is not a transition
/// the fiber may take.
#[tokio::test]
async fn a_disposed_fiber_refuses_every_later_transition() {
    let trace = Trace::new();
    let (fiber, source) = ready(recording(&trace, "alpha"));
    fiber.quiesce().await;
    fiber.dispose().await;
    let settled = fiber.record();

    fiber.restart(TransitionCause::ExplicitRestart);
    source.ready(epoch(2));
    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Disposed);
    assert_eq!(fiber.record().transitions, settled.transitions);
    assert_eq!(trace.count("load:alpha"), 1);
}

/// A fiber disposed before it ever activated goes straight to `Disposed` without
/// pretending to unload something it never loaded.
#[tokio::test]
async fn disposing_a_pending_fiber_skips_unloading() {
    let trace = Trace::new();
    let source = ReadinessSource::new(None);
    let fiber = Fiber::spawn(recording(&trace, "alpha"), source.signal());
    fiber.quiesce().await;

    fiber.dispose().await;

    assert_eq!(path(&fiber.record().transitions), vec![FiberState::Disposed]);
    assert!(trace.entries().is_empty());
}

/// Uids are allocated once and never reused, whatever happened to the fiber that
/// held one before (R3).
#[tokio::test]
async fn fiber_uids_are_never_reused() {
    let mut seen = Vec::new();
    for _ in 0..4 {
        let source = ReadinessSource::new(Some(epoch(1)));
        let fiber = Fiber::spawn(
            body(|_setup| Box::pin(async { Ok(()) })),
            source.signal(),
        );
        fiber.quiesce().await;
        seen.push(fiber.id());
        fiber.dispose().await;
    }

    let mut unique = seen.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), seen.len());
}

/// Dropping the last handle tears the fiber down rather than leaking its supervisor.
#[tokio::test]
async fn dropping_the_last_handle_disposes_the_fiber() {
    let trace = Trace::new();
    let recorded = trace.clone();
    let (fiber, _source) = ready(body(move |mut setup| {
        let recorded = recorded.clone();
        Box::pin(async move {
            setup.effect(
                "leased",
                Disposer::sync(move || {
                    recorded.push("undo:leased");
                    Ok(())
                }),
            )?;
            Ok(())
        })
    }));
    fiber.quiesce().await;
    let watcher = fiber.states();

    drop(fiber);
    let mut watcher = watcher;
    while *watcher.borrow_and_update() != FiberState::Disposed {
        if watcher.changed().await.is_err() {
            break;
        }
    }

    assert_eq!(trace.entries(), vec!["undo:leased"]);
}
