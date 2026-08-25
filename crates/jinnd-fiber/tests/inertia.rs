//! The single-flight inertia loop: a launched transition always lands, and the
//! targets that arrive while it is in flight coalesce instead of racing it.

#![cfg(not(feature = "loom"))]

mod support;

use jinnd_api::{FiberState, TransitionCause};
use jinnd_fiber::{Fiber, ReadinessSource};
use support::{Gate, Trace, body, epoch, gated, gated_undo, path, ready, undo};

/// TS origin: `packages/core/tests/fiber.spec.ts`, `inertia lock 1` — a dependency
/// withdrawn mid-load does not cancel the launched load; the load lands, and only
/// then does the fiber unload.
#[tokio::test]
async fn a_target_change_during_loading_never_launches_a_second_transition() {
    let trace = Trace::new();
    let gate = Gate::new();
    let (fiber, source) = ready(gated(&trace, "alpha", &gate));
    gate.entered(1).await;

    source.withdraw();
    assert_eq!(fiber.state(), FiberState::Loading);

    gate.release();
    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Pending);
    assert_eq!(
        trace.entries(),
        vec!["load:alpha", "land:alpha", "undo:alpha"]
    );
    assert_eq!(trace.count("load:alpha"), 1);
}

/// An activation that is already stale when it lands never publishes `Active`: the
/// states a fiber shows are states it actually rests in.
#[tokio::test]
async fn a_stale_activation_lands_straight_into_unloading() {
    let trace = Trace::new();
    let gate = Gate::new();
    let (fiber, source) = ready(gated(&trace, "alpha", &gate));
    gate.entered(1).await;

    source.withdraw();
    gate.release();
    fiber.quiesce().await;

    assert_eq!(
        path(&fiber.record().transitions),
        vec![
            FiberState::Loading,
            FiberState::Unloading,
            FiberState::Pending
        ]
    );
}

/// TS origin: `packages/core/tests/fiber.spec.ts`, `inertia lock 2` — a target that
/// changes and changes back while a load is in flight coalesces to no work at all.
#[tokio::test]
async fn a_target_that_returns_to_itself_during_loading_coalesces_to_nothing() {
    let trace = Trace::new();
    let gate = Gate::new();
    let (fiber, source) = ready(gated(&trace, "alpha", &gate));
    gate.entered(1).await;

    source.withdraw();
    source.ready(epoch(1));
    gate.release();
    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(trace.entries(), vec!["load:alpha", "land:alpha"]);
    assert_eq!(
        path(&fiber.record().transitions),
        vec![FiberState::Loading, FiberState::Active]
    );
}

/// A dependency that comes back as a *different* generation is a real change: the
/// landed activation is withdrawn and the fiber reloads against the new epoch.
#[tokio::test]
async fn a_new_dependency_generation_during_loading_forces_exactly_one_reload() {
    let trace = Trace::new();
    let gate = Gate::new();
    let (fiber, source) = ready(gated(&trace, "alpha", &gate));
    gate.entered(1).await;

    source.ready(epoch(2));
    gate.release();
    gate.entered(2).await;
    gate.release();
    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(
        trace.entries(),
        vec![
            "load:alpha",
            "land:alpha",
            "undo:alpha",
            "load:alpha",
            "land:alpha"
        ]
    );
}

/// TS origin: `packages/core/tests/fiber.spec.ts`, `inertia lock 1` tail — a target
/// restored while the unload is in flight reloads once the unload has landed.
#[tokio::test]
async fn a_target_restored_during_unloading_reloads_after_the_unload_lands() {
    let trace = Trace::new();
    let gate = Gate::new();
    let recorded = trace.clone();
    let held = gate.clone();
    let fiber_body = body(move |mut setup| {
        let recorded = recorded.clone();
        let held = held.clone();
        Box::pin(async move {
            recorded.push("load:alpha");
            setup.effect("alpha", gated_undo(&recorded, "alpha", &held))?;
            Ok(())
        })
    });
    let (fiber, source) = ready(fiber_body);
    fiber.quiesce().await;

    source.withdraw();
    gate.entered(1).await;
    assert_eq!(fiber.state(), FiberState::Unloading);

    source.ready(epoch(1));
    gate.release();
    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(
        trace.entries(),
        vec!["load:alpha", "undo:alpha", "load:alpha"]
    );
}

/// Many targets arriving during one in-flight transition coalesce into the latest
/// one — intermediate targets are neither lost nor separately serviced.
#[tokio::test]
async fn intermediate_targets_coalesce_into_the_latest_one() {
    let trace = Trace::new();
    let gate = Gate::new();
    let (fiber, source) = ready(gated(&trace, "alpha", &gate));
    gate.entered(1).await;

    source.withdraw();
    source.ready(epoch(2));
    source.withdraw();
    source.ready(epoch(3));
    fiber.restart(TransitionCause::ConfigChanged);
    gate.release();
    gate.entered(2).await;
    gate.release();
    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(trace.count("load:alpha"), 2);
    assert_eq!(trace.count("undo:alpha"), 1);
}

/// An activation whose target went stale is told so, cooperatively: the token lets a
/// body stop early, and it never aborts the transition from outside (R1).
#[tokio::test]
async fn a_stale_activation_is_told_cooperatively_and_still_lands() {
    let trace = Trace::new();
    let gate = Gate::new();
    let recorded = trace.clone();
    let held = gate.clone();
    let fiber_body = body(move |mut setup| {
        let recorded = recorded.clone();
        let held = held.clone();
        Box::pin(async move {
            setup.effect("early", undo(&recorded, "early"))?;
            held.enter().await;
            // Waiting on the token proves the fiber tells a stale activation so,
            // and that the activation still gets to finish on its own terms.
            setup.cancellation().cancelled().await;
            recorded.push("stale");
            setup.effect("late", undo(&recorded, "late"))?;
            Ok(())
        })
    });
    let (fiber, source) = ready(fiber_body);
    gate.entered(1).await;
    gate.release();

    source.withdraw();
    fiber.quiesce().await;

    assert_eq!(trace.entries(), vec!["stale", "undo:late", "undo:early"]);
}

/// A body that is not stale is never told it is.
#[tokio::test]
async fn an_activation_whose_target_holds_is_never_told_it_is_stale() {
    let trace = Trace::new();
    let gate = Gate::new();
    let recorded = trace.clone();
    let held = gate.clone();
    let source = ReadinessSource::new(Some(epoch(1)));
    let fiber = Fiber::spawn(
        body(move |setup| {
            let recorded = recorded.clone();
            let held = held.clone();
            Box::pin(async move {
                held.enter().await;
                recorded.push(if setup.cancelled() { "stale" } else { "live" });
                Ok(())
            })
        }),
        source.signal(),
    );
    gate.entered(1).await;

    gate.release();
    fiber.quiesce().await;

    assert_eq!(trace.entries(), vec!["live"]);
    assert_eq!(fiber.state(), FiberState::Active);
}
