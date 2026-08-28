//! Suspend ≠ dispose (M2-K4; decision log 2026-08-28). A fiber asked to
//! suspend releases its kernel registrations and keeps its world mutations:
//! the suspend replay runs each suspendable effect's suspend path and every
//! plain effect's undo, LIFO, then the fiber rests `Disposed` under the
//! `Suspend` cause — the cell is gone, the entry's contribution is not.
//! A restart is the same suspension followed by a fresh activation; only
//! disposal runs the full inverse trail.

#![cfg(not(feature = "loom"))]

mod support;

use std::sync::Arc;

use jinnd_api::{FiberState, TransitionCause};
use jinnd_fiber::FiberBody;
use support::{Trace, body, path, ready, undo};

fn worldly(trace: &Trace) -> Arc<dyn FiberBody> {
    let recorded = trace.clone();
    body(move |mut setup| {
        let recorded = recorded.clone();
        Box::pin(async move {
            setup.effect("listener", undo(&recorded, "listener"))?;
            setup.suspendable_effect(
                "world",
                undo(&recorded, "world"),
                undo(&recorded, "suspend-world"),
            )?;
            setup.effect("alarm", undo(&recorded, "alarm"))?;
            Ok(())
        })
    })
}

#[tokio::test]
async fn suspend_releases_registrations_keeps_the_world_and_rests_suspended() {
    let trace = Trace::new();
    let (fiber, _source) = ready(worldly(&trace));
    fiber.quiesce().await;
    assert_eq!(fiber.state(), FiberState::Active);

    fiber.suspend().await;

    assert_eq!(fiber.state(), FiberState::Disposed);
    assert_eq!(
        trace.entries(),
        vec!["undo:alarm", "undo:suspend-world", "undo:listener"]
    );
    let transitions = fiber.record().transitions;
    assert_eq!(
        path(&transitions),
        vec![
            FiberState::Loading,
            FiberState::Active,
            FiberState::Unloading,
            FiberState::Disposed
        ]
    );
    assert!(
        transitions
            .iter()
            .filter(|transition| transition.to == FiberState::Disposed)
            .all(|transition| transition.cause == TransitionCause::Suspend),
        "the terminal transition is a suspension, never a disposal: {transitions:?}"
    );
    assert!(fiber.effects().is_empty());
}

#[tokio::test]
async fn dispose_runs_the_full_inverse_trail() {
    let trace = Trace::new();
    let (fiber, _source) = ready(worldly(&trace));
    fiber.quiesce().await;

    fiber.dispose().await;

    assert_eq!(fiber.state(), FiberState::Disposed);
    assert_eq!(
        trace.entries(),
        vec!["undo:alarm", "undo:world", "undo:listener"]
    );
}

#[tokio::test]
async fn a_restart_suspends_then_reactivates() {
    let trace = Trace::new();
    let (fiber, _source) = ready(worldly(&trace));
    fiber.quiesce().await;

    fiber.restart(TransitionCause::ConfigChanged);
    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(
        trace.entries(),
        vec!["undo:alarm", "undo:suspend-world", "undo:listener"],
        "an incarnation replacement suspends: the world mutation is retained"
    );
}

/// A disposal that lands while a suspension is in flight still ends in a
/// disposal: the suspend replay lands, then the fiber finishes disposing.
#[tokio::test]
async fn a_disposal_after_a_suspension_still_lands_disposed() {
    let trace = Trace::new();
    let (fiber, _source) = ready(worldly(&trace));
    fiber.quiesce().await;
    fiber.suspend().await;
    fiber.dispose().await;
    assert_eq!(fiber.state(), FiberState::Disposed);
    assert_eq!(
        trace.entries(),
        vec!["undo:alarm", "undo:suspend-world", "undo:listener"],
        "nothing is withdrawn twice"
    );
}
