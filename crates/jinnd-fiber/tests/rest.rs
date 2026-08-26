//! The rest observation (M1-P6c round 2): a fiber is at rest exactly when it
//! owes no transition — the committed state equals the desired one and none
//! is in flight. The loader's amendment gate builds on the causal half: code
//! a transition itself runs (the body, and tasks the body spawns and awaits)
//! never observes its own fiber at rest.

#![cfg(not(feature = "loom"))]

mod support;

use std::sync::{Arc, Mutex};

use jinnd_api::FiberState;
use jinnd_fiber::{Fiber, ReadinessSource};
use support::{Gate, Trace, body, epoch, gated, ready};

#[tokio::test]
async fn a_fiber_rests_between_transitions_and_never_during_one() {
    let trace = Trace::new();
    let gate = Gate::new();
    let (fiber, _source) = ready(gated(&trace, "alpha", &gate));

    // The activation is in flight: the fiber owes its landing.
    gate.entered(1).await;
    assert!(!fiber.resting(), "an activation in flight is not at rest");

    gate.release();
    fiber.quiesce().await;
    assert_eq!(fiber.state(), FiberState::Active);
    assert!(fiber.resting(), "a settled fiber is at rest");

    // Disposal owes a withdrawal; once it lands the fiber rests for good.
    fiber.dispose().await;
    assert_eq!(fiber.state(), FiberState::Disposed);
    assert!(fiber.resting(), "a disposed fiber owes nothing");
}

type Slot = Arc<Mutex<Option<Arc<Fiber>>>>;

/// The awaited-helper deadlock shape (M1-P6c round 2): the body spawns a task
/// asking about its own fiber and awaits the answer. However many task
/// boundaries the question crosses, it happens-after the transition began, so
/// it must never observe the fiber at rest.
#[tokio::test]
async fn a_task_spawned_and_awaited_by_the_body_never_observes_rest() {
    let slot: Slot = Arc::new(Mutex::new(None));
    let observed: Arc<Mutex<Option<bool>>> = Arc::new(Mutex::new(None));
    let asked = {
        let slot = Arc::clone(&slot);
        let observed = Arc::clone(&observed);
        body(move |_setup| {
            let slot = Arc::clone(&slot);
            let observed = Arc::clone(&observed);
            Box::pin(async move {
                let Some(fiber) = slot
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .clone()
                else {
                    return Ok(());
                };
                let probe = tokio::spawn(async move { fiber.resting() });
                let seen = probe.await.unwrap_or(true);
                *observed.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(seen);
                Ok(())
            })
        })
    };

    // Readiness is withheld until the body can reach its own handle.
    let source = ReadinessSource::new(None);
    let fiber = Arc::new(Fiber::spawn(asked, source.signal()));
    *slot.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(Arc::clone(&fiber));
    source.ready(epoch(1));
    fiber.quiesce().await;

    assert_eq!(
        *observed.lock().unwrap_or_else(|poison| poison.into_inner()),
        Some(false),
        "work launched and awaited by an activation saw its own fiber at rest"
    );
    assert!(
        fiber.resting(),
        "the fiber rests once the activation landed"
    );
}
