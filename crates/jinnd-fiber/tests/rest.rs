//! The rest observation (M1-P6c round 2): a fiber is at rest exactly when it
//! owes no transition — the committed state equals the desired one and none
//! is in flight. The loader's amendment gate builds on the causal half: code
//! a transition itself runs (the body, and tasks the body spawns and awaits)
//! never observes its own fiber at rest.

#![cfg(not(feature = "loom"))]

mod support;

use std::sync::{Arc, Mutex};

use jinnd_api::{FiberState, Owed};
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

/// The round-3 law (M1-P6c): rest lowers ATOMICALLY with the target write —
/// in restart's own critical section, never deferred to supervisor
/// scheduling. The assert runs with no await after the restart, so on a
/// current-thread runtime the supervisor provably has not run: the answer
/// must already be `false` the moment `restart` returns.
#[tokio::test]
async fn a_restated_target_is_never_observed_at_rest() {
    let (fiber, _source) = ready(body(|_setup| Box::pin(async { Ok(()) })));
    fiber.quiesce().await;
    assert!(fiber.resting(), "a settled fiber is at rest");

    fiber.restart(jinnd_api::TransitionCause::ExplicitRestart);
    assert!(
        !fiber.resting(),
        "the moment restart returns, committed != target: never at rest"
    );

    fiber.quiesce().await;
    assert!(fiber.resting(), "the reload landed: at rest again");
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

/// A body whose single inverse blocks at `gate`: the withdrawal the fiber
/// owes stays observable for exactly as long as the test wants it.
fn holding(trace: &Trace, gate: &Gate) -> Arc<dyn jinnd_fiber::FiberBody> {
    let (trace, gate) = (trace.clone(), gate.clone());
    body(move |mut setup| {
        let (trace, gate) = (trace.clone(), gate.clone());
        Box::pin(async move {
            setup.effect("held", support::gated_undo(&trace, "held", &gate))?;
            Ok(())
        })
    })
}

/// M2-K9: WHAT a fiber owes is a typed answer, never a bit. The three
/// dispositions are genuinely different futures for anyone waiting on the
/// fiber, so the kernel names them apart at the one place it knows: a
/// caller held off by a fiber that is being DISPOSED must never be told to
/// wait for a restart that is not coming, and one held off by a suspension
/// must not be told the same either — a resume may never arrive on its own.
#[tokio::test]
async fn a_fiber_names_what_it_owes_and_never_calls_a_disposal_a_reload() {
    // A reload: the target moved and this incarnation is REPLACED.
    let (fiber, _source) = ready(body(|_setup| Box::pin(async { Ok(()) })));
    fiber.quiesce().await;
    assert_eq!(fiber.owed(), None, "a settled fiber owes nothing");
    fiber.restart(jinnd_api::TransitionCause::ExplicitRestart);
    assert_eq!(
        fiber.owed(),
        Some(Owed::Reload),
        "a restart replaces the incarnation"
    );
    fiber.quiesce().await;
    assert_eq!(fiber.owed(), None, "the reload landed");

    // A disposal, observed from INSIDE its own withdrawal replay — exactly
    // the window a refused caller is told about. Terminal: nothing comes
    // after it, so the honest answer is not `Reload`.
    let trace = Trace::new();
    let gate = Gate::new();
    let (doomed, _doomed_source) = ready(holding(&trace, &gate));
    let doomed = Arc::new(doomed);
    doomed.quiesce().await;
    let withdrawal = tokio::spawn({
        let doomed = Arc::clone(&doomed);
        async move { doomed.dispose().await }
    });
    gate.entered(1).await;
    assert_eq!(
        doomed.owed(),
        Some(Owed::Disposal),
        "a disposal is terminal: naming it a reload sends a caller to wait forever"
    );
    gate.release();
    withdrawal.await.unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(doomed.owed(), None, "a disposed fiber owes nothing more");

    // A suspension: the cell stops and the entry persists. A resume may
    // bring it back, and may never come — its own answer, not a reload.
    let paused = Trace::new();
    let resume = Gate::new();
    let (stopping, _stopping_source) = ready(holding(&paused, &resume));
    let stopping = Arc::new(stopping);
    stopping.quiesce().await;
    let suspension = tokio::spawn({
        let stopping = Arc::clone(&stopping);
        async move { stopping.suspend().await }
    });
    resume.entered(1).await;
    assert_eq!(
        stopping.owed(),
        Some(Owed::Suspension),
        "a suspension awaits a resume, not a restart"
    );
    resume.release();
    suspension.await.unwrap_or_else(|error| panic!("{error}"));
}
