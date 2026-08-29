//! What a fiber owes, sampled at the TRANSITIONAL instants of a real
//! supervised lifecycle rather than at its resting ones (M2-K9 round 4).
//!
//! Three rounds each found one more state that falsely promised a
//! replacement, and each was found from OUTSIDE by driving a real fiber and
//! reading it mid-transition. The pure enumeration in `src/owed/tests.rs`
//! proves the law over the state the kernel can represent; these tests prove
//! the other half — that every instant a real lifecycle passes through IS
//! one of those representable states, sampled while the transition is in
//! flight and parked at a gate.
//!
//! Every case here reads the answer while a transition is genuinely
//! unfinished, then plays the lifecycle out and checks what the answer
//! PROMISED against what actually landed: a `Reload` is followed by a real
//! replacement, and a `Stalled` by nothing at all.

#![cfg(not(feature = "loom"))]

mod support;

use std::sync::Arc;

use jinnd_api::{FiberState, Owed, TransitionCause};
use jinnd_fiber::{FiberBody, Setup};
use support::{Gate, Trace, body, failure, gated_undo, ready};

/// A body that records one activation and registers an inverse that parks at
/// `gate`, so a test can read the fiber while its teardown is in flight.
fn parked(trace: &Trace, gate: &Gate) -> Arc<dyn FiberBody> {
    let trace = trace.clone();
    let gate = gate.clone();
    body(move |setup| activation(setup, trace.clone(), gate.clone(), Ok(())))
}

/// The same, for an activation that fails after it has applied an effect:
/// the cleanup that follows is what round 3 read as a coming restart.
fn parked_failing(trace: &Trace, gate: &Gate) -> Arc<dyn FiberBody> {
    let trace = trace.clone();
    let gate = gate.clone();
    body(move |setup| {
        activation(
            setup,
            trace.clone(),
            gate.clone(),
            Err(failure("the activation could not finish")),
        )
    })
}

fn activation<'a>(
    mut setup: Setup<'a>,
    trace: Trace,
    gate: Gate,
    outcome: Result<(), jinnd_api::KernelError>,
) -> jinnd_api::KernelFuture<'a, ()> {
    Box::pin(async move {
        trace.push("load");
        setup.effect("applied", gated_undo(&trace, "applied", &gate))?;
        outcome
    })
}

/// The verifier's round-3 probe, in-tree: a failed activation enters its
/// cleanup with the doom already decided, so the fiber owes a STALL from the
/// first instant of that cleanup — never a replacement R9 has already
/// forbidden. Round 3 answered `Reload` here because the failure was
/// committed only after the cleanup awaited.
#[tokio::test]
async fn a_failed_activation_owes_a_stall_from_the_first_instant_of_its_cleanup() {
    let trace = Trace::new();
    let gate = Gate::new();
    let (fiber, _source) = ready(parked_failing(&trace, &gate));

    // The inverse is parked: the cleanup is in flight and unfinished.
    gate.entered(1).await;
    assert_eq!(
        fiber.owed(),
        Some(Owed::Stalled),
        "promised a replacement while R9 was landing the failure"
    );

    gate.release();
    fiber.quiesce().await;
    assert_eq!(fiber.state(), FiberState::Failed);
    assert_eq!(
        trace.count("load"),
        1,
        "nothing was scheduled, exactly as the stall said"
    );
    assert_eq!(fiber.owed(), None, "a settled failure owes nothing further");
}

/// The packet's own case, and the one answer that PROMISES: a restart is
/// owed the moment its target lands, stays owed for the whole replacement —
/// including while the outgoing incarnation's teardown is parked — and the
/// replacement it promised actually lands.
#[tokio::test]
async fn a_restart_owes_a_reload_for_the_whole_replacement_and_it_lands() {
    let trace = Trace::new();
    let gate = Gate::new();
    let (fiber, _source) = ready(parked(&trace, &gate));
    fiber.quiesce().await;
    assert_eq!(fiber.owed(), None, "a live activation on its aim is served");

    fiber.restart(TransitionCause::ExplicitRestart);
    assert_eq!(
        fiber.owed(),
        Some(Owed::Reload),
        "the moment the target moves, a replacement is scheduled"
    );
    gate.entered(1).await;
    assert_eq!(
        fiber.owed(),
        Some(Owed::Reload),
        "the promise holds while the outgoing teardown is in flight"
    );

    gate.release();
    fiber.quiesce().await;
    assert_eq!(trace.count("load"), 2, "the promised replacement landed");
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(fiber.owed(), None);
    gate.release();
}

/// A dependency withdrawn mid-teardown: the unload is in flight and nothing
/// will follow it, so the answer is a stall and the fiber rests `Pending`
/// with no second activation.
#[tokio::test]
async fn a_teardown_with_no_environment_to_return_to_owes_a_stall() {
    let trace = Trace::new();
    let gate = Gate::new();
    let (fiber, source) = ready(parked(&trace, &gate));
    fiber.quiesce().await;

    source.withdraw();
    gate.entered(1).await;
    assert_eq!(
        fiber.owed(),
        Some(Owed::Stalled),
        "promised a restart with the dependency it needs withdrawn"
    );

    gate.release();
    fiber.quiesce().await;
    assert_eq!(fiber.state(), FiberState::Pending);
    assert_eq!(trace.count("load"), 1, "nothing was scheduled");
}

/// A disposal in flight is never sold as a coming restart: it is terminal,
/// and the caller is told exactly that while the replay is still parked —
/// so a caller that would have re-resolved is not talked into waiting for a
/// replacement that is never coming.
#[tokio::test]
async fn a_disposal_in_flight_owes_a_disposal_and_never_a_reload() {
    let trace = Trace::new();
    let gate = Gate::new();
    let (fiber, _source) = ready(parked(&trace, &gate));
    fiber.quiesce().await;
    let fiber = Arc::new(fiber);

    let disposing = {
        let fiber = Arc::clone(&fiber);
        tokio::spawn(async move { fiber.dispose().await })
    };
    gate.entered(1).await;
    assert_eq!(
        fiber.owed(),
        Some(Owed::Disposal),
        "a terminal withdrawal in flight was answered as something else"
    );

    gate.release();
    disposing.await.unwrap_or_else(|_| unreachable!());
    assert_eq!(fiber.state(), FiberState::Disposed);
    assert_eq!(trace.count("load"), 1);
    assert_eq!(fiber.owed(), None, "the disposal has been served in full");
}
