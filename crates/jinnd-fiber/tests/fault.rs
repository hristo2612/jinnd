//! M2-K25 (c): an instance the kernel ends AFTER activation fails its own
//! fiber, on the record — the fiber engine's one post-activation input.
//! The body reports the death through the `FaultSink` its activation was
//! handed; the supervisor plans `Active → Unloading → Failed` under
//! `BodyFaulted`, withdraws exactly the activation's effects (I1), and
//! rests `Failed` until the environment moves (R9). A fault landing
//! mid-transition is reconciled by that transition's landing — one
//! terminal state, never a second withdrawal, never `Failed` after
//! `Disposed` (R1 single-flight; R11).

#![cfg(not(feature = "loom"))]

mod support;

use std::sync::{Arc, Mutex};

use jinnd_api::{ErrorCode, FiberState, TransitionCause};
use jinnd_fiber::{FaultSink, Fiber};
use support::{Gate, Trace, body, epoch, failure, gated_undo, path, ready, undo};

type Held = Arc<Mutex<Option<FaultSink>>>;

/// A body that registers one inverse and hands its fault sink out, so the
/// test can report the incarnation's death from outside — the way the
/// wasm lane's death watch does.
fn faultable(trace: &Trace, held: &Held, gate: Option<&Gate>) -> Arc<dyn jinnd_fiber::FiberBody> {
    let trace = trace.clone();
    let held = Arc::clone(held);
    let gate = gate.cloned();
    body(move |mut setup| {
        let trace = trace.clone();
        let held = Arc::clone(&held);
        let gate = gate.clone();
        Box::pin(async move {
            trace.push("load");
            let inverse = match &gate {
                Some(gate) => gated_undo(&trace, "seat", gate),
                None => undo(&trace, "seat"),
            };
            setup.effect("seat", inverse)?;
            *held.lock().unwrap_or_else(|p| p.into_inner()) = Some(setup.faults());
            Ok(())
        })
    })
}

fn sink(held: &Held) -> FaultSink {
    held.lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
        .unwrap_or_else(|| panic!("the body handed its sink out"))
}

async fn until(fiber: &Fiber, state: FiberState) {
    let mut states = fiber.states();
    let _ = states.wait_for(|current| *current == state).await;
}

/// The (c) shape: the live instance dies; the fiber fails ITSELF, under
/// its own cause, withdrawing exactly what it applied, the death on its
/// record with its own attribution — and rests there (R9, R11, Law 2).
#[tokio::test]
async fn a_fault_after_activation_fails_the_fiber_on_the_record() {
    let trace = Trace::new();
    let held: Held = Arc::default();
    let (fiber, _source) = ready(faultable(&trace, &held, None));
    fiber.quiesce().await;
    assert_eq!(fiber.state(), FiberState::Active);

    assert!(
        sink(&held).fault(failure("guest exceeded its call deadline")),
        "a fault of the live incarnation acts"
    );
    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Failed);
    assert_eq!(trace.entries(), vec!["load", "undo:seat"]);
    let record = fiber.record();
    assert_eq!(
        path(&record.transitions),
        vec![
            FiberState::Loading,
            FiberState::Active,
            FiberState::Unloading,
            FiberState::Failed
        ]
    );
    assert_eq!(record.transitions[2].cause, TransitionCause::BodyFaulted);
    assert_eq!(record.transitions[3].cause, TransitionCause::BodyFaulted);
    assert_eq!(record.failures.len(), 1);
    assert_eq!(record.failures[0].code, ErrorCode::PluginFailed);
    assert_eq!(
        record.failures[0].fiber,
        Some(fiber.id()),
        "the death is attributed to the fiber that died"
    );
    assert!(
        fiber.effects().is_empty(),
        "exactly its contribution withdrew"
    );
    assert!(
        fiber.resting(),
        "it rests Failed: nothing is scheduled (R9)"
    );
}

/// R9 with M2-K24 (c): the failed fiber is not retried against an
/// unchanged environment; a moved dependency re-arms it once — and a
/// late notice from the DEAD incarnation is recorded, never acted on.
#[tokio::test]
async fn a_faulted_fiber_is_not_retried_until_its_environment_moves() {
    let trace = Trace::new();
    let held: Held = Arc::default();
    let (fiber, source) = ready(faultable(&trace, &held, None));
    fiber.quiesce().await;
    let dead = sink(&held);
    dead.fault(failure("guest trapped"));
    fiber.quiesce().await;
    assert_eq!(fiber.state(), FiberState::Failed);

    fiber.quiesce().await;
    assert_eq!(trace.count("load"), 1, "not retried unchanged");

    source.ready(epoch(2));
    fiber.quiesce().await;
    assert_eq!(fiber.state(), FiberState::Active);
    assert_eq!(trace.count("load"), 2, "re-armed once by the moved epoch");

    assert!(
        !dead.fault(failure("a late notice from the dead incarnation")),
        "a stale fault never acts"
    );
    fiber.quiesce().await;
    assert_eq!(
        fiber.state(),
        FiberState::Active,
        "the successor is untouched"
    );
    let record = fiber.record();
    assert_eq!(
        record.failures.len(),
        2,
        "recorded all the same (no lost fault)"
    );
    assert_eq!(
        record.transitions.len(),
        6,
        "no transition for the stale notice"
    );
}

/// A fault reported while a disposal's withdrawal is in flight: exactly
/// one terminal state, `Disposed`, no second withdrawal, the fault on the
/// record — and never `Failed` after `Disposed`.
#[tokio::test]
async fn a_fault_during_an_in_flight_disposal_lands_exactly_once() {
    let trace = Trace::new();
    let held: Held = Arc::default();
    let gate = Gate::new();
    let (fiber, _source) = ready(faultable(&trace, &held, Some(&gate)));
    fiber.quiesce().await;
    let fiber = Arc::new(fiber);

    let disposing = {
        let fiber = Arc::clone(&fiber);
        tokio::spawn(async move { fiber.dispose().await })
    };
    until(&fiber, FiberState::Unloading).await;
    gate.entered(1).await;
    sink(&held).fault(failure("guest trapped mid-undo"));
    gate.release();
    disposing.await.unwrap_or_else(|error| panic!("{error}"));

    let record = fiber.record();
    assert_eq!(
        path(&record.transitions),
        vec![
            FiberState::Loading,
            FiberState::Active,
            FiberState::Unloading,
            FiberState::Disposed
        ],
        "one terminal state, and it is the disposal's"
    );
    assert_eq!(trace.count("undo:seat"), 1, "one withdrawal, not two");
    assert_eq!(record.failures.len(), 1, "the fault is on the record");
}

/// A notice that arrives after disposal is stale terminal input: it is
/// recorded, but cannot lower rest after the supervisor has exited (I3).
#[tokio::test]
async fn a_fault_after_disposal_cannot_wake_the_terminal_fiber() {
    let trace = Trace::new();
    let held: Held = Arc::default();
    let (fiber, _source) = ready(faultable(&trace, &held, None));
    fiber.quiesce().await;
    let dead = sink(&held);

    fiber.dispose().await;
    assert_eq!(fiber.state(), FiberState::Disposed);
    assert!(fiber.resting());
    assert!(!dead.fault(failure("late death after disposal")));
    assert!(fiber.resting(), "terminal input cannot strand quiescence");
    assert_eq!(
        fiber.record().failures.len(),
        1,
        "the fact is still recorded"
    );
}

/// A fault reported while the fiber is `Unloading` for a config restart:
/// the landing reconciles it — the clean unload leads straight into the
/// restart's load, one withdrawal of the dead incarnation, no `Failed`.
#[tokio::test]
async fn a_fault_during_a_restart_unload_is_reconciled_by_the_landing() {
    let trace = Trace::new();
    let held: Held = Arc::default();
    let gate = Gate::new();
    let (fiber, _source) = ready(faultable(&trace, &held, Some(&gate)));
    fiber.quiesce().await;

    fiber.restart(TransitionCause::ConfigChanged);
    until(&fiber, FiberState::Unloading).await;
    gate.entered(1).await;
    sink(&held).fault(failure("guest exceeded its call deadline"));
    gate.release();
    fiber.quiesce().await;

    assert_eq!(fiber.state(), FiberState::Active);
    let record = fiber.record();
    assert_eq!(
        path(&record.transitions),
        vec![
            FiberState::Loading,
            FiberState::Active,
            FiberState::Unloading,
            FiberState::Pending,
            FiberState::Loading,
            FiberState::Active
        ],
        "the restart lands; the fault never becomes a second unload"
    );
    assert_eq!(trace.count("undo:seat"), 1);
    assert_eq!(trace.count("load"), 2);
    assert_eq!(record.failures.len(), 1, "recorded exactly once");
}
