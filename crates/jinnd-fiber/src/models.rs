//! Loom models for the inertia lock: launch vs target change vs cancellation.
//!
//! The tokio supervisor cannot compile under loom — `select!`, `Notify` and
//! `watch` are not loom primitives — so every concurrency-sensitive decision it
//! takes goes through [`SteeringCell`], and is modelled here over loom's own
//! primitives. [`SteeringCell::absorb`] is the exact function the supervisor's
//! absorb path calls; [`absorb_into`] raises a modelled flag precisely where the
//! supervisor raises the cooperative `CancellationToken` (R1 — told, never
//! aborted).

use std::any::TypeId;

use loom::sync::Arc;
use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use loom::thread;

use jinnd_api::{
    DependencySnapshot, Epoch, FiberId, Generation, Realm, ServiceType, TransitionCause,
};

use crate::plan::Aim;
use crate::steering::SteeringCell;

fn aim(revision: u64) -> Aim {
    Aim {
        epoch: None,
        revision,
    }
}

/// A dependency identity distinguishable by generation, as the registry will
/// publish and the readiness signal will mirror.
fn dep(generation: u64) -> Option<Epoch> {
    Some(Epoch {
        dependencies: vec![DependencySnapshot {
            service: ServiceType {
                type_id: TypeId::of::<()>(),
                name: "jinn.model/dependency",
            },
            provider: FiberId(u64::MAX),
            generation: Generation(generation),
            realm: Realm::Root,
        }],
    })
}

/// The supervisor's absorb, with the token modelled as a loom flag: fold the
/// observed input in, and raise the flag exactly when the in-flight aim went
/// stale with it.
fn absorb_into(cell: &SteeringCell, epoch: Option<Epoch>, cancelled: &AtomicBool) {
    if cell.absorb(epoch) {
        cancelled.store(true, Ordering::SeqCst);
    }
}

/// The supervisor's half of one round: launch a transition, learn whether it
/// went stale, land it, then read the target it must reconcile against.
///
/// Whether a change that lands *after* this read is serviced is the loop's
/// business, not the cell's — the loop re-reads every input at the top of the
/// next round, and the writer's wake-up is what guarantees there is one. What
/// the cell owes is modelled below: no update is lost, and staleness is never
/// invented.
fn round(cell: &SteeringCell) -> (bool, Aim) {
    cell.launch(aim(0));
    let stale = cell.stale();
    cell.land();
    (stale, cell.desired().aim)
}

/// A restart racing a launch is applied exactly once, and if the in-flight
/// activation was told it went stale, the target really had moved by then.
#[test]
fn a_restart_racing_a_launch_is_applied_exactly_once() {
    loom::model(|| {
        let cell = Arc::new(SteeringCell::new(None));
        let writer = Arc::clone(&cell);
        let restarting = thread::spawn(move || writer.restart(TransitionCause::ExplicitRestart));

        let (stale, landed) = round(&cell);

        restarting.join().unwrap_or_else(|_| unreachable!());
        assert!(
            !stale || landed == aim(1),
            "staleness was reported for a target that had not moved"
        );
        assert_eq!(
            cell.desired().aim,
            aim(1),
            "the restart was lost, or applied twice"
        );
    });
}

/// Disposal racing a launch is subject to the same rule, and it is sticky: no
/// interleaving ever leaves it withdrawn.
#[test]
fn a_disposal_racing_a_launch_is_never_lost_or_withdrawn() {
    loom::model(|| {
        let cell = Arc::new(SteeringCell::new(None));
        let writer = Arc::clone(&cell);
        let disposing = thread::spawn(move || writer.dispose());

        let (stale, _) = round(&cell);

        disposing.join().unwrap_or_else(|_| unreachable!());
        assert!(
            !stale || cell.desired().disposing,
            "staleness was reported before disposal was requested"
        );
        assert!(cell.desired().disposing, "the disposal was lost");
    });
}

/// Two writers racing one landing: both changes are applied, and what the
/// supervisor reconciles against afterwards is the coalesced latest target, not
/// either writer's private view.
#[test]
fn concurrent_targets_coalesce_into_one_latest_target() {
    loom::model(|| {
        let cell = Arc::new(SteeringCell::new(None));
        let first = Arc::clone(&cell);
        let second = Arc::clone(&cell);
        let restarting = thread::spawn(move || first.restart(TransitionCause::ExplicitRestart));
        let disposing = thread::spawn(move || second.dispose());

        round(&cell);

        restarting.join().unwrap_or_else(|_| unreachable!());
        disposing.join().unwrap_or_else(|_| unreachable!());
        let desired = cell.desired();
        assert_eq!(desired.aim, aim(1));
        assert!(desired.disposing);
    });
}

/// A launch while one transition is already in flight is a broken supervisor,
/// not a plugin failure: the cell refuses it however the threads interleave.
#[test]
fn the_cell_refuses_a_second_transition_in_flight() {
    loom::model(|| {
        let cell = SteeringCell::new(None);
        cell.launch(aim(0));
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cell.launch(aim(1))))
                .is_err()
        );
    });
}

/// Launch vs restart vs cancellation, through the supervisor's own absorb: a
/// restart already completed when the absorb runs is always told to the
/// in-flight activation (no missed signal), a raised signal always means the
/// target had really moved by the landing (no invented signal), and the restart
/// itself is never lost.
#[test]
fn a_restart_completed_before_the_absorb_always_raises_cancellation() {
    loom::model(|| {
        let cell = Arc::new(SteeringCell::new(None));
        let cancelled = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let writer_cell = Arc::clone(&cell);
        let writer_done = Arc::clone(&done);
        let writer = thread::spawn(move || {
            writer_cell.restart(TransitionCause::ExplicitRestart);
            writer_done.store(true, Ordering::SeqCst);
        });

        cell.launch(aim(0));
        let done_before = done.load(Ordering::SeqCst);
        absorb_into(&cell, None, &cancelled);
        cell.land();
        let raised = cancelled.load(Ordering::SeqCst);
        let seen = cell.desired().aim;

        writer.join().unwrap_or_else(|_| unreachable!());
        assert!(
            !raised || seen == aim(1),
            "cancellation was raised for a target that had not moved"
        );
        assert!(
            !done_before || raised,
            "a restart that completed before the absorb was never told"
        );
        assert_eq!(cell.desired().aim, aim(1), "the restart was lost");
    });
}

/// Launch vs disposal vs cancellation: the same absorb path, disposing lane —
/// no missed signal, no invented signal, and disposal stays sticky throughout.
#[test]
fn a_disposal_completed_before_the_absorb_always_raises_cancellation() {
    loom::model(|| {
        let cell = Arc::new(SteeringCell::new(None));
        let cancelled = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicBool::new(false));
        let writer_cell = Arc::clone(&cell);
        let writer_done = Arc::clone(&done);
        let writer = thread::spawn(move || {
            writer_cell.dispose();
            writer_done.store(true, Ordering::SeqCst);
        });

        cell.launch(aim(0));
        let done_before = done.load(Ordering::SeqCst);
        absorb_into(&cell, None, &cancelled);
        cell.land();
        let raised = cancelled.load(Ordering::SeqCst);
        let seen_disposing = cell.desired().disposing;

        writer.join().unwrap_or_else(|_| unreachable!());
        assert!(
            !raised || seen_disposing,
            "cancellation was raised before disposal was requested"
        );
        assert!(
            !done_before || raised,
            "a disposal that completed before the absorb was never told"
        );
        assert!(cell.desired().disposing, "the disposal was lost");
    });
}

/// Launch vs a dependency-epoch change vs cancellation: the lane the readiness
/// signal arrives through. The mirror is faithful — after the absorb the cell
/// holds exactly the epoch it observed — and staleness is raised exactly when
/// the observed epoch differs from the launched one, in either direction.
#[test]
fn an_epoch_change_absorbed_mid_flight_raises_cancellation_exactly_then() {
    loom::model(|| {
        let cell = Arc::new(SteeringCell::new(dep(1)));
        let cancelled = Arc::new(AtomicBool::new(false));
        let signal = Arc::new(AtomicU64::new(1));
        let writer_signal = Arc::clone(&signal);
        let writer = thread::spawn(move || writer_signal.store(2, Ordering::SeqCst));

        let target = cell.desired().aim;
        cell.launch(target);
        let seen = signal.load(Ordering::SeqCst);
        absorb_into(&cell, dep(seen), &cancelled);
        cell.land();

        writer.join().unwrap_or_else(|_| unreachable!());
        assert_eq!(
            cell.desired().aim.epoch,
            dep(seen),
            "the mirror dropped what the absorb observed"
        );
        assert_eq!(
            cancelled.load(Ordering::SeqCst),
            seen == 2,
            "staleness must track exactly whether the observed epoch moved"
        );
    });
}
