//! Loom models for the M2-K4 suspension lane: suspend vs launch, and
//! suspend vs dispose precedence. Split from `models.rs` by responsibility
//! (R10 file hygiene).

use loom::sync::Arc;
use loom::thread;

use jinnd_api::TransitionCause;

use crate::steering::SteeringCell;

use super::round;

/// Suspension racing a launch (M2-K4) obeys the disposal rule: sticky, never
/// lost, and staleness reported only once the target really moved.
#[test]
fn a_suspension_racing_a_launch_is_never_lost_or_withdrawn() {
    loom::model(|| {
        let cell = Arc::new(SteeringCell::new(None));
        let writer = Arc::clone(&cell);
        let suspending = thread::spawn(move || writer.suspend());

        let (stale, _) = round(&cell);

        suspending.join().unwrap_or_else(|_| unreachable!());
        assert!(
            !stale || cell.desired().suspending,
            "staleness was reported before suspension was requested"
        );
        assert!(cell.desired().suspending, "the suspension was lost");
    });
}

/// Suspend vs dispose (M2-K4): whichever order the two requests interleave
/// in, the coalesced target holds both, and the planner's choice for a live
/// activation is the disposal — a suspension never downgrades a disposal to
/// a retention, and a disposal never loses the suspension it outranks.
#[test]
fn a_disposal_outranks_a_racing_suspension_whichever_lands_first() {
    loom::model(|| {
        let cell = Arc::new(SteeringCell::new(None));
        let first = Arc::clone(&cell);
        let second = Arc::clone(&cell);
        let suspending = thread::spawn(move || first.suspend());
        let disposing = thread::spawn(move || second.dispose());

        round(&cell);

        suspending.join().unwrap_or_else(|_| unreachable!());
        disposing.join().unwrap_or_else(|_| unreachable!());
        let desired = cell.desired();
        assert!(desired.disposing && desired.suspending);
        let live = crate::plan::Committed {
            state: jinnd_api::FiberState::Active,
            active_for: Some(desired.aim.clone()),
            ..crate::plan::Committed::new()
        };
        assert_eq!(
            crate::plan::plan(&live, &desired),
            Some(crate::plan::Step::Unload {
                cause: TransitionCause::ExplicitDispose
            }),
            "a disposal must win the unload's mode"
        );
    });
}
