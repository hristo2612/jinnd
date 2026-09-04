//! What each corner MEANS, named one at a time. The proof lives next door
//! in [`super::properties`]; these cases exist so a reader (and a future
//! round's reviewer) can see the intent behind a corner the quantified
//! laws only visit anonymously.

use jinnd_api::{FiberState, Owed};

use super::{aim, desired, unsatisfied};
use crate::owed::owed;
use crate::plan::{Committed, Desired};

/// The in-flight arm (M2-K26 (d)): a transition already RUNNING is a
/// replacement the planner cannot re-plan — it answers `None` mid-flight
/// — but its landing is not a mystery: a clean landing rests on
/// [`Committed::unloaded`] (an unload) or serves outright (a load), and
/// from there the planner loads for a satisfiable target. Before this
/// arm both states fell past the allowlist and answered `Stalled` while a
/// load was in flight — so `jinn:introspect` told an operator "nothing is
/// coming" for the whole of a restart's activation, and a walk that
/// selected the replacement's listener (harness FINDINGS #47's second
/// half) was told the same. The allowlist stays closed: the arm carries
/// its proof in the planner's own answer over the landing's projection.
#[test]
fn an_in_flight_transition_owes_the_reload_its_landing_reaches() {
    for state in [FiberState::Loading, FiberState::Unloading] {
        for target in [aim(0, 0), aim(0, 1)] {
            let committed = Committed {
                state,
                active_for: Some(aim(0, 0)),
                ..Committed::new()
            };
            assert_eq!(
                owed(&committed, &desired(target)),
                Some(Owed::Reload),
                "{state:?} is a replacement in flight and must promise it"
            );
        }
    }
}

/// The arm's other edge: an in-flight transition whose landing the
/// planner will NOT load from still stalls — a fault reported for the
/// incarnation (M2-K25: the landing is `Failed`, never retried under the
/// same aim) and a withdrawn dependency (next case) both fall past it.
#[test]
fn an_in_flight_transition_with_a_faulted_incarnation_owes_a_stall() {
    for state in [FiberState::Loading, FiberState::Unloading] {
        let committed = Committed {
            state,
            active_for: Some(aim(0, 0)),
            ..Committed::new()
        };
        let faulted = Desired {
            faulted: true,
            ..desired(aim(0, 0))
        };
        assert_eq!(
            owed(&committed, &faulted),
            Some(Owed::Stalled),
            "{state:?} with a dead incarnation promised a replacement"
        );
    }
}

/// The verifier's round-2 probe, as a named case: a dependency is
/// withdrawn, so `plan` cannot load — a stall, never a restart nobody
/// scheduled.
#[test]
fn a_withdrawn_dependency_owes_a_stall_and_not_a_restart() {
    for state in [
        FiberState::Unloading,
        FiberState::Active,
        FiberState::Pending,
        FiberState::Loading,
    ] {
        let committed = Committed {
            state,
            ..Committed::new()
        };
        assert_eq!(
            owed(&committed, &desired(unsatisfied())),
            Some(Owed::Stalled),
            "{state:?} promised a restart with its dependency withdrawn"
        );
    }
}

/// Terminal and R9-blocked fibers owe a stall too: the supervisor has
/// returned, or the environment has not moved, so nothing they owe will
/// ever be scheduled.
#[test]
fn a_target_no_round_will_ever_serve_owes_a_stall() {
    let target = desired(aim(0, 1));
    let disposed = Committed {
        state: FiberState::Disposed,
        ..Committed::new()
    };
    assert_eq!(owed(&disposed, &target), Some(Owed::Stalled));
    // The disposal this fiber could not replay is still ASKED of it — the
    // request is sticky and never withdrawn, which is the only way this
    // state is reached. Round 3 built the case without the request and so
    // asserted a stall against a fiber the planner could genuinely still
    // load; the inverted allowlist reads the planner rather than
    // short-circuiting on the flag, and caught it.
    let unclean = Committed {
        state: FiberState::Failed,
        disposal_failed: true,
        ..Committed::new()
    };
    let asked = Desired {
        disposing: true,
        ..target.clone()
    };
    assert_eq!(owed(&unclean, &asked), Some(Owed::Stalled));
    let refused_retry = Committed {
        state: FiberState::Failed,
        failed_under: Some(aim(0, 1)),
        ..Committed::new()
    };
    assert_eq!(owed(&refused_retry, &target), Some(Owed::Stalled));
}

/// A disposed fiber has SERVED its disposal: it owes nothing, and a
/// caller reading it is not sent chasing an environment change.
#[test]
fn a_disposed_fiber_has_served_its_disposal() {
    let disposed = Committed {
        state: FiberState::Disposed,
        ..Committed::new()
    };
    for request in [(true, false), (false, true)] {
        let asked = Desired {
            disposing: request.0,
            suspending: request.1,
            faulted: false,
            ..desired(aim(0, 0))
        };
        assert_eq!(owed(&disposed, &asked), None);
    }
}

/// The sticky requests keep their own answers, ranked as `plan` ranks
/// them: a disposal is never sold as a coming restart (round 2), and a
/// disposal asked alongside a suspension outranks it.
#[test]
fn a_disposal_outranks_a_suspension_and_neither_is_a_restart() {
    let live = Committed {
        state: FiberState::Active,
        active_for: Some(aim(0, 0)),
        ..Committed::new()
    };
    let suspending = Desired {
        suspending: true,
        faulted: false,
        ..desired(aim(0, 0))
    };
    assert_eq!(owed(&live, &suspending), Some(Owed::Suspension));
    let both = Desired {
        disposing: true,
        ..suspending
    };
    assert_eq!(owed(&live, &both), Some(Owed::Disposal));
}

/// A live activation made for the desired aim SERVES it: nothing owed, so
/// nothing refused. Without this the window between a restart landing and
/// the supervisor raising rest would refuse callers in the name of a
/// restart that had already landed.
#[test]
fn a_live_activation_on_the_desired_aim_owes_nothing() {
    let live = Committed {
        state: FiberState::Active,
        active_for: Some(aim(0, 0)),
        ..Committed::new()
    };
    assert_eq!(owed(&live, &desired(aim(0, 0))), None);
    assert_eq!(owed(&live, &desired(aim(0, 1))), Some(Owed::Reload));
}
