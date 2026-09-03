//! The planner's own cases: the ten-rule calculus stated as data.
//!
//! Split out of `plan.rs` at the module seam this crate already uses for
//! `owed` (R10, M2-K9 round 4): the calculus and the cases that pin it are
//! different responsibilities, and the file had grown past the hard cap.

use std::any::TypeId;

use super::{Aim, Committed, Desired, Step, plan};
use jinnd_api::{
    DependencySnapshot, Epoch, FiberId, FiberState, Generation, Realm, ServiceType, TransitionCause,
};

fn epoch(generation: u64) -> Epoch {
    // The registry supplies real dependency snapshots; the planner only ever
    // compares epochs, so one distinguishable snapshot is enough here.
    Epoch {
        dependencies: vec![DependencySnapshot {
            service: ServiceType {
                type_id: TypeId::of::<()>(),
                name: "jinn.test/dependency",
            },
            provider: FiberId(1),
            generation: Generation(generation),
            realm: Realm::Root,
        }],
    }
}

fn aim(generation: u64, revision: u64) -> Aim {
    Aim {
        epoch: Some(epoch(generation)),
        revision,
    }
}

fn desired(aim: Aim) -> Desired {
    Desired {
        aim,
        cause: TransitionCause::InitialLoad,
        disposing: false,
        suspending: false,
        faulted: false,
    }
}

fn committed(state: FiberState) -> Committed {
    Committed {
        state,
        ..Committed::new()
    }
}

/// M2-K4: a suspension unloads under its own cause and finishes an idle
/// fiber (a disposal outranking it is the loom model's claim).
#[test]
fn a_suspension_unloads_under_its_own_cause_and_a_disposal_outranks_it() {
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
    assert_eq!(
        plan(&live, &suspending),
        Some(Step::Unload {
            cause: TransitionCause::Suspend
        })
    );
    assert_eq!(
        plan(&committed(FiberState::Pending), &suspending),
        Some(Step::Finish)
    );
}

#[test]
fn a_satisfied_pending_fiber_is_owed_a_load() {
    let step = plan(&committed(FiberState::Pending), &desired(aim(0, 0)));
    assert!(matches!(step, Some(Step::Load { .. })));
}

#[test]
fn an_unsatisfied_pending_fiber_is_owed_nothing() {
    let unsatisfied = Desired {
        aim: Aim {
            epoch: None,
            revision: 0,
        },
        ..desired(aim(0, 0))
    };
    assert_eq!(plan(&committed(FiberState::Pending), &unsatisfied), None);
}

#[test]
fn an_active_fiber_whose_aim_still_holds_is_quiescent() {
    let state = Committed {
        state: FiberState::Active,
        active_for: Some(aim(0, 0)),
        ..Committed::new()
    };
    assert_eq!(plan(&state, &desired(aim(0, 0))), None);
}

#[test]
fn an_active_fiber_whose_aim_moved_is_owed_an_unload() {
    let state = Committed {
        state: FiberState::Active,
        active_for: Some(aim(0, 0)),
        ..Committed::new()
    };
    assert!(matches!(
        plan(&state, &desired(aim(0, 1))),
        Some(Step::Unload { .. })
    ));
}

#[test]
fn a_fiber_mid_transition_is_never_owed_a_second_one() {
    for state in [FiberState::Loading, FiberState::Unloading] {
        assert_eq!(plan(&committed(state), &desired(aim(0, 0))), None);
    }
}

#[test]
fn a_failed_fiber_is_owed_nothing_under_the_aim_it_failed_on() {
    let state = Committed {
        state: FiberState::Failed,
        failed_under: Some(aim(0, 0)),
        ..Committed::new()
    };
    assert_eq!(plan(&state, &desired(aim(0, 0))), None);
    assert!(matches!(
        plan(&state, &desired(aim(0, 1))),
        Some(Step::Load { .. })
    ));
}

#[test]
fn disposal_unloads_a_live_activation_and_finishes_an_inactive_one() {
    let disposing = Desired {
        disposing: true,
        ..desired(aim(0, 0))
    };
    let active = Committed {
        state: FiberState::Active,
        active_for: Some(aim(0, 0)),
        ..Committed::new()
    };
    assert_eq!(
        plan(&active, &disposing),
        Some(Step::Unload {
            cause: TransitionCause::ExplicitDispose
        })
    );
    for state in [FiberState::Pending, FiberState::Failed] {
        assert_eq!(plan(&committed(state), &disposing), Some(Step::Finish));
    }
    assert_eq!(plan(&committed(FiberState::Disposed), &disposing), None);
}

#[test]
fn a_failed_disposal_is_not_reattempted_against_an_unchanged_scope() {
    let disposing = Desired {
        disposing: true,
        ..desired(aim(0, 0))
    };
    let state = Committed {
        state: FiberState::Failed,
        disposal_failed: true,
        ..Committed::new()
    };
    assert_eq!(plan(&state, &disposing), None);
}
