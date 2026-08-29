//! The round-3 law, proved as an IMPLICATION over the whole input space
//! rather than case by case (M2-K9 round 3): wherever the derivation
//! answers [`Owed::Reload`], a replacement must genuinely be schedulable.
//!
//! The generator below is the whole cartesian product of the space, so the
//! verifier's dependency-loss probe is a sample it would have produced
//! unaided — which is the point. The named cases that follow are there to
//! say what each corner MEANS, not to carry the proof.

use std::any::TypeId;

use jinnd_api::{
    DependencySnapshot, Epoch, FiberId, FiberState, Generation, Owed, Realm, ServiceType,
    TransitionCause,
};

use super::owed;
use crate::plan::{Aim, Committed, Desired, Step, plan};

/// Every state the planner knows: enumerated here so a new one is a
/// failing case rather than an untested corner.
const STATES: [FiberState; 6] = [
    FiberState::Pending,
    FiberState::Loading,
    FiberState::Active,
    FiberState::Failed,
    FiberState::Unloading,
    FiberState::Disposed,
];

fn epoch(generation: u64) -> Epoch {
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

/// The aim of a fiber whose dependency has been withdrawn: no epoch, so
/// the planner cannot load for it.
fn unsatisfied() -> Aim {
    Aim {
        epoch: None,
        revision: 0,
    }
}

fn desired(aim: Aim) -> Desired {
    Desired {
        aim,
        cause: TransitionCause::InitialLoad,
        disposing: false,
        suspending: false,
    }
}

/// Whether the planner can reach a `Load` from `committed`, following the
/// supervisor's own landings.
///
/// Only the CLEAN landing is modeled, and a load already in flight counts
/// as reached: both make this the OPTIMISTIC reading of what a fiber can
/// still do (an unclean landing rests `Failed`, which can do strictly
/// less). A `Reload` the optimistic reading cannot justify is therefore a
/// promise the kernel definitely cannot keep — exactly the implication
/// under test.
fn reaches_load(start: &Committed, desired: &Desired) -> bool {
    let mut committed = start.clone();
    for _ in 0..4 {
        match plan(&committed, desired) {
            Some(Step::Load { .. }) => return true,
            // A disposal or a suspension is being finished; no load follows.
            Some(Step::Finish) => return false,
            Some(Step::Unload { .. }) => {
                committed.state = FiberState::Pending;
                committed.active_for = None;
            }
            None => match committed.state {
                // A load is in flight: the replacement is not merely
                // schedulable, it is already running.
                FiberState::Loading => return true,
                FiberState::Unloading => {
                    committed.state = FiberState::Pending;
                    committed.active_for = None;
                }
                FiberState::Pending
                | FiberState::Active
                | FiberState::Failed
                | FiberState::Disposed => return false,
            },
        }
    }
    false
}

/// Every (committed, desired) pair the space can present, reachable or
/// not. Testing a superset costs nothing here and cannot produce a false
/// pass: a derivation that holds over the superset holds on the reachable
/// part of it.
fn every_pair() -> Vec<(Committed, Desired)> {
    let aims = [Some(aim(0, 0)), Some(aim(0, 1)), None];
    let mut pairs = Vec::new();
    for state in STATES {
        for active_for in &aims {
            for failed_under in &aims {
                for disposal_failed in [false, true] {
                    for disposing in [false, true] {
                        for suspending in [false, true] {
                            for target in [aim(0, 0), aim(0, 1), unsatisfied()] {
                                pairs.push((
                                    Committed {
                                        state,
                                        active_for: active_for.clone(),
                                        failed_under: failed_under.clone(),
                                        disposal_failed,
                                    },
                                    Desired {
                                        disposing,
                                        suspending,
                                        ..desired(target)
                                    },
                                ));
                            }
                        }
                    }
                }
            }
        }
    }
    pairs
}

/// The law: `Reload` is the one answer that PROMISES a replacement, so it
/// is given only where a replacement is genuinely schedulable. Round 1
/// broke this for disposal and suspension, round 2 for a withdrawn
/// dependency — both through a fall-through that answered `Reload`
/// whenever nothing else matched. The implication closes the class.
#[test]
fn a_reload_is_only_promised_where_a_load_is_actually_reachable() {
    let mut promised = 0u32;
    for (committed, desired) in every_pair() {
        if owed(&committed, &desired) == Some(Owed::Reload) {
            promised += 1;
            assert!(
                reaches_load(&committed, &desired),
                "promised a restart nobody scheduled: {committed:?} / {desired:?}"
            );
        }
    }
    assert!(promised > 0, "the enumeration never exercised a promise");
}

/// The converse half: owing NOTHING means the committed state serves the
/// desired one, so the planner has nothing to run either. Without it a
/// "serves fine" answer could hide a real pending transition and put the
/// stall back that this packet exists to remove.
#[test]
fn owing_nothing_means_the_planner_has_nothing_to_run() {
    for (committed, desired) in every_pair() {
        if owed(&committed, &desired).is_none() {
            assert_eq!(
                plan(&committed, &desired),
                None,
                "owed nothing while a step was planned: {committed:?} / {desired:?}"
            );
        }
    }
}

/// The round-4 inversion, stated as the closure of the ALLOWLIST: a
/// committed state the allowlist does not name owes a STALL, whatever else
/// is true of it. `Loading` and `Unloading` are the states standing in for
/// "not named" here — the supervisor commits neither, so nothing about them
/// can be proved from the planner, and the conservative answer is the only
/// honest one. Rounds 1-3 each answered `Reload` from an unnamed state
/// because the fall-through was optimistic; after the inversion an unnamed
/// state cannot reach `Reload` at all.
#[test]
fn a_state_the_allowlist_does_not_name_owes_a_stall() {
    for state in [FiberState::Loading, FiberState::Unloading] {
        for target in [aim(0, 0), aim(0, 1)] {
            let committed = Committed {
                state,
                active_for: Some(aim(0, 0)),
                ..Committed::new()
            };
            assert_eq!(
                owed(&committed, &desired(target)),
                Some(Owed::Stalled),
                "{state:?} is not on the allowlist and promised a replacement"
            );
        }
    }
}

/// The guard that keeps the inversion from degenerating: answering
/// `Stalled` to everything would satisfy "never promise a replacement
/// falsely" and destroy the packet, so a stall is forbidden wherever the
/// planner would schedule a load RIGHT NOW. The two properties together
/// pin the answer from both sides.
#[test]
fn a_stall_is_never_given_where_the_planner_would_load_right_now() {
    let mut stalled = 0u32;
    for (committed, desired) in every_pair() {
        if owed(&committed, &desired) == Some(Owed::Stalled) {
            stalled += 1;
            assert!(
                !matches!(plan(&committed, &desired), Some(Step::Load { .. })),
                "stalled a caller while a load was the planned step: {committed:?} / {desired:?}"
            );
        }
    }
    assert!(stalled > 0, "the enumeration never exercised a stall");
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
