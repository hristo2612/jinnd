//! The ten-rule calculus: what has landed, what is wanted, and the one transition
//! that closes the gap.
//!
//! A total function, with no tokio and no plugin body anywhere near it. Keeping the
//! decision separate from the machinery that carries it out is what makes the fiber
//! engine's temporal semantics testable as data rather than as timing (R2).

use jinnd_api::{Epoch, FiberState, TransitionCause};

/// The identity of the environment one activation is made for.
///
/// `epoch` is `None` while any injected dependency is unavailable. `revision`
/// carries the changes that force a reload without changing the dependency set —
/// an operator restart, a config edit — so an unchanged epoch still reloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Aim {
    pub epoch: Option<Epoch>,
    pub revision: u64,
}

/// The latest desired state, with the reason it last changed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Desired {
    pub aim: Aim,
    pub cause: TransitionCause,
    pub disposing: bool,
}

/// What has landed, as opposed to what is wanted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Committed {
    pub state: FiberState,
    /// The aim the live activation was made for, while one is live.
    pub active_for: Option<Aim>,
    /// The aim the last failure happened under, so it is not attempted again (R9).
    pub failed_under: Option<Aim>,
    /// True once a disposal's own withdrawal failed: the fiber rests `Failed` and
    /// the replay is not reattempted against an unchanged scope (R9).
    pub disposal_failed: bool,
}

impl Committed {
    pub(crate) fn new() -> Self {
        Self {
            state: FiberState::Pending,
            active_for: None,
            failed_under: None,
            disposal_failed: false,
        }
    }
}

/// The one transition a fiber may launch next.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Step {
    /// Run the plugin body for `aim`.
    Load { aim: Aim, cause: TransitionCause },
    /// Replay the activation's effects, last registered first.
    Unload { cause: TransitionCause },
    /// Finish disposal from a state that holds no live activation.
    Finish,
}

/// The single transition owed by the gap between `committed` and `desired`.
///
/// Returns `None` at quiescence, and `None` while a transition is in flight — a
/// fiber mid-`Loading` or mid-`Unloading` has already launched the only transition
/// it is allowed to have.
pub(crate) fn plan(committed: &Committed, desired: &Desired) -> Option<Step> {
    match committed.state {
        // Disposal is terminal: nothing reanimates a disposed fiber.
        FiberState::Disposed => None,
        // A transition is in flight; its landing does the reconciling.
        FiberState::Loading | FiberState::Unloading => None,
        FiberState::Active => {
            if desired.disposing {
                Some(Step::Unload {
                    cause: TransitionCause::ExplicitDispose,
                })
            } else if committed.active_for.as_ref() == Some(&desired.aim) {
                None
            } else {
                Some(Step::Unload {
                    cause: desired.cause.clone(),
                })
            }
        }
        FiberState::Pending | FiberState::Failed => {
            if desired.disposing {
                // A disposal whose own withdrawal failed rests `Failed`: the replay
                // is not reattempted against an unchanged scope (R9).
                if committed.disposal_failed {
                    return None;
                }
                return Some(Step::Finish);
            }
            desired.aim.epoch.as_ref()?;
            // R9: a failed fiber is not retried against an unchanged environment.
            if committed.state == FiberState::Failed
                && committed.failed_under.as_ref() == Some(&desired.aim)
            {
                return None;
            }
            Some(Step::Load {
                aim: desired.aim.clone(),
                cause: desired.cause.clone(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::any::TypeId;

    use super::{Aim, Committed, Desired, Step, plan};
    use jinnd_api::{
        DependencySnapshot, Epoch, FiberId, FiberState, Generation, Realm, ServiceType,
        TransitionCause,
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
        }
    }

    fn committed(state: FiberState) -> Committed {
        Committed {
            state,
            ..Committed::new()
        }
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
}
