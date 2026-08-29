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
    /// Suspension requested (M2-K4): the cell stops, the entry persists.
    /// Sticky like disposal, and outranked by it.
    pub suspending: bool,
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
    /// The state a CLEAN unload commits: no live activation, ready for
    /// whatever the next round plans.
    ///
    /// ONE definition, written by the supervisor's landing and read by
    /// [`crate::owed`]'s allowlist (M2-K9 round 4). The projection an
    /// oracle reasons over and the state the kernel actually lands are the
    /// same value by construction, so they cannot drift into telling a
    /// caller two different stories.
    pub(crate) fn unloaded(&self) -> Self {
        Self {
            state: FiberState::Pending,
            active_for: None,
            ..self.clone()
        }
    }

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
            } else if desired.suspending {
                Some(Step::Unload {
                    cause: TransitionCause::Suspend,
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
            if desired.disposing || desired.suspending {
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
mod tests;
