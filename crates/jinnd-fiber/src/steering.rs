//! The inertia lock: the one shared cell every input writes to.
//!
//! The lock is the single-flight rule (R1): while a transition is in flight the cell
//! records new targets and never launches a second one. A launched transition
//! therefore always lands, and the reconciliation that follows it reads the *latest*
//! target rather than the one it started from — intermediate targets coalesce
//! instead of queueing.
//!
//! The cell's own lock is held for a field read or a field write and never across an
//! `await` or a call into plugin code. That is the whole discipline; the loom models
//! at the bottom of this file are what keep it honest.

use jinnd_api::{Epoch, Owed, TransitionCause};

use crate::plan::{Aim, Desired};
use crate::sync::Mutex;

/// The shared cell every input writes to and the supervisor reads from.
///
/// The lock is held for a field read or a field write and never across an `await`
/// or a call into plugin code (R1). That is the whole discipline; the loom models
/// below are what keep it honest.
#[derive(Debug)]
pub(crate) struct SteeringCell {
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    desired: Desired,
    /// The aim of the in-flight activation, while there is one.
    in_flight: Option<Aim>,
    /// True while the fiber owes no transition: the last one landed and the
    /// committed state equals `desired` (the REST gate, M1-P6c).
    resting: bool,
    /// Bumped by every target write, in the same critical section that lowers
    /// `resting` (the round-3 law): a settle presenting a stale stamp is
    /// refused, so rest and target can never be observed out of sync.
    moved: u64,
}

impl Inner {
    /// The target moved: rest lowers HERE, atomically with the write — never
    /// deferred to supervisor scheduling (M1-P6c round 3).
    fn stir(&mut self) {
        self.resting = false;
        self.moved = self.moved.wrapping_add(1);
    }
}

impl SteeringCell {
    pub(crate) fn new(epoch: Option<Epoch>) -> Self {
        Self {
            inner: Mutex::new(Inner {
                desired: Desired {
                    aim: Aim { epoch, revision: 0 },
                    cause: TransitionCause::InitialLoad,
                    disposing: false,
                    suspending: false,
                },
                in_flight: None,
                // A fresh fiber still owes its first reconciliation pass.
                resting: false,
                moved: 0,
            }),
        }
    }

    /// The latest desired state.
    pub(crate) fn desired(&self) -> Desired {
        self.with(|inner| inner.desired.clone())
    }

    /// The latest desired state, stamped: the stamp is what a later
    /// [`SteeringCell::settle_rest`] must present, so rest is only ever
    /// raised over the exact target this read observed (M1-P6c round 3).
    pub(crate) fn observed(&self) -> (Desired, u64) {
        self.with(|inner| (inner.desired.clone(), inner.moved))
    }

    /// True while the fiber owes no transition (the REST gate). Lowered
    /// atomically with every target write; raised only by a settle whose
    /// stamp is still current — the two can never be observed out of sync.
    pub(crate) fn resting(&self) -> bool {
        self.with(|inner| inner.resting)
    }

    /// What the fiber owes, when it owes anything (M2-K9). Read in the SAME
    /// critical section as the rest bit, so a caller never sees "owes
    /// something" paired with a stale answer about WHAT.
    ///
    /// Disposal outranks suspension exactly as the planner ranks them: a
    /// fiber asked to suspend and then to dispose owes a disposal, and a
    /// caller told so must not wait for a resume that would not help.
    pub(crate) fn owed(&self) -> Option<Owed> {
        self.with(|inner| {
            if inner.resting {
                return None;
            }
            Some(if inner.desired.disposing {
                Owed::Disposal
            } else if inner.desired.suspending {
                Owed::Suspension
            } else {
                Owed::Reload
            })
        })
    }

    /// Raises rest, unless a target write moved the cell since `observed`
    /// was stamped — then the settle is stale and refused (round-3 law).
    /// Returns whether the fiber now rests.
    pub(crate) fn settle_rest(&self, observed: u64) -> bool {
        self.with(|inner| {
            if inner.moved != observed {
                return false;
            }
            inner.resting = true;
            true
        })
    }

    /// Mirrors the readiness signal. Returns whether it actually changed.
    pub(crate) fn set_epoch(&self, epoch: Option<Epoch>) -> bool {
        self.with(|inner| {
            if inner.desired.aim.epoch == epoch {
                return false;
            }
            inner.desired.aim.epoch = epoch;
            inner.desired.cause = TransitionCause::DependencyChanged;
            inner.stir();
            true
        })
    }

    /// Forces a reload even when the dependency set is unchanged.
    pub(crate) fn restart(&self, cause: TransitionCause) {
        self.with(|inner| {
            inner.desired.aim.revision = inner.desired.aim.revision.wrapping_add(1);
            inner.desired.cause = cause;
            inner.stir();
        });
    }

    /// Requests disposal. Once requested it is never withdrawn.
    pub(crate) fn dispose(&self) {
        self.with(|inner| {
            if !inner.desired.disposing {
                inner.desired.disposing = true;
                inner.stir();
            }
        });
    }

    /// Requests suspension (M2-K4). Sticky once requested; a disposal
    /// requested before or after it still wins the planner's choice.
    pub(crate) fn suspend(&self) {
        self.with(|inner| {
            if !inner.desired.suspending {
                inner.desired.suspending = true;
                inner.stir();
            }
        });
    }

    /// Records that a transition for `aim` is in flight.
    ///
    /// # Panics
    ///
    /// If a transition is already in flight. That is the single-flight rule, and a
    /// second launch would mean the supervisor loop is broken rather than a plugin.
    pub(crate) fn launch(&self, aim: Aim) {
        self.with(|inner| {
            assert!(
                inner.in_flight.is_none(),
                "a fiber may have at most one transition in flight"
            );
            inner.in_flight = Some(aim);
        });
    }

    /// Records that the in-flight transition landed.
    pub(crate) fn land(&self) {
        self.with(|inner| inner.in_flight = None);
    }

    /// Folds one observed input into the cell and reports whether the in-flight
    /// activation went stale with it.
    ///
    /// This is the decision half of the supervisor's absorb path: the caller raises
    /// the cooperative cancellation signal exactly when this returns true. It lives
    /// on the cell so the loom models in [`crate::models`] drive the very function
    /// the supervisor does.
    pub(crate) fn absorb(&self, epoch: Option<Epoch>) -> bool {
        self.set_epoch(epoch);
        self.stale()
    }

    /// True when the in-flight activation is already known to be obsolete.
    ///
    /// Nothing is aborted on the strength of this: it is what the supervisor tells
    /// the body through its cancellation token, so a body that cares can stop early.
    pub(crate) fn stale(&self) -> bool {
        self.with(|inner| match &inner.in_flight {
            None => false,
            Some(aim) => {
                inner.desired.disposing || inner.desired.suspending || *aim != inner.desired.aim
            }
        })
    }

    fn with<T>(&self, change: impl FnOnce(&mut Inner) -> T) -> T {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        change(&mut inner)
    }
}
