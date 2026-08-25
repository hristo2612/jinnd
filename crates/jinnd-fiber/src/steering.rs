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

use jinnd_api::{Epoch, TransitionCause};

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
}

impl SteeringCell {
    pub(crate) fn new(epoch: Option<Epoch>) -> Self {
        Self {
            inner: Mutex::new(Inner {
                desired: Desired {
                    aim: Aim { epoch, revision: 0 },
                    cause: TransitionCause::InitialLoad,
                    disposing: false,
                },
                in_flight: None,
            }),
        }
    }

    /// The latest desired state.
    pub(crate) fn desired(&self) -> Desired {
        self.with(|inner| inner.desired.clone())
    }

    /// Mirrors the readiness signal. Returns whether it actually changed.
    pub(crate) fn set_epoch(&self, epoch: Option<Epoch>) -> bool {
        self.with(|inner| {
            if inner.desired.aim.epoch == epoch {
                return false;
            }
            inner.desired.aim.epoch = epoch;
            inner.desired.cause = TransitionCause::DependencyChanged;
            true
        })
    }

    /// Forces a reload even when the dependency set is unchanged.
    pub(crate) fn restart(&self, cause: TransitionCause) {
        self.with(|inner| {
            inner.desired.aim.revision = inner.desired.aim.revision.wrapping_add(1);
            inner.desired.cause = cause;
        });
    }

    /// Requests disposal. Once requested it is never withdrawn.
    pub(crate) fn dispose(&self) {
        self.with(|inner| inner.desired.disposing = true);
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

    /// True when the in-flight activation is already known to be obsolete.
    ///
    /// Nothing is aborted on the strength of this: it is what the supervisor tells
    /// the body through its cancellation token, so a body that cares can stop early.
    pub(crate) fn stale(&self) -> bool {
        self.with(|inner| match &inner.in_flight {
            None => false,
            Some(aim) => inner.desired.disposing || *aim != inner.desired.aim,
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

#[cfg(all(test, feature = "loom"))]
mod models {
    use super::SteeringCell;
    use crate::plan::Aim;
    use jinnd_api::TransitionCause;
    use loom::sync::Arc;
    use loom::thread;

    fn aim(revision: u64) -> Aim {
        Aim {
            epoch: None,
            revision,
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
            let restarting =
                thread::spawn(move || writer.restart(TransitionCause::ExplicitRestart));

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
}
