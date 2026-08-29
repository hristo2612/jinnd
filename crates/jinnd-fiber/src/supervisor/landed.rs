//! What a landed transition COMMITS (M2-K9 round 3).
//!
//! Split from the loop that runs the transitions (R10): running one and
//! recording what it earned are different responsibilities, and the second
//! is the one a reader checks against [`crate::owed`] — every answer that
//! function gives is derived from exactly the state written here.
//!
//! The supervisor task is the only writer, and each write goes through the
//! steering cell's own critical section (R1: held for a field update,
//! never across an `await` or a call into plugin code). A reader asking
//! what a fiber owes therefore sees the landed state and the target
//! together, never one of them stale.

use jinnd_api::{FiberState, TransitionCause};
use jinnd_effects::ReplayReport;

use super::{Cell, unclean};
use crate::plan::Aim;

impl Cell {
    /// The activation for `aim` is live: it serves that aim, and whatever
    /// this fiber failed under before no longer stands in its way.
    pub(super) fn activated(&self, aim: Aim) {
        self.shared.steering.commit(|committed| {
            committed.state = FiberState::Active;
            committed.active_for = Some(aim);
            committed.failed_under = None;
        });
    }

    /// The activation failed and exactly what it applied was withdrawn
    /// (I1). The aim it failed under is recorded so it is not retried
    /// against an environment that has not moved (R9).
    pub(super) fn activation_failed(&self, aim: Aim) {
        self.shared.steering.commit(|committed| {
            committed.state = FiberState::Failed;
            committed.active_for = None;
            committed.failed_under = Some(aim);
        });
    }

    /// A clean unload: no live activation, and the fiber is ready for
    /// whatever the next round plans.
    /// Written through [`Committed::unloaded`], which is also what
    /// [`crate::owed`]'s allowlist projects a clean landing to (M2-K9
    /// round 4): one definition, so the oracle's projection and the
    /// kernel's landing cannot drift.
    pub(super) fn unloaded(&self) {
        self.shared
            .steering
            .commit(|committed| *committed = committed.unloaded());
    }

    /// An unclean unload: the residue is in the record and the aim it
    /// happened under is not reattempted (R9, R11).
    pub(super) fn unload_failed(&self, aim: &Aim) {
        self.shared.steering.commit(|committed| {
            committed.state = FiberState::Failed;
            committed.active_for = None;
            committed.failed_under = Some(aim.clone());
        });
    }

    /// Commits the state a disposal's — or a suspension's — replay earned:
    /// `Disposed` for a clean replay, `Failed` for an unclean one (R11) — a
    /// fiber that could not withdraw never claims it is gone, and the
    /// failed replay is not reattempted against an unchanged scope (R9).
    /// Either way the replay has completed and reported, under `cause`.
    pub(super) fn disposal_landed(&mut self, report: &ReplayReport, cause: TransitionCause) {
        if report.is_clean() {
            self.shared
                .steering
                .commit(|committed| committed.state = FiberState::Disposed);
            self.publish(FiberState::Disposed, cause);
        } else {
            self.shared.fail(unclean(self.shared.id, report));
            self.shared.steering.commit(|committed| {
                committed.state = FiberState::Failed;
                committed.disposal_failed = true;
            });
            self.publish(FiberState::Failed, cause);
        }
    }
}
