//! What the supervisor WITHDRAWS and PUBLISHES: the drain-and-replay of
//! one activation's scope, and the transition/effect-tree writes every
//! observer reads. Split from the loop that plans and runs the transitions
//! (R10 file hygiene).

use jinnd_api::{FiberState, Transition, TransitionCause};
use jinnd_effects::{EffectScope, ReplayReport};
use tokio_util::sync::CancellationToken;

use crate::landing::land;

use super::Cell;

impl Cell {
    /// Drains this activation's scope, replays it, and starts the next one
    /// from an empty tree.
    ///
    /// The drain pass runs every draining effect's phase — a dying provider
    /// waits out its dependents — to completion BEFORE any inverse replays
    /// (I2, paper Alg 5): dependents unloading during the drain still call
    /// the dying service and observe its contribution whole. The replay runs
    /// inside the teardown context marker: plugin-owned inverses execute on
    /// this fiber's task, and anything they call can consult
    /// [`crate::in_teardown`] to refuse work that must not wait on a
    /// teardown in flight (R1, M1-P6b). The task-agnostic half is the
    /// withdrawal cell, raised for exactly the drain-and-replay span: work an
    /// inverse spawns onto another task escapes the marker but happens-after
    /// the raise, so [`crate::Fiber::withdrawing`] still answers it truthfully.
    ///
    /// `full` selects the withdrawal proper — every inverse runs — over the
    /// suspend replay (M2-K4): suspendable effects run their suspend path,
    /// every other effect its inverse, world mutations retained.
    pub(super) async fn withdraw(&mut self, full: bool) -> ReplayReport {
        let cancel = CancellationToken::new();
        let report = {
            let Cell {
                shared,
                signal,
                scope,
                ..
            } = self;
            let _span = shared.withdrawal.begin();
            let work = async {
                scope.drain().await;
                if full {
                    scope.replay().await
                } else {
                    scope.suspend().await
                }
            };
            crate::teardown::marked(land(work, signal.as_mut(), shared, &cancel)).await
        };
        self.scope = EffectScope::new();
        self.shared.replayed(report.clone());
        self.publish_effects();
        report
    }

    pub(super) fn publish(&self, to: FiberState, cause: TransitionCause) {
        let from = *self.shared.state.borrow();
        if from == to {
            return;
        }
        // Recorded before it is published, so an observer woken by the state change
        // never reads a history that is missing the transition it just saw.
        self.shared.transitioned(Transition {
            fiber: self.shared.id,
            from,
            to,
            cause,
        });
        self.shared.state.send_replace(to);
    }

    pub(super) fn publish_effects(&self) {
        self.shared.published(self.scope.tree());
    }
}
