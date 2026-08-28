//! The per-fiber supervisor task (R1).
//!
//! One tokio task owns one fiber's whole life. It holds the plugin body, the
//! activation's effect scope and the committed state, so none of those is ever
//! behind a lock that a plugin call could be holding — the fiber's state is
//! published through a `watch` channel instead, and the only shared mutable cell is
//! the steering one, whose lock is never held across an `await`.
//!
//! The loop is: read every input, ask [`plan`] for the one transition owed, run it
//! to completion, then read every input again. A transition that is launched always
//! lands: targets that arrive while it is in flight are absorbed into the steering
//! cell and reconciled afterwards, never raced against it.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use jinnd_api::{FiberState, Transition, TransitionCause};
use jinnd_effects::{EffectScope, ReplayReport};
use tokio_util::sync::CancellationToken;

use crate::body::FiberBody;
use crate::landing::land;
use crate::plan::{Aim, Committed, Step, plan};
use crate::readiness::ReadinessSignal;
use crate::shared::Shared;

mod contained;

use contained::{activate, unclean};

/// Everything one fiber's supervisor owns outright.
pub(crate) struct Cell {
    shared: Arc<Shared>,
    body: Arc<dyn FiberBody>,
    signal: Box<dyn ReadinessSignal>,
    scope: EffectScope,
    committed: Committed,
}

impl Cell {
    pub(crate) fn new(
        shared: Arc<Shared>,
        body: Arc<dyn FiberBody>,
        signal: Box<dyn ReadinessSignal>,
    ) -> Self {
        Self {
            shared,
            body,
            signal,
            scope: EffectScope::new(),
            committed: Committed::new(),
        }
    }
}

/// Runs one fiber until it is disposed.
///
/// One round per iteration: read the probe, read the inputs, and either run the one
/// transition they owe or acknowledge that they owe none. Reading the probe *before*
/// the inputs is what makes the acknowledgement mean "I re-read every input after
/// you asked, and there was nothing left to do" — and re-reading it every round is
/// what keeps that true when a wake-up is absorbed by a transition already in flight
/// rather than by the loop.
pub(crate) async fn supervise(mut cell: Cell) {
    loop {
        let asked = cell.shared.probe.load(Ordering::SeqCst);
        cell.sync_signal();
        // Rest was already lowered by whatever target write owes this step —
        // atomically, in the steering cell's own critical section (M1-P6c
        // round 3) — so code a transition reaches never observes its own
        // fiber at rest. The settle below presents the stamp this read
        // observed: a target that moves meanwhile makes the settle stale and
        // rest stays lowered until the next round serves it.
        let (desired, observed) = cell.shared.steering.observed();
        if let Some(step) = plan(&cell.committed, &desired) {
            cell.run(step).await;
            continue;
        }
        cell.shared.steering.settle_rest(observed);
        cell.shared.settle(asked);
        if cell.committed.state == FiberState::Disposed || cell.committed.disposal_failed {
            // Settled for good: no probe will ever go unanswered again.
            cell.shared.settle(u64::MAX);
            return;
        }
        cell.wait().await;
    }
}

impl Cell {
    /// Mirrors the readiness signal into the steering cell.
    fn sync_signal(&self) {
        self.shared.steering.set_epoch(self.signal.epoch());
    }

    fn next_step(&self) -> Option<Step> {
        plan(&self.committed, &self.shared.steering.desired())
    }

    /// Blocks until some input may have moved.
    async fn wait(&mut self) {
        let Cell { shared, signal, .. } = self;
        tokio::select! {
            () = shared.wake.notified() => {}
            () = signal.changed() => {}
        }
    }

    async fn run(&mut self, step: Step) {
        match step {
            Step::Load { aim, cause } => self.load(aim, cause).await,
            Step::Unload { cause } => self.unload(cause).await,
            Step::Finish => self.finish().await,
        }
    }

    /// Runs the plugin body once, for `aim`.
    ///
    /// The activation lands whatever happens to the target meanwhile; a target that
    /// moved is reported to the body through its cancellation token and reconciled
    /// after the landing. An activation that is already stale when it lands never
    /// publishes `Active` — the states a fiber shows are states it rests in.
    async fn load(&mut self, aim: Aim, cause: TransitionCause) {
        let Some(epoch) = aim.epoch.clone() else {
            return;
        };
        self.shared.steering.launch(aim.clone());
        self.publish(FiberState::Loading, cause.clone());

        let cancel = CancellationToken::new();
        let outcome = {
            let Cell {
                shared,
                body,
                signal,
                scope,
                ..
            } = self;
            let work = activate(body.as_ref(), scope, shared.id, &epoch, cancel.clone());
            land(work, signal.as_mut(), shared, &cancel).await
        };
        self.shared.steering.land();

        match outcome {
            Ok(()) => {
                self.committed.state = FiberState::Active;
                self.committed.active_for = Some(aim);
                self.committed.failed_under = None;
                self.publish_effects();
                self.sync_signal();
                if self.next_step().is_none() {
                    self.publish(FiberState::Active, cause);
                }
            }
            Err(error) => {
                // Exactly what this activation applied, and nothing else, is
                // withdrawn (I1); the fiber then rests failed rather than retrying
                // against an environment that has not moved (R9).
                self.shared.fail(error);
                self.publish(FiberState::Unloading, cause.clone());
                self.withdraw(true).await;
                self.committed.state = FiberState::Failed;
                self.committed.active_for = None;
                self.committed.failed_under = Some(aim);
                self.publish(FiberState::Failed, cause);
            }
        }
    }

    /// Stops the live activation: a disposal withdraws the full inverse
    /// trail; every other unload — a restart, a suspension — SUSPENDS it
    /// (M2-K4): the entry persists, so its world mutations are retained and
    /// only kernel registrations release. The mode is the planned step's,
    /// decided before any inverse runs; a disposal arriving meanwhile lands
    /// afterwards as a `Finish` over what the suspension left.
    async fn unload(&mut self, cause: TransitionCause) {
        self.publish(FiberState::Unloading, cause.clone());
        let full = cause == TransitionCause::ExplicitDispose;
        let report = self.withdraw(full).await;
        self.committed.active_for = None;

        let desired = self.shared.steering.desired();
        if desired.disposing || desired.suspending {
            self.disposal_landed(&report, cause);
        } else if report.is_clean() {
            self.committed.state = FiberState::Pending;
            self.publish(FiberState::Pending, cause);
        } else {
            self.shared.fail(unclean(self.shared.id, &report));
            self.committed.state = FiberState::Failed;
            self.committed.failed_under = Some(self.shared.steering.desired().aim);
            self.publish(FiberState::Failed, cause);
        }
    }

    /// Disposes (or suspends) a fiber that holds no live activation.
    async fn finish(&mut self) {
        let cause = if self.shared.steering.desired().disposing {
            TransitionCause::ExplicitDispose
        } else {
            TransitionCause::Suspend
        };
        let report = self
            .withdraw(cause == TransitionCause::ExplicitDispose)
            .await;
        self.disposal_landed(&report, cause);
    }

    /// Commits the state a disposal's — or a suspension's — replay earned:
    /// `Disposed` for a clean replay, `Failed` for an unclean one (R11) — a
    /// fiber that could not withdraw never claims it is gone, and the failed
    /// replay is not reattempted against an unchanged scope (R9). Either
    /// way the replay has completed and reported, under `cause`.
    fn disposal_landed(&mut self, report: &ReplayReport, cause: TransitionCause) {
        if report.is_clean() {
            self.committed.state = FiberState::Disposed;
            self.publish(FiberState::Disposed, cause);
        } else {
            self.shared.fail(unclean(self.shared.id, report));
            self.committed.state = FiberState::Failed;
            self.committed.disposal_failed = true;
            self.publish(FiberState::Failed, cause);
        }
    }

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
    async fn withdraw(&mut self, full: bool) -> ReplayReport {
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

    fn publish(&self, to: FiberState, cause: TransitionCause) {
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

    fn publish_effects(&self) {
        self.shared.published(self.scope.tree());
    }
}
