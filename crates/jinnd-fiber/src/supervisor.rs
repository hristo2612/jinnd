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

use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use jinnd_api::{Epoch, ErrorCode, FiberId, FiberState, KernelError, Transition, TransitionCause};
use jinnd_effects::{EffectScope, ReplayReport};
use tokio_util::sync::CancellationToken;

use crate::body::{FiberBody, Setup};
use crate::contain::contained;
use crate::plan::{Aim, Committed, Step, plan};
use crate::readiness::ReadinessSignal;
use crate::shared::Shared;

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
        if let Some(step) = cell.next_step() {
            cell.run(step).await;
            continue;
        }
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
                self.withdraw().await;
                self.committed.state = FiberState::Failed;
                self.committed.active_for = None;
                self.committed.failed_under = Some(aim);
                self.publish(FiberState::Failed, cause);
            }
        }
    }

    /// Withdraws the live activation.
    async fn unload(&mut self, cause: TransitionCause) {
        self.publish(FiberState::Unloading, cause.clone());
        let report = self.withdraw().await;
        self.committed.active_for = None;

        if self.shared.steering.desired().disposing {
            self.disposal_landed(&report);
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

    /// Disposes a fiber that holds no live activation.
    async fn finish(&mut self) {
        let report = self.withdraw().await;
        self.disposal_landed(&report);
    }

    /// Commits the state a disposal's withdrawal earned: `Disposed` for a clean
    /// replay, `Failed` for an unclean one (R11) — a fiber that could not withdraw
    /// never claims it is gone, and the failed replay is not reattempted against an
    /// unchanged scope (R9). Either way the withdrawal has completed and reported.
    fn disposal_landed(&mut self, report: &ReplayReport) {
        if report.is_clean() {
            self.committed.state = FiberState::Disposed;
            self.publish(FiberState::Disposed, TransitionCause::ExplicitDispose);
        } else {
            self.shared.fail(unclean(self.shared.id, report));
            self.committed.state = FiberState::Failed;
            self.committed.disposal_failed = true;
            self.publish(FiberState::Failed, TransitionCause::ExplicitDispose);
        }
    }

    /// Replays this activation's scope and starts the next one from an empty tree.
    async fn withdraw(&mut self) -> ReplayReport {
        let cancel = CancellationToken::new();
        let report = {
            let Cell {
                shared,
                signal,
                scope,
                ..
            } = self;
            let work = scope.replay();
            land(work, signal.as_mut(), shared, &cancel).await
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

/// Runs one activation behind panic containment (R11).
async fn activate<'a>(
    body: &'a dyn FiberBody,
    scope: &'a mut EffectScope,
    fiber: FiberId,
    epoch: &'a Epoch,
    cancel: CancellationToken,
) -> Result<(), KernelError> {
    let setup = Setup::new(fiber, epoch, scope, cancel);
    contained(fiber, move || body.activate(setup)).await
}

/// Drives one transition to its landing while absorbing every input that arrives.
///
/// This is the inertia lock in code: `work` is never dropped, never raced and never
/// aborted. Inputs are folded into the steering cell as they arrive, and the only
/// thing they may do to the transition in flight is tell it, cooperatively, that its
/// target has already moved.
async fn land<F: Future>(
    work: F,
    signal: &mut dyn ReadinessSignal,
    shared: &Shared,
    cancel: &CancellationToken,
) -> F::Output {
    let mut work = pin!(work);
    loop {
        tokio::select! {
            output = &mut work => return output,
            () = absorb(&mut *signal, shared, cancel) => {}
        }
    }
}

/// Waits for one input to move, folds it in, and reports staleness to the activation.
async fn absorb(signal: &mut dyn ReadinessSignal, shared: &Shared, cancel: &CancellationToken) {
    {
        tokio::select! {
            () = shared.wake.notified() => {}
            () = signal.changed() => {}
        }
    }
    shared.steering.set_epoch(signal.epoch());
    if shared.steering.stale() {
        cancel.cancel();
    }
}

/// The failure an unclean withdrawal is recorded as.
fn unclean(fiber: FiberId, report: &ReplayReport) -> KernelError {
    let residue: Vec<&str> = report
        .unclean()
        .map(|effect| effect.label.as_str())
        .collect();
    KernelError {
        code: ErrorCode::EffectFailed,
        message: format!(
            "these effects were not withdrawn cleanly: {}",
            residue.join(", ")
        ),
        fiber: Some(fiber),
    }
}
