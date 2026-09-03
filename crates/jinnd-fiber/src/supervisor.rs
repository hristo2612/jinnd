//! The per-fiber supervisor task (R1).
//!
//! One tokio task owns one fiber's whole life. It holds the plugin body and the
//! activation's effect scope, so neither is ever behind a lock that a plugin call
//! could be holding — the fiber's state is published through a `watch` channel,
//! and the only shared mutable cell is the steering one, whose lock is never held
//! across an `await`. What has LANDED lives in that cell too (M2-K9 round 3), with
//! this task as its only writer: a reader asking what the fiber owes then reads the
//! landed state and the target together, never one of them stale.
//!
//! The loop is: read every input, ask [`plan`] for the one transition owed, run it
//! to completion, then read every input again. A transition that is launched always
//! lands: targets that arrive while it is in flight are absorbed into the steering
//! cell and reconciled afterwards, never raced against it.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use jinnd_api::{FiberState, TransitionCause};
use jinnd_effects::EffectScope;
use tokio_util::sync::CancellationToken;

use crate::body::{FaultSink, FiberBody};
use crate::landing::land;
use crate::plan::{Aim, Step, plan};
use crate::readiness::ReadinessSignal;
use crate::shared::Shared;

mod contained;
mod landed;
mod publish;

use contained::{activate, unclean};

/// Everything one fiber's supervisor owns outright.
pub(crate) struct Cell {
    shared: Arc<Shared>,
    body: Arc<dyn FiberBody>,
    signal: Box<dyn ReadinessSignal>,
    scope: EffectScope,
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
        let (committed, desired, observed) = cell.shared.steering.observed();
        if let Some(step) = plan(&committed, &desired) {
            cell.run(step).await;
            continue;
        }
        cell.shared.steering.settle_rest(observed);
        cell.shared.settle(asked);
        if committed.state == FiberState::Disposed || committed.disposal_failed {
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
        let (committed, desired, _) = self.shared.steering.observed();
        plan(&committed, &desired)
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
            // Minted AFTER the launch, so it names this incarnation (M2-K25).
            let faults = FaultSink::new(Arc::clone(shared));
            let work = activate(
                body.as_ref(),
                scope,
                shared.id,
                &epoch,
                cancel.clone(),
                faults,
            );
            land(work, signal.as_mut(), shared, &cancel).await
        };
        self.shared.steering.land();

        match outcome {
            Ok(()) => {
                self.activated(aim);
                self.publish_effects();
                self.sync_signal();
                if self.next_step().is_none() {
                    self.publish(FiberState::Active, cause);
                }
            }
            Err(error) => {
                // The doom is COMMITTED BEFORE the cleanup it causes (M2-K9
                // round 4). Nothing about the outcome is still open here: the
                // activation has failed, and R9 already forbids retrying this
                // aim. Landing that decision first is what makes the state a
                // concurrent reader sees during the cleanup already TRUE —
                // round 3 committed it afterwards, so the whole cleanup was
                // spent answering callers from a snapshot this failure was
                // about to invalidate, and they were promised a replacement
                // R9 would never schedule.
                self.shared.fail(error);
                self.activation_failed(aim);
                // Exactly what this activation applied, and nothing else, is
                // withdrawn (I1); the fiber then rests failed rather than retrying
                // against an environment that has not moved (R9).
                self.publish(FiberState::Unloading, cause.clone());
                self.withdraw(true).await;
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
        if cause == TransitionCause::BodyFaulted {
            return self.fault_unload().await;
        }
        self.publish(FiberState::Unloading, cause.clone());
        let full = cause == TransitionCause::ExplicitDispose;
        let report = self.withdraw(full).await;
        let desired = self.shared.steering.desired();
        if desired.disposing || desired.suspending {
            self.disposal_landed(&report, cause);
        } else if report.is_clean() {
            self.unloaded();
            self.publish(FiberState::Pending, cause);
        } else {
            self.shared.fail(unclean(self.shared.id, &report));
            self.unload_failed(&desired.aim);
            self.publish(FiberState::Failed, cause);
        }
    }

    /// The live incarnation died (M2-K25): the doom is COMMITTED BEFORE
    /// the cleanup (the M2-K9 round-4 ordering law — a reader mid-cleanup
    /// already sees `Failed`, never a replacement R9 will not schedule),
    /// then the activation's whole contribution withdraws — guest inverses
    /// answer as they can, the residue is in the replay report — and the
    /// fiber rests `Failed` under the aim the dead activation served, so
    /// it is not retried until that aim moves (R9, R11).
    async fn fault_unload(&mut self) {
        let dead = self.shared.steering.observed().0.active_for;
        self.faulted(dead);
        self.publish(FiberState::Unloading, TransitionCause::BodyFaulted);
        self.publish(FiberState::Failed, TransitionCause::BodyFaulted);
        self.withdraw(true).await;
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
}
