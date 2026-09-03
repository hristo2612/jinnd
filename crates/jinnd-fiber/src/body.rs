//! What a plugin body is, from the fiber's side.

use std::sync::Arc;

use jinnd_api::{EffectId, Epoch, FiberId, KernelError, KernelFuture};
use jinnd_effects::{Disposer, EffectScope};
use tokio_util::sync::CancellationToken;

use crate::shared::Shared;

/// One plugin's activation, run once per activation of the fiber that owns it.
///
/// The body is the only code in this crate the kernel does not own, so it is the
/// only place a panic or an error can originate; both are contained at this boundary
/// (R11) and neither can reach a sibling fiber.
pub trait FiberBody: Send + Sync + 'static {
    /// Applies this plugin's contribution, registering an inverse for each part of
    /// it through `setup` (R5).
    fn activate<'a>(&'a self, setup: Setup<'a>) -> KernelFuture<'a, ()>;
}

/// The activation's handle on its own fiber.
///
/// Everything a body may do to shared state goes through here, so the fiber's
/// contribution is exactly the effect tree this scope holds — which is what makes
/// withdrawing it exact (I1).
#[derive(Debug)]
pub struct Setup<'a> {
    fiber: FiberId,
    epoch: &'a Epoch,
    effects: &'a mut EffectScope,
    cancel: CancellationToken,
    faults: FaultSink,
}

impl<'a> Setup<'a> {
    pub(crate) fn new(
        fiber: FiberId,
        epoch: &'a Epoch,
        effects: &'a mut EffectScope,
        cancel: CancellationToken,
        faults: FaultSink,
    ) -> Self {
        Self {
            fiber,
            epoch,
            effects,
            cancel,
            faults,
        }
    }

    /// The channel this activation reports its OWN death through
    /// (M2-K25): a body whose live instance the kernel ends after
    /// activation — a deadline, a trap, an exhausted delivery budget —
    /// hands the error here, and the fiber fails itself on the record.
    /// Cloneable and `'static`: a watcher task keeps it past the
    /// activation's return.
    #[must_use]
    pub fn faults(&self) -> FaultSink {
        self.faults.clone()
    }

    /// The fiber being activated.
    #[must_use]
    pub fn fiber(&self) -> FiberId {
        self.fiber
    }

    /// The dependency identity this activation was made for.
    ///
    /// It is fixed for the activation's whole life: a provider that changes forces a
    /// clean unload and a new activation rather than a silent swap under this one.
    #[must_use]
    pub fn epoch(&self) -> &Epoch {
        self.epoch
    }

    /// True once this activation is known to be obsolete.
    ///
    /// Cooperative only (R1): nothing is aborted from outside, and a body that
    /// ignores this still runs to completion and still lands. Observing it lets a
    /// long activation stop applying work that is already owed a withdrawal —
    /// whatever it did apply is withdrawn exactly, whichever it chooses.
    #[must_use]
    pub fn cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// The token behind [`cancelled`](Setup::cancelled), for inverses that want a
    /// cancellation point of their own.
    #[must_use]
    pub fn cancellation(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Applies one effect by registering its inverse (R5).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InactiveContext`](jinnd_api::ErrorCode::InactiveContext) once
    /// this activation's scope has been replayed.
    pub fn effect(
        &mut self,
        label: impl Into<String>,
        disposer: Disposer,
    ) -> Result<EffectId, KernelError> {
        self.effects.register(label, disposer)
    }

    /// Applies an effect with a drain phase (I2): `drain` runs before ANY
    /// inverse of this fiber's withdrawal — the provision shape, where
    /// dependents must be waited out while the contribution is still whole —
    /// and `undo` is the inverse proper, complete on its own.
    ///
    /// # Errors
    ///
    /// As [`effect`](Setup::effect).
    pub fn draining_effect(
        &mut self,
        label: impl Into<String>,
        drain: Disposer,
        undo: Disposer,
    ) -> Result<EffectId, KernelError> {
        self.effects.register_draining(label, drain, undo)
    }

    /// Applies an effect with a suspend path (M2-K4): `undo` is the inverse
    /// a full withdrawal runs — disposal, a failed activation's cleanup —
    /// and `suspend` is what a suspension runs INSTEAD, releasing the
    /// effect's kernel-held resources while its world mutation is retained
    /// for the entry's next incarnation (decision log 2026-08-28; Law 3).
    ///
    /// # Errors
    ///
    /// As [`effect`](Setup::effect).
    pub fn suspendable_effect(
        &mut self,
        label: impl Into<String>,
        undo: Disposer,
        suspend: Disposer,
    ) -> Result<EffectId, KernelError> {
        self.effects.register_suspendable(label, undo, suspend)
    }

    /// Applies an effect nested under `parent`, so that withdrawing `parent`
    /// withdraws this one first.
    ///
    /// # Errors
    ///
    /// As [`effect`](Setup::effect), plus
    /// [`ErrorCode::EffectFailed`](jinnd_api::ErrorCode::EffectFailed) when `parent`
    /// is not live in this activation's scope.
    pub fn child_effect(
        &mut self,
        parent: EffectId,
        label: impl Into<String>,
        disposer: Disposer,
    ) -> Result<EffectId, KernelError> {
        self.effects.register_child(parent, label, disposer)
    }
}

/// One incarnation's fault channel — the fiber engine's ONE
/// post-activation input (M2-K25). Minted per activation, so a fault
/// names the incarnation that reported it: the fiber plans
/// `Active → Unloading → Failed` under [`TransitionCause::BodyFaulted`]
/// for the live one, and records without acting a notice from an
/// incarnation that has already gone (R9 — a death never dooms the
/// successor; R11 — it dooms nothing but its own cell).
///
/// [`TransitionCause::BodyFaulted`]: jinnd_api::TransitionCause::BodyFaulted
#[derive(Clone, Debug)]
pub struct FaultSink {
    shared: Arc<Shared>,
    incarnation: u64,
}

impl FaultSink {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        let incarnation = shared.steering.incarnation();
        Self {
            shared,
            incarnation,
        }
    }

    /// Reports the death. The error lands on the fiber's record under the
    /// fiber's attribution whatever happens next (no lost fault); the
    /// fiber is doomed only when this is the LIVE incarnation, which the
    /// answer says. Never blocks, never awaits, never touches plugin code
    /// (R1): a target write and a wake, like every other input.
    pub fn fault(&self, mut error: KernelError) -> bool {
        error.fiber.get_or_insert(self.shared.id);
        self.shared.fail(error);
        let acts = self.shared.steering.fault(self.incarnation);
        if acts {
            self.shared.wake.notify_one();
        }
        acts
    }
}
