//! The inverse behind a disposer, and the steps a stepwise one replays.

use std::future::Future;

use jinnd_api::{KernelError, KernelFuture, Undo};
use tokio_util::sync::CancellationToken;

/// The [`Undo`] every whole disposer is built from.
///
/// The closure runs when the inverse is replayed, not when it is registered: an
/// inverse never starts work at registration time (R9 — no side-effectful
/// construction). A synchronous inverse is the same thing returning a ready future,
/// so the engine has one adapter to reason about rather than two (R10).
pub struct FutureUndo<F>(F);

impl<F, Fut> FutureUndo<F>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), KernelError>> + Send + 'static,
{
    /// Registers `undo` as the inverse of the effect being applied.
    pub fn new(undo: F) -> Self {
        Self(undo)
    }
}

impl<F, Fut> Undo for FutureUndo<F>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), KernelError>> + Send + 'static,
{
    fn undo(self: Box<Self>) -> KernelFuture<'static, ()> {
        Box::pin((self.0)())
    }
}

/// One step of a stepwise inverse, built when the step is reached.
pub type UndoStep = Box<dyn FnOnce() -> KernelFuture<'static, ()> + Send + 'static>;

/// Lifts a synchronous closure into an [`UndoStep`].
pub fn step<F>(undo: F) -> UndoStep
where
    F: FnOnce() -> Result<(), KernelError> + Send + 'static,
{
    Box::new(move || Box::pin(std::future::ready(undo())))
}

/// An ordered sequence of inverse steps with a cancellation point between steps.
///
/// The engine checks `cancel` before each step and stops there, reporting how far it
/// got, so a withdrawal that was interrupted is never mistaken for a complete one.
/// The token is the seam the fiber engine fills in: it hands over a token whose
/// cancellation is epoch-checked, and that knowledge stays out of this crate (R10).
pub struct StepwiseUndo {
    steps: Vec<UndoStep>,
    cancel: CancellationToken,
}

impl StepwiseUndo {
    /// Registers `steps`, to be replayed in the given order under `cancel`.
    #[must_use]
    pub fn new(steps: Vec<UndoStep>, cancel: CancellationToken) -> Self {
        Self { steps, cancel }
    }

    pub(crate) fn into_parts(self) -> (Vec<UndoStep>, CancellationToken) {
        (self.steps, self.cancel)
    }
}
