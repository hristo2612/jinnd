//! The three forms an effect's inverse can take.

use std::future::Future;

use jinnd_api::{KernelError, Undo};
use tokio_util::sync::CancellationToken;

use crate::undo::{FutureUndo, StepwiseUndo, UndoStep};

/// The inverse one effect registered.
///
/// Every form is consumed by value when it runs, which is what makes exactly-once
/// withdrawal structural rather than a flag the engine has to keep honest.
pub enum Disposer {
    /// One inverse that runs to completion in a single call.
    Whole(Box<dyn Undo>),
    /// An ordered sequence with a cooperative cancellation point between steps.
    Stepwise(StepwiseUndo),
}

impl Disposer {
    /// A synchronous inverse.
    pub fn sync<F>(undo: F) -> Self
    where
        F: FnOnce() -> Result<(), KernelError> + Send + 'static,
    {
        Self::future(move || std::future::ready(undo()))
    }

    /// An awaited inverse. The closure runs at replay, never at registration.
    pub fn future<F, Fut>(undo: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), KernelError>> + Send + 'static,
    {
        Self::Whole(Box::new(FutureUndo::new(undo)))
    }

    /// A stepwise inverse with a cancellation point between steps.
    #[must_use]
    pub fn stepwise(steps: Vec<UndoStep>, cancel: CancellationToken) -> Self {
        Self::Stepwise(StepwiseUndo::new(steps, cancel))
    }
}
