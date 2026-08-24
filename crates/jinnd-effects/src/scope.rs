//! The live effect tree and its last-in-first-out teardown.

use jinnd_api::{EffectDescriptor, EffectId, KernelError, Undo};
use tokio_util::sync::CancellationToken;

use crate::report::ReplayReport;
use crate::undo::{StepwiseUndo, UndoStep};

/// The inverse one effect registered.
pub enum Disposer {
    /// One inverse that runs to completion in a single call.
    Whole(Box<dyn Undo>),
    /// An ordered sequence with a cancellation point between steps.
    Stepwise(StepwiseUndo),
}

impl Disposer {
    /// A synchronous inverse.
    pub fn sync<F>(_undo: F) -> Self
    where
        F: FnOnce() -> Result<(), KernelError> + Send + 'static,
    {
        todo!("NO_IMPL: disposer forms")
    }

    /// An awaited inverse.
    pub fn future<F, Fut>(_undo: F) -> Self
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), KernelError>> + Send + 'static,
    {
        todo!("NO_IMPL: disposer forms")
    }

    /// A stepwise inverse with a cancellation point between steps.
    #[must_use]
    pub fn stepwise(_steps: Vec<UndoStep>, _cancel: CancellationToken) -> Self {
        todo!("NO_IMPL: disposer forms")
    }
}

/// One scope's live effect tree.
pub struct EffectScope;

impl EffectScope {
    /// An empty scope.
    #[must_use]
    pub fn new() -> Self {
        todo!("NO_IMPL: effect tree")
    }

    /// Registers an effect at the top of this scope.
    pub fn register(
        &mut self,
        _label: impl Into<String>,
        _disposer: impl Into<Disposer>,
    ) -> Result<EffectId, KernelError> {
        todo!("NO_IMPL: effect tree")
    }

    /// Registers an effect nested under `parent`.
    pub fn register_child(
        &mut self,
        _parent: EffectId,
        _label: impl Into<String>,
        _disposer: impl Into<Disposer>,
    ) -> Result<EffectId, KernelError> {
        todo!("NO_IMPL: effect tree")
    }

    /// The live effect tree, with labels and nesting.
    #[must_use]
    pub fn tree(&self) -> Vec<EffectDescriptor> {
        todo!("NO_IMPL: effect tree")
    }

    /// True when this scope holds no live effect.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        todo!("NO_IMPL: effect tree")
    }

    /// Withdraws every live effect, last registered first.
    pub async fn replay(&mut self) -> ReplayReport {
        todo!("NO_IMPL: replay")
    }
}

impl Default for EffectScope {
    fn default() -> Self {
        Self::new()
    }
}
