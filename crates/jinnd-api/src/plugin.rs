//! The typed plugin contract and its activation surface (pre-work extraction,
//! M1-P8; zero semantic change).

use std::fmt::Debug;

use crate::{ContextId, EffectHost, Epoch, FiberId, Inject, KernelFuture};

/// Result of one plugin activation.
#[derive(Debug)]
pub struct ActivationReceipt {
    pub fiber: FiberId,
    pub epoch: Epoch,
}

/// Context and dependency snapshot handed to a plugin body once per activation.
pub struct Activation<'a, D> {
    pub context: ContextId,
    pub fiber: FiberId,
    pub dependencies: &'a D,
    /// Teardown-effect registrar charged to this activation's fiber
    /// (authorized M1-P7 additive delta: I2 teardown-time observation).
    pub effects: &'a dyn EffectHost,
}

impl<D: Debug> Debug for Activation<'_, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Activation")
            .field("context", &self.context)
            .field("fiber", &self.fiber)
            .field("dependencies", &self.dependencies)
            .finish_non_exhaustive()
    }
}

/// Typed plugin contract. Implementations execute only behind a sandboxed host.
pub trait PluginContract: Send + Sync + 'static {
    type Config: Clone + Debug + Send + Sync + 'static;
    type Dependencies: Inject;

    const NAME: &'static str;

    fn activate<'a>(
        &'a self,
        activation: Activation<'a, Self::Dependencies>,
        config: Self::Config,
    ) -> KernelFuture<'a, ()>;
}
