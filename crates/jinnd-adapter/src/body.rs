//! The facade plugin wrapped as a fiber body.

use std::any::Any;
use std::sync::Mutex;

use jinnd_api::{Activation, ContextId, KernelFuture, PluginContract};
use jinnd_fiber::{FiberBody, Setup};

/// One facade plugin behind the fiber engine's body seam.
///
/// The config cell holds the *latest stated* config; each activation reads it
/// once at its start, so a config update lands as a full clean reload observing
/// the new value (§3, "Epoch gating" — never a mutation under a live activation).
pub(crate) struct FacadeBody<P: PluginContract> {
    plugin: P,
    context: ContextId,
    dependencies: P::Dependencies,
    config: Mutex<P::Config>,
}

impl<P: PluginContract> FacadeBody<P> {
    /// Wraps `plugin`, if its dependency type is one the facade can satisfy.
    ///
    /// The facade has no dependency-declaration API yet, so only a plugin whose
    /// `Dependencies` is `()` can honestly activate: anything else is refused at
    /// spawn rather than faked (the invariant suite's recorded facade gap).
    pub(crate) fn conjure(plugin: P, context: ContextId, config: P::Config) -> Option<Self> {
        let unit: Box<dyn Any> = Box::new(());
        let dependencies = *unit.downcast::<P::Dependencies>().ok()?;
        Some(Self {
            plugin,
            context,
            dependencies,
            config: Mutex::new(config),
        })
    }

    /// States a new latest config for the next activation to read.
    pub(crate) fn state_config(&self, config: P::Config) {
        *self
            .config
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = config;
    }
}

impl<P: PluginContract> FiberBody for FacadeBody<P> {
    fn activate<'a>(&'a self, setup: Setup<'a>) -> KernelFuture<'a, ()> {
        let config = self
            .config
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        let fiber = setup.fiber();
        Box::pin(async move {
            self.plugin
                .activate(
                    Activation {
                        context: self.context,
                        fiber,
                        dependencies: &self.dependencies,
                    },
                    config,
                )
                .await
        })
    }
}
