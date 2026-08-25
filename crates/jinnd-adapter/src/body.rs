//! The facade plugin wrapped as a fiber body.

use std::sync::Mutex;

use jinnd_api::{Activation, ContextId, Inject, KernelFuture, PluginContract};
use jinnd_context::Context;
use jinnd_effects::Disposer;
use jinnd_fiber::{FiberBody, Setup};
use jinnd_registry::{ActivationResolver, Registry};

/// One facade plugin behind the fiber engine's body seam.
///
/// The config cell holds the *latest stated* config; each activation reads it
/// once at its start, so a config update lands as a full clean reload observing
/// the new value (§3, "Epoch gating" — never a mutation under a live activation).
pub(crate) struct FacadeBody<P: PluginContract> {
    plugin: P,
    context: ContextId,
    at: Context<()>,
    registry: Registry,
    config: Mutex<P::Config>,
}

impl<P: PluginContract> FacadeBody<P> {
    pub(crate) fn new(
        plugin: P,
        context: ContextId,
        at: Context<()>,
        registry: Registry,
        config: P::Config,
    ) -> Self {
        Self {
            plugin,
            context,
            at,
            registry,
            config: Mutex::new(config),
        }
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
    fn activate<'a>(&'a self, mut setup: Setup<'a>) -> KernelFuture<'a, ()> {
        let config = self
            .config
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        let fiber = setup.fiber();
        Box::pin(async move {
            // One owned dependency snapshot per activation (R4), each resolution
            // leased so a dying provider waits for this consumer (I2).
            let resolver = ActivationResolver::new(&self.registry, &self.at);
            let dependencies = P::Dependencies::inject(&resolver)?;
            let guards = resolver.into_guards();
            if !guards.is_empty() {
                // Registered before any plugin effect: LIFO replay returns the
                // leases last, so teardown may still call the dying service (I2).
                setup.effect(
                    "injected service leases",
                    Disposer::sync(move || {
                        drop(guards);
                        Ok(())
                    }),
                )?;
            }
            self.plugin
                .activate(
                    Activation {
                        context: self.context,
                        fiber,
                        dependencies: &dependencies,
                    },
                    config,
                )
                .await
        })
    }
}
