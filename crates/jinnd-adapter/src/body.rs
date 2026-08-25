//! The facade plugin wrapped as a fiber body.

use std::sync::Mutex;

use jinnd_api::{Activation, Inject, KernelFuture, PluginContract};
use jinnd_context::Context;
use jinnd_effects::Disposer;
use jinnd_fiber::{FiberBody, Setup};
use jinnd_registry::{ActivationResolver, Registry};

/// One facade plugin behind the fiber engine's body seam.
///
/// The config and context cells hold the *latest stated* values; each
/// activation reads them once at its start, so a config update or a loader
/// rebind lands as a full clean reload observing the new value (§3, "Epoch
/// gating" — never a mutation under a live activation).
pub(crate) struct FacadeBody<P: PluginContract> {
    plugin: P,
    at: Mutex<Context<()>>,
    registry: Registry,
    config: Mutex<P::Config>,
}

impl<P: PluginContract> FacadeBody<P> {
    pub(crate) fn new(plugin: P, at: Context<()>, registry: Registry, config: P::Config) -> Self {
        Self {
            plugin,
            at: Mutex::new(at),
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

    /// States a rebuilt context for the next activation to resolve in.
    pub(crate) fn rebind(&self, at: Context<()>) {
        *self.at.lock().unwrap_or_else(|poison| poison.into_inner()) = at;
    }

    fn context(&self) -> Context<()> {
        self.at
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }
}

impl<P: PluginContract> FiberBody for FacadeBody<P> {
    fn activate<'a>(&'a self, mut setup: Setup<'a>) -> KernelFuture<'a, ()> {
        let config = self
            .config
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone();
        let at = self.context();
        let fiber = setup.fiber();
        Box::pin(async move {
            // One owned dependency snapshot per activation (R4), each resolution
            // leased so a dying provider waits for this consumer (I2).
            let resolver = ActivationResolver::new(&self.registry, &at);
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
                        context: at.id(),
                        fiber,
                        dependencies: &dependencies,
                    },
                    config,
                )
                .await
        })
    }
}
