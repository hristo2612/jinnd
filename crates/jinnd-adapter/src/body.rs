//! The facade plugin wrapped as a fiber body.

use std::sync::Mutex;

use jinnd_api::{
    Activation, EffectHost, ErrorCode, Inject, KernelError, KernelFuture, PluginContract, Undo,
};
use jinnd_context::Context;
use jinnd_effects::Disposer;
use jinnd_fiber::{FiberBody, Setup};
use jinnd_registry::{ActivationResolver, Registry};

/// The teardown-effect registrar handed to one activation (authorized M1-P7
/// additive delta: I2 teardown-time observation). Effects collected here are
/// flushed into the activation's scope after the plugin body settles —
/// success or failure alike, so a failing activation still owes its
/// registered inverses (I1) — and replay LIFO before the injected-service
/// leases return, so a teardown effect may still observe its dying
/// dependencies (I2).
/// One collected teardown effect: its label and its inverse.
type Collected = (String, Box<dyn Undo>);

pub(crate) struct HostedEffects {
    collected: Mutex<Option<Vec<Collected>>>,
}

impl HostedEffects {
    pub(crate) fn new() -> Self {
        Self {
            collected: Mutex::new(Some(Vec::new())),
        }
    }

    /// Flushes what the activation registered into its fiber scope, in
    /// registration order, and closes the host.
    pub(crate) fn flush(&self, setup: &mut Setup<'_>) -> Result<(), KernelError> {
        let drained = crate::lock(&self.collected).take().unwrap_or_default();
        for (label, undo) in drained {
            setup.effect(label, Disposer::Whole(undo))?;
        }
        Ok(())
    }
}

impl EffectHost for HostedEffects {
    fn register(&self, label: String, undo: Box<dyn Undo>) -> Result<(), KernelError> {
        match &mut *crate::lock(&self.collected) {
            Some(list) => {
                list.push((label, undo));
                Ok(())
            }
            None => Err(crate::error(
                ErrorCode::InactiveContext,
                "the activation has settled; its teardown effects are closed",
            )),
        }
    }
}

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
        Box::pin(async move {
            let dependencies = lease::<P::Dependencies>(&self.registry, &at, &mut setup)?;
            drive(&self.plugin, &at, &dependencies, config, &mut setup).await
        })
    }
}

/// One owned dependency snapshot per activation (R4), each resolution leased
/// so a dying provider waits for this consumer — registered before any plugin
/// effect, so LIFO replay returns the leases last and teardown may still call
/// the dying service (I2).
pub(crate) fn lease<D: Inject>(
    registry: &Registry,
    at: &Context<()>,
    setup: &mut Setup<'_>,
) -> Result<D, KernelError> {
    let resolver = ActivationResolver::new(registry, at);
    let dependencies = D::inject(&resolver)?;
    let guards = resolver.into_guards();
    if !guards.is_empty() {
        setup.effect(
            "injected service leases",
            Disposer::sync(move || {
                drop(guards);
                Ok(())
            }),
        )?;
    }
    Ok(dependencies)
}

/// Runs the plugin body once behind the teardown-effect host. Flushed on
/// success AND failure: a failing activation still owes the inverses it
/// registered (I1); the plugin's own error outranks a flush refusal.
pub(crate) async fn drive<P: PluginContract>(
    plugin: &P,
    at: &Context<()>,
    dependencies: &P::Dependencies,
    config: P::Config,
    setup: &mut Setup<'_>,
) -> Result<(), KernelError> {
    let host = HostedEffects::new();
    let outcome = plugin
        .activate(
            Activation {
                context: at.id(),
                fiber: setup.fiber(),
                dependencies,
                effects: &host,
            },
            config,
        )
        .await;
    let flushed = host.flush(setup);
    outcome.and(flushed)
}
