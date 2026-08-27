//! The providing-plugin lane: one package that both injects its plugin's
//! declared dependencies and provides a service from each activation
//! (authorized M1-P7 additive delta per the invariant_progress IOU — this is
//! the lane shape a dependency cycle is expressed through, I3).

use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use jinnd_api::{
    Activation, Inject, KernelError, KernelFuture, PluginContract, ServiceContract, ServiceType,
};
use jinnd_context::Context;
use jinnd_fiber::{FiberBody, Setup};
use jinnd_loader::host::Rebind;
use jinnd_loader::{PackageLane, SpawnRequest, host::config_of};
use jinnd_registry::{ActivationResolver, Registry};

use crate::body::HostedEffects;
use crate::{SharedFibers, lock};

/// One providing-plugin entry's body: resolves the plugin's dependencies,
/// provides `S` (built from the latest config) in the realm the entry's
/// context resolves it in, then runs the plugin body — the union of the
/// plugin and provider bodies, in the order I2 requires (leases first, then
/// the draining provision, then plugin effects).
pub(crate) struct ProvidingBody<C, S: ServiceContract, P: PluginContract, F> {
    build: Arc<F>,
    registry: Registry,
    at: Mutex<Context<()>>,
    config: Mutex<C>,
    _provides: PhantomData<fn() -> (S, P)>,
}

impl<C, S, P, F> ProvidingBody<C, S, P, F>
where
    C: Clone + std::fmt::Debug + Send + Sync + 'static,
    S: ServiceContract,
    P: PluginContract,
    F: Fn(C) -> Result<(P, P::Config, Arc<S>), KernelError> + Send + Sync + 'static,
{
    fn state_config(&self, config: C) {
        *lock(&self.config) = config;
    }
}

impl<C, S, P, F> Rebind for ProvidingBody<C, S, P, F>
where
    C: Clone + std::fmt::Debug + Send + Sync + 'static,
    S: ServiceContract,
    P: PluginContract,
    F: Fn(C) -> Result<(P, P::Config, Arc<S>), KernelError> + Send + Sync + 'static,
{
    fn rebind(&self, at: Context<()>) {
        *lock(&self.at) = at;
    }
}

impl<C, S, P, F> FiberBody for ProvidingBody<C, S, P, F>
where
    C: Clone + std::fmt::Debug + Send + Sync + 'static,
    S: ServiceContract,
    P: PluginContract,
    F: Fn(C) -> Result<(P, P::Config, Arc<S>), KernelError> + Send + Sync + 'static,
{
    fn activate<'a>(&'a self, mut setup: Setup<'a>) -> KernelFuture<'a, ()> {
        let fiber = setup.fiber();
        Box::pin(async move {
            let config = lock(&self.config).clone();
            let at = lock(&self.at).clone();
            // `build` is plugin-authored: its panic is this plugin's failure,
            // contained here (R11; the M1-P4 spawn-boundary pattern).
            let (plugin, plugin_config, value) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.build)(config)))
                    .map_err(|_| {
                    crate::error(
                        jinnd_api::ErrorCode::PluginFailed,
                        "the providing build panicked",
                    )
                })??;
            // One owned dependency snapshot per activation (R4), leased so a
            // dying provider waits for this consumer (I2) — before any other
            // effect, so LIFO replay returns the leases last.
            let resolver = ActivationResolver::new(&self.registry, &at);
            let dependencies = P::Dependencies::inject(&resolver)?;
            let guards = resolver.into_guards();
            if !guards.is_empty() {
                setup.effect(
                    "injected service leases",
                    jinnd_effects::Disposer::sync(move || {
                        drop(guards);
                        Ok(())
                    }),
                )?;
            }
            // The provision realm is whatever the entry's context resolves
            // the service in (LAW §3); a draining effect so dependents are
            // waited out before ANY of this fiber's inverses replay (I2).
            let tree = at.tree();
            let realm = tree
                .realm_value(at.realm_of(tree.key_of::<S>().name()))
                .unwrap_or(jinnd_api::Realm::Root);
            let provision = self.registry.provide::<S, ()>(
                &tree.root(),
                &realm,
                fiber,
                value,
                &self.registry.vitality(true),
            )?;
            setup.draining_effect(
                format!("provide {}", S::NAME),
                provision.drain,
                provision.undo,
            )?;
            let host = HostedEffects::new();
            let outcome = plugin
                .activate(
                    Activation {
                        context: at.id(),
                        fiber,
                        dependencies: &dependencies,
                        effects: &host,
                    },
                    plugin_config,
                )
                .await;
            let flushed = host.flush(&mut setup);
            outcome.and(flushed)
        })
    }
}

/// The lane: declares both halves, so static cycle detection sees the true
/// dependency graph (I3), and spawns [`ProvidingBody`] entries.
///
/// # Errors
///
/// [`jinnd_api::ErrorCode::PluginFailed`] when the plugin's dependency
/// declaration panics — the lane is never built (R11).
pub(crate) fn providing_lane<C, S, P, F>(
    fibers: SharedFibers,
    registry: Registry,
    build: F,
) -> Result<PackageLane, KernelError>
where
    C: Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static,
    S: ServiceContract,
    P: PluginContract,
    F: Fn(C) -> Result<(P, P::Config, Arc<S>), KernelError> + Send + Sync + 'static,
{
    let build = Arc::new(build);
    Ok(PackageLane {
        injects: crate::wiring::declared::<P>()?,
        provides: Some(ServiceType::of::<S>()),
        spawn: Box::new(move |request: SpawnRequest<'_>| {
            let config = config_of::<C>(request.config)?;
            let body = Arc::new(ProvidingBody::<C, S, P, F> {
                build: Arc::clone(&build),
                registry: registry.clone(),
                at: Mutex::new(request.at.clone()),
                config: Mutex::new(config),
                _provides: PhantomData,
            });
            let restate = |body: &ProvidingBody<C, S, P, F>, config: C| {
                body.state_config(config);
                Ok(())
            };
            Ok(crate::wiring::spawned(&fibers, body, request, restate))
        }),
    })
}
