//! Loader-lane wiring: how facade package registrations become the loader's
//! [`PackageLane`]s (authorized M1-P6 adapter delta; R1, R3, R5). The generic
//! bodies and handles live in `jinnd_loader::host`; this module contributes
//! only what is harness-specific — the facade body and the shared fiber map.

use std::any::Any;
use std::sync::Arc;

use jinnd_api::{Inject, KernelError, PluginContract, ServiceContract, ServiceType};
use jinnd_fiber::{Fiber, FiberBody};
use jinnd_loader::host::{LaneHandle, ProviderBody, Rebind, config_of};
use jinnd_loader::{EntryHandle, PackageLane, SpawnRequest};
use jinnd_registry::Registry;

use crate::body::FacadeBody;
use crate::{FiberEntry, SharedFibers};

impl<P: PluginContract> Rebind for FacadeBody<P> {
    fn rebind(&self, at: jinnd_context::Context<()>) {
        FacadeBody::rebind(self, at);
    }
}

/// A lane for plugin entries built by `build` from an entry's config payload.
///
/// # Errors
///
/// [`jinnd_api::ErrorCode::PluginFailed`] when the plugin's dependency
/// declaration panics — the lane is never built (R11).
pub(crate) fn plugin_lane<C, P, F>(
    fibers: SharedFibers,
    registry: Registry,
    build: F,
) -> Result<PackageLane, KernelError>
where
    C: Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static,
    P: PluginContract,
    F: Fn(C) -> Result<(P, P::Config), KernelError> + Send + Sync + 'static,
{
    let build = Arc::new(build);
    Ok(PackageLane {
        injects: declared::<P>()?,
        provides: None,
        spawn: Box::new(move |request: SpawnRequest<'_>| {
            let config = config_of::<C>(request.config)?;
            let (plugin, plugin_config) = (build)(config)?;
            let body = Arc::new(FacadeBody::new(
                plugin,
                request.at.clone(),
                registry.clone(),
                plugin_config,
            ));
            let build = Arc::clone(&build);
            let restate = move |body: &FacadeBody<P>, config: C| {
                let (_, plugin_config) = (build)(config)?;
                body.state_config(plugin_config);
                Ok(())
            };
            Ok(spawned(&fibers, body, request, restate))
        }),
    })
}

/// A lane for provider entries: each activation provides `S`, built by
/// `provide` from the entry's config, in the realm the entry's context
/// resolves `S` in (LAW §3 isolation).
pub(crate) fn provider_lane<C, S, F>(
    fibers: SharedFibers,
    registry: Registry,
    provide: F,
) -> PackageLane
where
    C: Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static,
    S: ServiceContract,
    F: Fn(C) -> Result<Arc<S>, KernelError> + Send + Sync + 'static,
{
    let provide = Arc::new(provide);
    PackageLane {
        injects: Vec::new(),
        provides: Some(ServiceType::of::<S>()),
        spawn: Box::new(move |request: SpawnRequest<'_>| {
            let config = config_of::<C>(request.config)?;
            let body = Arc::new(ProviderBody::new(
                Arc::clone(&provide),
                registry.clone(),
                request.at.clone(),
                config,
            ));
            let restate = |body: &ProviderBody<C, S, F>, config: C| {
                body.state_config(config);
                Ok(())
            };
            Ok(spawned(&fibers, body, request, restate))
        }),
    }
}

/// The dependency declaration of `P`, gathered with its panic contained: the
/// declaration is plugin-owned code, so its panic is answered as this plugin's
/// failure — never as an empty declaration — and refuses the operation that
/// needed it, nothing else (R11; the M1-P4 spawn-boundary pattern).
pub(crate) fn declared<P: PluginContract>() -> Result<Vec<ServiceType>, KernelError> {
    std::panic::catch_unwind(P::Dependencies::declare).map_err(|_| KernelError {
        code: jinnd_api::ErrorCode::PluginFailed,
        message: "the dependency declaration panicked".to_owned(),
        fiber: None,
    })
}

/// Spawns `body` gated on `signal` and records it in the shared fiber map so
/// the facade answers for it — the one fiber-tracking seam (facade spawns and
/// loader lanes alike).
pub(crate) fn track<B: FiberBody>(
    fibers: &SharedFibers,
    body: Arc<B>,
    signal: impl jinnd_fiber::ReadinessSignal,
) -> Arc<FiberEntry> {
    let fiber = Arc::new(Fiber::spawn(
        Arc::clone(&body) as Arc<dyn FiberBody>,
        signal,
    ));
    let entry = Arc::new(FiberEntry {
        fiber,
        body: body as Arc<dyn Any + Send + Sync>,
    });
    crate::lock(fibers).insert(entry.fiber.id(), Arc::clone(&entry));
    entry
}

/// Tracks `body` per [`track`], wrapped as the loader's handle.
pub(crate) fn spawned<B, C, R>(
    fibers: &SharedFibers,
    body: Arc<B>,
    request: SpawnRequest<'_>,
    restate: R,
) -> Arc<dyn EntryHandle>
where
    B: FiberBody + Rebind,
    C: Clone + 'static,
    R: Fn(&B, C) -> Result<(), KernelError> + Send + Sync + 'static,
{
    let entry = track(fibers, Arc::clone(&body), request.signal);
    Arc::new(LaneHandle::new(Arc::clone(&entry.fiber), body, restate))
}
