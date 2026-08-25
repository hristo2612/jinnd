//! Loader-lane wiring: how facade package registrations become the loader's
//! [`PackageLane`]s, and how spawned entries surface as [`EntryHandle`]s the
//! loader can drive (authorized M1-P6 adapter delta; R1, R3, R5).

use std::any::Any;
use std::sync::{Arc, Mutex};

use jinnd_api::{
    ErrorCode, FiberId, FiberState, Inject, KernelError, KernelFuture, PluginContract,
    ServiceContract, ServiceType, TransitionCause,
};
use jinnd_context::Context;
use jinnd_fiber::{Fiber, FiberBody, Setup};
use jinnd_loader::{EntryHandle, PackageLane, SpawnRequest};
use jinnd_registry::Registry;

use crate::body::FacadeBody;
use crate::{FiberEntry, SharedFibers, error};

/// A lane for plugin entries built by `build` from an entry's config payload.
pub(crate) fn plugin_lane<C, P, F>(
    fibers: SharedFibers,
    registry: Registry,
    build: F,
) -> PackageLane
where
    C: Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static,
    P: PluginContract,
    F: Fn(C) -> Result<(P, P::Config), KernelError> + Send + Sync + 'static,
{
    let build = Arc::new(build);
    PackageLane {
        injects: declared::<P>(),
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
    }
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
            let body = Arc::new(ProviderBody {
                provide: Arc::clone(&provide),
                registry: registry.clone(),
                at: Mutex::new(request.at.clone()),
                config: Mutex::new(config),
                _service: std::marker::PhantomData,
            });
            let restate = |body: &ProviderBody<C, S, F>, config: C| {
                *body
                    .config
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner()) = config;
                Ok(())
            };
            Ok(spawned(&fibers, body, request, restate))
        }),
    }
}

/// The dependency declaration of `P`, gathered with its panic contained: a
/// throwing declaration must fail its own spawn, nothing else (R11).
fn declared<P: PluginContract>() -> Vec<ServiceType> {
    std::panic::catch_unwind(P::Dependencies::declare).unwrap_or_default()
}

fn config_of<C: Clone + 'static>(config: &(dyn Any + Send + Sync)) -> Result<C, KernelError> {
    config.downcast_ref::<C>().cloned().ok_or_else(|| {
        error(
            ErrorCode::InvalidProfile,
            "the entry's config payload is not this lane's config type",
        )
    })
}

/// Spawns `body` gated on the loader's signal, records it in the shared fiber
/// map, and wraps it as the loader's handle.
fn spawned<B, C, R>(
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
    let fiber = Fiber::spawn(Arc::clone(&body) as Arc<dyn FiberBody>, request.signal);
    let entry = Arc::new(FiberEntry {
        fiber,
        body: Arc::clone(&body) as Arc<dyn Any + Send + Sync>,
    });
    crate::lock(fibers).insert(entry.fiber.id(), Arc::clone(&entry));
    Arc::new(LaneHandle {
        entry,
        body,
        restate,
        _config: std::marker::PhantomData::<fn(C)>,
    })
}

/// What a lane body must support so the loader can rebind it.
pub(crate) trait Rebind: Send + Sync + 'static {
    fn rebind(&self, at: Context<()>);
}

impl<P: PluginContract> Rebind for FacadeBody<P> {
    fn rebind(&self, at: Context<()>) {
        FacadeBody::rebind(self, at);
    }
}

/// A provider entry's body: provides on activation, withdraws on unload (R5).
struct ProviderBody<C, S: ServiceContract, F> {
    provide: Arc<F>,
    registry: Registry,
    at: Mutex<Context<()>>,
    config: Mutex<C>,
    _service: std::marker::PhantomData<fn() -> S>,
}

impl<C, S, F> Rebind for ProviderBody<C, S, F>
where
    C: Clone + std::fmt::Debug + Send + Sync + 'static,
    S: ServiceContract,
    F: Fn(C) -> Result<Arc<S>, KernelError> + Send + Sync + 'static,
{
    fn rebind(&self, at: Context<()>) {
        *self.at.lock().unwrap_or_else(|poison| poison.into_inner()) = at;
    }
}

impl<C, S, F> FiberBody for ProviderBody<C, S, F>
where
    C: Clone + std::fmt::Debug + Send + Sync + 'static,
    S: ServiceContract,
    F: Fn(C) -> Result<Arc<S>, KernelError> + Send + Sync + 'static,
{
    fn activate<'a>(&'a self, mut setup: Setup<'a>) -> KernelFuture<'a, ()> {
        let fiber = setup.fiber();
        Box::pin(async move {
            let config = self
                .config
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone();
            let at = self
                .at
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone();
            let value = (self.provide)(config)?;
            let tree = at.tree();
            // The provision realm is whatever the entry's context resolves the
            // service in; the registry anchors named realms globally (LAW §3).
            let realm = tree
                .realm_value(at.realm_of(tree.key_of::<S>().name()))
                .unwrap_or(jinnd_api::Realm::Root);
            let provision = self.registry.provide::<S, ()>(
                &tree.root(),
                &realm,
                fiber,
                value,
                &self.registry.vitality(true),
            );
            setup.effect(format!("provide {}", S::NAME), provision.undo)?;
            Ok(())
        })
    }
}

/// The loader's handle over one lane-spawned fiber.
struct LaneHandle<B, C, R> {
    entry: Arc<FiberEntry>,
    body: Arc<B>,
    restate: R,
    _config: std::marker::PhantomData<fn(C)>,
}

impl<B, C, R> EntryHandle for LaneHandle<B, C, R>
where
    B: Rebind,
    C: Clone + 'static,
    R: Fn(&B, C) -> Result<(), KernelError> + Send + Sync + 'static,
{
    fn id(&self) -> FiberId {
        self.entry.fiber.id()
    }

    fn state(&self) -> FiberState {
        self.entry.fiber.state()
    }

    fn restart(&self, cause: TransitionCause) {
        self.entry.fiber.restart(cause);
    }

    fn restate(&self, config: &(dyn Any + Send + Sync)) -> Result<(), KernelError> {
        (self.restate)(&self.body, config_of::<C>(config)?)
    }

    fn rebind(&self, at: Context<()>) {
        self.body.rebind(at);
    }

    fn dispose(&self) -> KernelFuture<'static, ()> {
        let entry = Arc::clone(&self.entry);
        Box::pin(async move {
            entry.fiber.dispose().await;
            Ok(())
        })
    }

    fn quiesce(&self) -> KernelFuture<'static, ()> {
        let entry = Arc::clone(&self.entry);
        Box::pin(async move {
            entry.fiber.quiesce().await;
            Ok(())
        })
    }
}
