//! Generic host-side backing for package lanes: the fiber bodies and handles a
//! host wires lanes with. Nothing here is harness-specific — the daemon's
//! hosts back the same seam (R7: one contract, swappable backing; R10).

use std::any::Any;
use std::sync::{Arc, Mutex};

use jinnd_api::{
    ErrorCode, FiberId, FiberState, KernelError, KernelFuture, ServiceContract, TransitionCause,
};
use jinnd_context::Context;
use jinnd_fiber::{Fiber, FiberBody, Setup};
use jinnd_registry::Registry;

use crate::lanes::EntryHandle;
use crate::state::error;

/// What a lane body must support so the loader can rebind its context.
pub trait Rebind: Send + Sync + 'static {
    fn rebind(&self, at: Context<()>);
}

/// Downcasts one entry's config payload to the lane's config type.
///
/// # Errors
///
/// [`ErrorCode::InvalidProfile`] for a foreign payload type.
pub fn config_of<C: Clone + 'static>(config: &(dyn Any + Send + Sync)) -> Result<C, KernelError> {
    config.downcast_ref::<C>().cloned().ok_or_else(|| {
        error(
            ErrorCode::InvalidProfile,
            "the entry's config payload is not this lane's config type",
        )
    })
}

/// A provider entry's body: each activation provides `S` — built from the
/// entry's config — in the realm the entry's context resolves `S` in, charged
/// to the entry's fiber and withdrawn with its activation (R5, I2).
pub struct ProviderBody<C, S: ServiceContract, F> {
    provide: Arc<F>,
    registry: Registry,
    at: Mutex<Context<()>>,
    config: Mutex<C>,
    _service: std::marker::PhantomData<fn() -> S>,
}

impl<C, S, F> ProviderBody<C, S, F>
where
    C: Clone + std::fmt::Debug + Send + Sync + 'static,
    S: ServiceContract,
    F: Fn(C) -> Result<Arc<S>, KernelError> + Send + Sync + 'static,
{
    pub fn new(provide: Arc<F>, registry: Registry, at: Context<()>, config: C) -> Self {
        Self {
            provide,
            registry,
            at: Mutex::new(at),
            config: Mutex::new(config),
            _service: std::marker::PhantomData,
        }
    }

    /// States a new latest config for the next activation to provide from.
    pub fn state_config(&self, config: C) {
        *self
            .config
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = config;
    }
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
            )?;
            // A draining effect (I2): dependents are waited out before ANY of
            // this fiber's inverses replay.
            setup.draining_effect(
                format!("provide {}", S::NAME),
                provision.drain,
                provision.undo,
            )?;
            Ok(())
        })
    }
}

/// The loader's handle over one lane-spawned fiber.
pub struct LaneHandle<B, C, R> {
    fiber: Arc<Fiber>,
    body: Arc<B>,
    restate: R,
    _config: std::marker::PhantomData<fn(C)>,
}

impl<B, C, R> LaneHandle<B, C, R>
where
    B: Rebind,
    C: Clone + 'static,
    R: Fn(&B, C) -> Result<(), KernelError> + Send + Sync + 'static,
{
    pub fn new(fiber: Arc<Fiber>, body: Arc<B>, restate: R) -> Self {
        Self {
            fiber,
            body,
            restate,
            _config: std::marker::PhantomData,
        }
    }
}

impl<B, C, R> EntryHandle for LaneHandle<B, C, R>
where
    B: Rebind,
    C: Clone + 'static,
    R: Fn(&B, C) -> Result<(), KernelError> + Send + Sync + 'static,
{
    fn id(&self) -> FiberId {
        self.fiber.id()
    }

    fn state(&self) -> FiberState {
        self.fiber.state()
    }

    fn withdrawing(&self) -> bool {
        self.fiber.withdrawing()
    }

    fn restart(&self, cause: TransitionCause) {
        self.fiber.restart(cause);
    }

    fn restate(&self, config: &(dyn Any + Send + Sync)) -> Result<(), KernelError> {
        (self.restate)(&self.body, config_of::<C>(config)?)
    }

    fn rebind(&self, at: Context<()>) {
        self.body.rebind(at);
    }

    fn dispose(&self) -> KernelFuture<'static, ()> {
        let fiber = Arc::clone(&self.fiber);
        Box::pin(async move {
            fiber.dispose().await;
            Ok(())
        })
    }

    fn quiesce(&self) -> KernelFuture<'static, ()> {
        let fiber = Arc::clone(&self.fiber);
        Box::pin(async move {
            fiber.quiesce().await;
            Ok(())
        })
    }
}
