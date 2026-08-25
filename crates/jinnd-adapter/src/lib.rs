//! Implementer-owned wiring between the stable facade and kernel subsystem crates.
//!
//! **This is the conformance-harness lane** (SOURCE-OF-TRUTH decision log,
//! 2026-08-25): the in-proc, statically-typed [`Kernel`] exists so the
//! verifier-owned invariant suite can drive kernel semantics. It is never a
//! plugin host and never ships in the daemon binary (Law 1). Wired as of M1-P4:
//! context, effects, fiber, and registry. Events and the profile loader keep
//! their `NO_KERNEL` stubs until their packets land.
//!
//! Harness conventions, stated once:
//!
//! * [`KERNEL_SCOPE`] (`FiberId(0)`) is the pseudo-fiber that owns facade-level
//!   provisions and effects; real fiber uids start at 1 and are never reused.
//! * An unknown fiber id reads as `Disposed` — a fiber this kernel never spawned
//!   is not live, and uids are never reused (R3) — with an empty history.
//! * A context id this kernel did not mint is refused with `InactiveContext`
//!   wherever the facade can express an error.

#![forbid(unsafe_code)]

mod body;

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex, MutexGuard};

use jinnd_api::{
    ContextId, EffectDescriptor, EffectId, ErrorCode, Event, EventListener, FiberId, FiberState,
    Inject, IsolationBinding, Kernel, KernelError, KernelFuture, PluginContract, Profile, Realm,
    ReconcileReport, ServiceContract, ServiceHandle, Transition, TransitionCause, Undo,
};
use jinnd_context::{Context, ContextTree};
use jinnd_effects::{Disposer, EffectScope};
use jinnd_fiber::Fiber;
use jinnd_registry::{Injection, Registry, Vitality};

use crate::body::FacadeBody;

/// The pseudo-fiber facade-level provisions and effects are charged to.
pub const KERNEL_SCOPE: FiberId = FiberId(0);

/// One spawned fiber and the body whose config the facade may re-state.
struct FiberEntry {
    fiber: Fiber,
    body: Arc<dyn std::any::Any + Send + Sync>,
}

struct Adapter {
    root: Context<()>,
    contexts: Mutex<HashMap<ContextId, Context<()>>>,
    fibers: Mutex<HashMap<FiberId, Arc<FiberEntry>>>,
    registry: Registry,
    kernel_scope: Mutex<EffectScope>,
    /// The kernel pseudo-fiber's vitality: always Active, never reported away.
    kernel_vitality: Vitality,
}

/// Returns the facade kernel used by verifier-owned invariant tests.
///
/// Implementation packets replace subsystem stubs here without changing the tests.
pub fn kernel() -> impl Kernel {
    let tree: ContextTree = ContextTree::new();
    let root = tree.root();
    let contexts = Mutex::new(HashMap::from([(root.id(), root.clone())]));
    let registry = Registry::new();
    let kernel_vitality = registry.vitality(true);
    Adapter {
        root,
        contexts,
        fibers: Mutex::new(HashMap::new()),
        registry,
        kernel_scope: Mutex::new(EffectScope::new()),
        kernel_vitality,
    }
}

impl Adapter {
    fn context(&self, id: ContextId) -> Result<Context<()>, KernelError> {
        lock(&self.contexts).get(&id).cloned().ok_or_else(|| {
            error(
                ErrorCode::InactiveContext,
                "this kernel minted no such context",
            )
        })
    }

    fn entry(&self, id: FiberId) -> Result<Arc<FiberEntry>, KernelError> {
        lock(&self.fibers).get(&id).map(Arc::clone).ok_or_else(|| {
            error(
                ErrorCode::MissingDependency,
                "this kernel spawned no such fiber",
            )
        })
    }
}

impl Kernel for Adapter {
    fn root_context(&self) -> ContextId {
        self.root.id()
    }

    fn derive_context(&self, parent: ContextId, isolation: Vec<IsolationBinding>) -> ContextId {
        match self.context(parent) {
            Ok(parent) => {
                let child = parent.derive().bind_all(&isolation).build();
                lock(&self.contexts).insert(child.id(), child.clone());
                child.id()
            }
            // The facade cannot answer with an error here; the dead id it gets
            // is refused (`InactiveContext`) wherever it is used.
            Err(_) => self.root.derive().build().id(),
        }
    }

    fn spawn<P: PluginContract>(
        &self,
        context: ContextId,
        plugin: P,
        config: P::Config,
    ) -> KernelFuture<'_, FiberId> {
        Box::pin(async move {
            let at = self.context(context)?;
            // Reactive availability (R1): the fiber activates only when every
            // declared service has an Active, checked provider, and any provider
            // change moves the epoch and forces a clean reload (R9).
            let readiness = self.registry.readiness(
                &at,
                Injection {
                    services: P::Dependencies::declare(),
                },
            );
            let body = Arc::new(FacadeBody::new(
                plugin,
                context,
                at,
                self.registry.clone(),
                config,
            ));
            let fiber = Fiber::spawn(
                Arc::clone(&body) as Arc<dyn jinnd_fiber::FiberBody>,
                readiness,
            );
            let id = fiber.id();
            let entry = Arc::new(FiberEntry { fiber, body });
            lock(&self.fibers).insert(id, Arc::clone(&entry));
            entry.fiber.quiesce().await;
            Ok(id)
        })
    }

    fn update<P: PluginContract>(&self, fiber: FiberId, config: P::Config) -> KernelFuture<'_, ()> {
        Box::pin(async move {
            let entry = self.entry(fiber)?;
            let Some(body) = entry.body.downcast_ref::<FacadeBody<P>>() else {
                return Err(error(
                    ErrorCode::PluginFailed,
                    "this fiber does not host the given plugin contract",
                ));
            };
            body.state_config(config);
            entry.fiber.restart(TransitionCause::ConfigChanged);
            entry.fiber.quiesce().await;
            Ok(())
        })
    }

    fn restart(&self, fiber: FiberId) -> KernelFuture<'_, ()> {
        Box::pin(async move {
            let entry = self.entry(fiber)?;
            entry.fiber.restart(TransitionCause::ExplicitRestart);
            entry.fiber.quiesce().await;
            Ok(())
        })
    }

    fn dispose(&self, fiber: FiberId) -> KernelFuture<'_, ()> {
        Box::pin(async move {
            let entry = self.entry(fiber)?;
            entry.fiber.dispose().await;
            Ok(())
        })
    }

    fn state(&self, fiber: FiberId) -> FiberState {
        lock(&self.fibers)
            .get(&fiber)
            .map_or(FiberState::Disposed, |entry| entry.fiber.state())
    }

    fn transitions(&self, fiber: FiberId) -> Vec<Transition> {
        lock(&self.fibers)
            .get(&fiber)
            .map(|entry| entry.fiber.record().transitions)
            .unwrap_or_default()
    }

    fn wait_for_quiescence(&self) -> KernelFuture<'_, ()> {
        Box::pin(async move {
            let entries: Vec<Arc<FiberEntry>> = lock(&self.fibers).values().cloned().collect();
            for entry in entries {
                entry.fiber.quiesce().await;
            }
            Ok(())
        })
    }

    fn provide<S: ServiceContract>(
        &self,
        context: ContextId,
        realm: Realm,
        value: Arc<S>,
    ) -> KernelFuture<'_, EffectId> {
        Box::pin(async move {
            let at = self.context(context)?;
            let provision = self.registry.provide::<S, ()>(
                &at,
                &realm,
                KERNEL_SCOPE,
                value,
                &self.kernel_vitality,
            );
            lock(&self.kernel_scope).register(format!("provide {}", S::NAME), provision.undo)
        })
    }

    fn resolve<S: ServiceContract>(
        &self,
        context: ContextId,
    ) -> Result<ServiceHandle<S>, KernelError> {
        self.registry.resolve::<S, ()>(&self.context(context)?)
    }

    fn register_effect(
        &self,
        context: ContextId,
        label: String,
        undo: Box<dyn Undo>,
    ) -> Result<EffectId, KernelError> {
        self.context(context)?;
        lock(&self.kernel_scope).register(label, Disposer::Whole(undo))
    }

    fn effect_tree(&self, fiber: FiberId) -> Vec<EffectDescriptor> {
        if fiber == KERNEL_SCOPE {
            return lock(&self.kernel_scope).tree();
        }
        lock(&self.fibers)
            .get(&fiber)
            .map(|entry| entry.fiber.effects())
            .unwrap_or_default()
    }

    fn listen<E: Event, L: EventListener<E>>(
        &self,
        _context: ContextId,
        _listener: L,
    ) -> Result<EffectId, KernelError> {
        todo!("NO_KERNEL: events")
    }

    fn dispatch<E: Event>(
        &self,
        _context: ContextId,
        _event: E,
    ) -> KernelFuture<'_, Vec<E::Output>> {
        todo!("NO_KERNEL: events")
    }

    fn reconcile<C: Clone + Debug + Send + Sync + 'static>(
        &self,
        _profile: Profile<C>,
    ) -> KernelFuture<'_, ReconcileReport> {
        todo!("NO_KERNEL: loader")
    }
}

/// Lock helper recovering from poisoning (R11): the maps and the kernel scope
/// hold valid data whatever thread panicked while touching them. No guard taken
/// here is ever held across an `await` or a call into plugin code (R1).
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

fn error(code: ErrorCode, message: &str) -> KernelError {
    KernelError {
        code,
        message: message.to_owned(),
        fiber: None,
    }
}
