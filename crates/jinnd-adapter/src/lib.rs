//! Implementer-owned wiring between the stable facade and kernel subsystem crates.
//!
//! **This is the conformance-harness lane** (SOURCE-OF-TRUTH decision log,
//! 2026-08-25): the in-proc, statically-typed [`Kernel`] exists so the
//! verifier-owned invariant suite can drive kernel semantics. It is never a
//! plugin host and never ships in the daemon binary (Law 1). Wired as of M1-P5:
//! context, effects, fiber, registry, and events. The profile loader keeps its
//! `NO_KERNEL` stub until its packet lands.
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
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex, MutexGuard};

use jinnd_api::{
    ContextId, DispatchReport, EffectDescriptor, EffectId, ErrorCode, Event, EventListener,
    FiberId, FiberState, Inject, IsolationBinding, Kernel, KernelError, KernelFuture,
    PluginContract, Profile, Realm, ReconcileReport, ServiceContract, ServiceHandle, Transition,
    TransitionCause, Undo,
};
use jinnd_context::{Context, ContextTree};
use jinnd_effects::{Disposer, EffectScope};
use jinnd_events::{EventBus, Registration};
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
    events: EventBus,
    /// Removal handles for live listener effects, so `unlisten` can withdraw
    /// one registration by its effect id; removal stays idempotent with the
    /// same undo held by the kernel scope.
    listeners: Mutex<HashMap<EffectId, Registration>>,
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
        events: EventBus::new(),
        listeners: Mutex::new(HashMap::new()),
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

    /// Registers a listener as an effect on the kernel scope (R5): the bus
    /// registration is the forward action, its idempotent removal is the undo.
    fn register_listener<E: Event, L: EventListener<E>>(
        &self,
        context: ContextId,
        listener: L,
        once: bool,
    ) -> Result<EffectId, KernelError> {
        self.context(context)?;
        let registration = self.events.listen(context, listener, once);
        let undo = registration.clone();
        let registered = lock(&self.kernel_scope).register(
            format!("listen {}", std::any::type_name::<E>()),
            Disposer::sync(move || {
                undo.remove();
                Ok(())
            }),
        );
        match registered {
            Ok(effect) => {
                lock(&self.listeners).insert(effect, registration);
                Ok(effect)
            }
            // A registration whose undo cannot be held is not allowed to
            // outlive this call (R5): withdraw it before reporting. The
            // removal can drop the final listener handle, whose destructor is
            // plugin code — contained, like everywhere else (R11).
            Err(error) => {
                let _ = panic::catch_unwind(AssertUnwindSafe(|| registration.remove()));
                Err(error)
            }
        }
    }

    /// Validates the caller and runs one full mode walk on the bus.
    async fn report<E: Event>(
        &self,
        context: ContextId,
        event: E,
    ) -> Result<DispatchReport<E>, KernelError> {
        self.context(context)?;
        Ok(self.events.dispatch(context, event).await)
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
            // The declaration is plugin-owned code running before any fiber
            // exists: its panic is contained right here and answered as this
            // plugin's failure, charged to no live fiber (R11).
            let services = panic::catch_unwind(AssertUnwindSafe(P::Dependencies::declare))
                .map_err(|_| {
                    error(
                        ErrorCode::PluginFailed,
                        "the dependency declaration panicked",
                    )
                })?;
            // Reactive availability (R1): the fiber activates only when every
            // declared service has an Active, checked provider, and any provider
            // change moves the epoch and forces a clean reload (R9).
            let readiness = self.registry.readiness(&at, Injection { services });
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
        context: ContextId,
        listener: L,
    ) -> Result<EffectId, KernelError> {
        self.register_listener(context, listener, false)
    }

    fn listen_once<E: Event, L: EventListener<E>>(
        &self,
        context: ContextId,
        listener: L,
    ) -> Result<EffectId, KernelError> {
        self.register_listener(context, listener, true)
    }

    fn unlisten(&self, effect: EffectId) -> Result<(), KernelError> {
        // Idempotent: an unknown, already-withdrawn, or non-listener effect id
        // is a no-op. A live record is withdrawn for real (R5): its inverse
        // runs and the record leaves the tree, exactly as a replay would do it.
        if lock(&self.listeners).remove(&effect).is_none() {
            return Ok(());
        }
        let detached = lock(&self.kernel_scope).detach(effect);
        if let Some(detached) = detached {
            // Driven with every lock released: the inverse can reach a final
            // listener handle's plugin-authored destructor (R1); the
            // withdrawal machinery contains whatever it does (R11).
            detached.withdraw_now();
        }
        Ok(())
    }

    fn dispatch<E: Event>(&self, context: ContextId, event: E) -> KernelFuture<'_, Vec<E::Output>> {
        Box::pin(async move {
            let report = self.report(context, event).await?;
            // Every listener has settled by now: a failure is reported after
            // the walk, never by aborting it (R9). The aggregate stays
            // observable through `dispatch_report`.
            match report.failures.into_iter().next() {
                None => Ok(report.outputs),
                Some(failure) => Err(failure),
            }
        })
    }

    fn dispatch_report<E: Event>(
        &self,
        context: ContextId,
        event: E,
    ) -> KernelFuture<'_, DispatchReport<E>> {
        Box::pin(async move { self.report(context, event).await })
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
