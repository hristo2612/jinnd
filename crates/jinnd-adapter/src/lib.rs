//! Implementer-owned wiring between the stable facade and kernel subsystem crates.

#![forbid(unsafe_code)]

use std::fmt::Debug;
use std::sync::Arc;

use jinnd_api::{
    ContextId, EffectDescriptor, EffectId, Event, EventListener, FiberId, FiberState, Kernel,
    KernelError, KernelFuture, PluginContract, Profile, Realm, ReconcileReport, ServiceContract,
    ServiceHandle, Transition, Undo,
};

#[derive(Debug)]
struct Adapter;

/// Returns the facade kernel used by verifier-owned invariant tests.
///
/// Implementation packets replace subsystem stubs here without changing the tests.
pub fn kernel() -> impl Kernel {
    Adapter
}

impl Kernel for Adapter {
    fn root_context(&self) -> ContextId {
        todo!("NO_KERNEL: context")
    }

    fn derive_context(
        &self,
        _parent: ContextId,
        _isolation: Vec<jinnd_api::IsolationBinding>,
    ) -> ContextId {
        todo!("NO_KERNEL: context")
    }

    fn spawn<P: PluginContract>(
        &self,
        _context: ContextId,
        _plugin: P,
        _config: P::Config,
    ) -> KernelFuture<'_, FiberId> {
        todo!("NO_KERNEL: fiber")
    }

    fn update<P: PluginContract>(
        &self,
        _fiber: FiberId,
        _config: P::Config,
    ) -> KernelFuture<'_, ()> {
        todo!("NO_KERNEL: fiber")
    }

    fn restart(&self, _fiber: FiberId) -> KernelFuture<'_, ()> {
        todo!("NO_KERNEL: fiber")
    }

    fn dispose(&self, _fiber: FiberId) -> KernelFuture<'_, ()> {
        todo!("NO_KERNEL: fiber")
    }

    fn state(&self, _fiber: FiberId) -> FiberState {
        todo!("NO_KERNEL: fiber")
    }

    fn transitions(&self, _fiber: FiberId) -> Vec<Transition> {
        todo!("NO_KERNEL: fiber")
    }

    fn wait_for_quiescence(&self) -> KernelFuture<'_, ()> {
        todo!("NO_KERNEL: fiber")
    }

    fn provide<S: ServiceContract>(
        &self,
        _context: ContextId,
        _realm: Realm,
        _value: Arc<S>,
    ) -> KernelFuture<'_, EffectId> {
        todo!("NO_KERNEL: services")
    }

    fn resolve<S: ServiceContract>(
        &self,
        _context: ContextId,
    ) -> Result<ServiceHandle<S>, KernelError> {
        todo!("NO_KERNEL: services")
    }

    fn register_effect(
        &self,
        _context: ContextId,
        _label: String,
        _undo: Box<dyn Undo>,
    ) -> Result<EffectId, KernelError> {
        todo!("NO_KERNEL: effects")
    }

    fn effect_tree(&self, _fiber: FiberId) -> Vec<EffectDescriptor> {
        todo!("NO_KERNEL: effects")
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
