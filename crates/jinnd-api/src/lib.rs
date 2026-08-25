//! Types-only contract between the M1 invariant suite and the future kernel.
//!
//! This crate deliberately contains no runtime, storage, scheduling, or test double.
//! Kernel packets implement these traits without changing verifier-owned tests.

#![forbid(unsafe_code)]

mod inject;

pub use inject::{Inject, ServiceResolver, ServiceType};

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// A sendable future returned by an asynchronous kernel contract.
pub type KernelFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, KernelError>> + Send + 'a>>;

/// Stable identity of a context in one kernel process.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContextId(pub u64);

/// Stable identity of a fiber while it is live.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FiberId(pub u64);

/// Stable identity of a reversible effect.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EffectId(pub u64);

/// Stable identity of a profile entry across reconciliations.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EntryId(pub String);

/// Provider generation. Values are monotonic and never reused for one service slot.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Generation(pub u64);

/// Realm identity used to isolate typed service slots.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Realm {
    Root,
    Local(EntryId),
    Shared(String),
}

/// Observable lifecycle state of one fiber.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FiberState {
    Pending,
    Loading,
    Active,
    Failed,
    Unloading,
    Disposed,
}

/// Why a fiber's desired activation changed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionCause {
    InitialLoad,
    DependencyChanged,
    ConfigChanged,
    ExplicitRestart,
    ExplicitDispose,
    ParentDisposed,
}

/// One committed transition recorded for observation and the ledger.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Transition {
    pub fiber: FiberId,
    pub from: FiberState,
    pub to: FiberState,
    pub cause: TransitionCause,
}

/// Stable error classes exposed by the kernel boundary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ErrorCode {
    InactiveContext,
    MissingDependency,
    DependencyCycle,
    PluginFailed,
    EffectFailed,
    ListenerFailed,
    InvalidProfile,
}

/// Structured error value. Plugin panics are converted before crossing this boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KernelError {
    pub code: ErrorCode,
    pub message: String,
    pub fiber: Option<FiberId>,
}

/// A typed service contract with its own observational-equivalence witness.
pub trait ServiceContract: Send + Sync + 'static {
    type Observation: Debug + PartialEq + Send + Sync + 'static;

    const NAME: &'static str;

    fn observe(&self) -> Self::Observation;
}

/// A resolved service paired with caller scope and provider generation (R4).
#[derive(Debug)]
pub struct ServiceHandle<S: ServiceContract> {
    pub service: Arc<S>,
    pub caller: ContextId,
    pub provider: FiberId,
    pub generation: Generation,
    pub realm: Realm,
}

/// One dependency generation captured for a single activation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DependencySnapshot {
    pub service: ServiceType,
    pub provider: FiberId,
    pub generation: Generation,
    pub realm: Realm,
}

/// Full dependency epoch owned by one activation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Epoch {
    pub dependencies: Vec<DependencySnapshot>,
}

/// Public description of the live reversible-effect tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectDescriptor {
    pub id: EffectId,
    pub label: String,
    pub children: Vec<EffectDescriptor>,
}

/// An inverse registered at the same boundary as its forward effect.
pub trait Undo: Send + 'static {
    fn undo(self: Box<Self>) -> KernelFuture<'static, ()>;
}

/// Result of one plugin activation.
#[derive(Debug)]
pub struct ActivationReceipt {
    pub fiber: FiberId,
    pub epoch: Epoch,
}

/// Context and dependency snapshot handed to a plugin body once per activation.
#[derive(Debug)]
pub struct Activation<'a, D> {
    pub context: ContextId,
    pub fiber: FiberId,
    pub dependencies: &'a D,
}

/// Typed plugin contract. Implementations execute only behind a sandboxed host.
pub trait PluginContract: Send + Sync + 'static {
    type Config: Clone + Debug + Send + Sync + 'static;
    type Dependencies: Inject;

    const NAME: &'static str;

    fn activate<'a>(
        &'a self,
        activation: Activation<'a, Self::Dependencies>,
        config: Self::Config,
    ) -> KernelFuture<'a, ()>;
}

/// Dispatch semantics are part of an event's type-level contract.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum DispatchMode {
    Emit,
    Parallel,
    Serial,
    Bail,
    Waterfall,
}

/// A typed event and its declared dispatch mode.
pub trait Event: Clone + Debug + Send + Sync + 'static {
    type Output: Debug + Send + Sync + 'static;

    const MODE: DispatchMode;
}

/// One typed event listener.
pub trait EventListener<E: Event>: Send + Sync + 'static {
    fn call<'a>(&'a self, caller: ContextId, event: E) -> KernelFuture<'a, E::Output>;
}

/// A dynamic plugin reference at the profile boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginRef {
    pub package: String,
    pub version: String,
    pub artifact_hash: String,
}

/// Isolation mapping applied to one profile entry or group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IsolationBinding {
    pub service: String,
    pub realm: Realm,
}

/// Typed profile entry used by reconcile-by-id.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProfileEntry<C> {
    pub id: EntryId,
    pub plugin: PluginRef,
    pub config: C,
    pub disabled: bool,
    pub parent: Option<EntryId>,
    pub isolation: Vec<IsolationBinding>,
}

/// Ordered profile document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Profile<C> {
    pub entries: Vec<ProfileEntry<C>>,
}

/// Observable result of one profile reconciliation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconcileReport {
    pub created: Vec<EntryId>,
    pub restarted: Vec<EntryId>,
    pub disposed: Vec<EntryId>,
    pub unchanged: Vec<EntryId>,
}

/// Types-only surface the future kernel must satisfy for verifier-owned tests.
pub trait Kernel: Send + Sync + 'static {
    fn root_context(&self) -> ContextId;

    fn derive_context(&self, parent: ContextId, isolation: Vec<IsolationBinding>) -> ContextId;

    fn spawn<P: PluginContract>(
        &self,
        context: ContextId,
        plugin: P,
        config: P::Config,
    ) -> KernelFuture<'_, FiberId>;

    fn update<P: PluginContract>(&self, fiber: FiberId, config: P::Config) -> KernelFuture<'_, ()>;

    fn restart(&self, fiber: FiberId) -> KernelFuture<'_, ()>;

    fn dispose(&self, fiber: FiberId) -> KernelFuture<'_, ()>;

    fn state(&self, fiber: FiberId) -> FiberState;

    fn transitions(&self, fiber: FiberId) -> Vec<Transition>;

    fn wait_for_quiescence(&self) -> KernelFuture<'_, ()>;

    fn provide<S: ServiceContract>(
        &self,
        context: ContextId,
        realm: Realm,
        value: Arc<S>,
    ) -> KernelFuture<'_, EffectId>;

    fn resolve<S: ServiceContract>(
        &self,
        context: ContextId,
    ) -> Result<ServiceHandle<S>, KernelError>;

    fn register_effect(
        &self,
        context: ContextId,
        label: String,
        undo: Box<dyn Undo>,
    ) -> Result<EffectId, KernelError>;

    fn effect_tree(&self, fiber: FiberId) -> Vec<EffectDescriptor>;

    fn listen<E: Event, L: EventListener<E>>(
        &self,
        context: ContextId,
        listener: L,
    ) -> Result<EffectId, KernelError>;

    fn dispatch<E: Event>(&self, context: ContextId, event: E) -> KernelFuture<'_, Vec<E::Output>>;

    fn reconcile<C: Clone + Debug + Send + Sync + 'static>(
        &self,
        profile: Profile<C>,
    ) -> KernelFuture<'_, ReconcileReport>;
}
