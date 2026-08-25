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
///
/// The three provided methods are the payload's side of dispatch, all defaulted
/// so an event declares only what its mode uses (authorized M1-P5 additive
/// delta; R3, R12).
pub trait Event: Clone + Debug + Send + Sync + 'static {
    type Output: Debug + Send + Sync + 'static;

    const MODE: DispatchMode;

    /// Inverted routing (LAW §3): the payload selects its listeners by
    /// interrogating each listener's registration context. Listeners never
    /// filter the payload. Default: every listener is selected.
    fn selects(&self, listener: ContextId) -> bool {
        let _ = listener;
        true
    }

    /// Bail dispatch: whether a resolved output is decisive. The kernel awaits
    /// every listener result and asks only then — a pending async result is
    /// never treated as bailed (R9). Default: every resolved output is decisive.
    fn decisive(&self, output: &Self::Output) -> bool {
        let _ = output;
        true
    }

    /// Waterfall dispatch: fold one listener's output into the payload before
    /// the next listener sees it. Returns whether the walk continues; `false`
    /// declines the rest of the chain. Default: drop the output, continue.
    fn absorb(&mut self, output: Self::Output) -> bool {
        let _ = output;
        true
    }
}

/// Every listener outcome of one dispatch, per the event's declared mode.
///
/// R9 mechanically: a failing listener never aborts a collecting walk; its
/// contained failure is observed here, after every listener settled.
#[derive(Debug)]
pub struct DispatchReport<E: Event> {
    /// The payload after the walk. Waterfall reads its accumulator from here.
    pub event: E,
    /// Resolved outputs in registration order. Emit ignores outputs; bail
    /// carries the decisive output alone; waterfall folds outputs into `event`.
    pub outputs: Vec<E::Output>,
    /// Contained listener failures, in the order they were observed.
    pub failures: Vec<KernelError>,
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

/// The reserved package naming a pure grouping entry: it spawns no fiber and
/// exists to carry children, disablement, and isolation directives (authorized
/// M1-P6 additive delta; LAW §3 "Profiles & loader").
pub const GROUP_PACKAGE: &str = "jinn.profile/group";

/// One contained per-entry failure of a reconciliation (R11: good entries
/// load, bad entries surface recorded errors; authorized M1-P6 additive delta).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntryFault {
    pub entry: EntryId,
    pub error: KernelError,
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
    /// Contained per-entry faults (R11); never a whole-reconcile failure
    /// (authorized M1-P6 additive delta).
    pub errors: Vec<EntryFault>,
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

    /// Registers a listener delivered at most once, then withdrawn by the
    /// kernel itself (authorized M1-P5 additive delta).
    fn listen_once<E: Event, L: EventListener<E>>(
        &self,
        context: ContextId,
        listener: L,
    ) -> Result<EffectId, KernelError>;

    /// Withdraws one listener registration by the effect `listen` returned.
    /// Idempotent: withdrawing a listener that is already gone is a no-op
    /// (authorized M1-P5 additive delta).
    fn unlisten(&self, effect: EffectId) -> Result<(), KernelError>;

    fn dispatch<E: Event>(&self, context: ContextId, event: E) -> KernelFuture<'_, Vec<E::Output>>;

    /// Dispatches with per-listener failure observation: every listener
    /// settles and every contained failure is reported, whatever the mode
    /// (R9; authorized M1-P5 additive delta).
    fn dispatch_report<E: Event>(
        &self,
        context: ContextId,
        event: E,
    ) -> KernelFuture<'_, DispatchReport<E>>;

    /// Reconciles the runtime onto `profile` by entry id: only affected fibers
    /// move, and the profile becomes the committed document of record. The
    /// `PartialEq` bound is what reconcile-by-id compares configs with
    /// (authorized M1-P6 delta; R3).
    fn reconcile<C: Clone + Debug + PartialEq + Send + Sync + 'static>(
        &self,
        profile: Profile<C>,
    ) -> KernelFuture<'_, ReconcileReport>;

    /// Registers the constructor for profile entries referencing `package`
    /// (R3's string-keyed lane for dynamically loaded plugins). `build` maps an
    /// entry's config payload to the plugin instance and its typed config.
    ///
    /// The registration is an effect on the kernel scope (R5): withdrawing the
    /// returned effect unregisters the package. Registering a package twice is
    /// refused — replacement is never silent (R9). (Authorized M1-P6 additive
    /// delta.)
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] on duplicate registration.
    fn register_package<C, P, F>(&self, package: &str, build: F) -> Result<EffectId, KernelError>
    where
        C: Clone + Debug + PartialEq + Send + Sync + 'static,
        P: PluginContract,
        F: Fn(C) -> Result<(P, P::Config), KernelError> + Send + Sync + 'static;

    /// Registers a provider package: each activation of such an entry provides
    /// `S` — built from the entry's config by `provide` — in the realm the
    /// entry's context resolves `S` in, charged to the entry's fiber and
    /// withdrawn with it (R5, I2). (Authorized M1-P6 additive delta.)
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] on duplicate registration.
    fn register_provider_package<C, S, F>(
        &self,
        package: &str,
        provide: F,
    ) -> Result<EffectId, KernelError>
    where
        C: Clone + Debug + PartialEq + Send + Sync + 'static,
        S: ServiceContract,
        F: Fn(C) -> Result<Arc<S>, KernelError> + Send + Sync + 'static;

    /// The fiber currently hosting `entry`, if any (authorized M1-P6 additive
    /// delta: entry-to-fiber observation).
    fn entry_fiber(&self, entry: &EntryId) -> Option<FiberId>;

    /// A runtime-originated config change: writes back to the committed
    /// document atomically, then reloads the entry's fiber observing the new
    /// config (LAW §3 bidirectional persistence; authorized M1-P6 additive
    /// delta).
    fn update_entry<C: Clone + Debug + PartialEq + Send + Sync + 'static>(
        &self,
        entry: &EntryId,
        config: C,
    ) -> KernelFuture<'_, ()>;

    /// A runtime-originated disposal: persists the entry as disabled with its
    /// config retained, then disposes its fiber (authorized M1-P6 additive
    /// delta).
    fn dispose_entry<C: Clone + Debug + PartialEq + Send + Sync + 'static>(
        &self,
        entry: &EntryId,
    ) -> KernelFuture<'_, ()>;

    /// The committed document as persisted, `None` before the first reconcile
    /// or under a foreign config type (authorized M1-P6 additive delta:
    /// persistence read-back).
    fn persisted_profile<C: Clone + Debug + PartialEq + Send + Sync + 'static>(
        &self,
    ) -> Option<Profile<C>>;
}
