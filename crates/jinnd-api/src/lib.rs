//! Types-only contract between the M1 invariant suite and the future kernel.
//!
//! This crate deliberately contains no runtime, storage, scheduling, or test double.
//! Kernel packets implement these traits without changing verifier-owned tests.

#![forbid(unsafe_code)]

mod forward;
mod inject;
mod ledger;

pub use forward::{EffectHost, ForwardAction, ForwardEffect};
pub use inject::{Inject, ServiceResolver, ServiceType};
pub use ledger::{
    LedgerEventKind, LedgerQuery, LedgerRecord, Receipt, RevertKey, RevertResolution, Witness,
};

use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// A sendable future returned by an asynchronous kernel contract.
pub type KernelFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, KernelError>> + Send + 'a>>;

/// Stable identity of a context in one kernel process.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContextId(pub u64);

/// Stable identity of a fiber while it is live.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FiberId(pub u64);

/// Stable identity of a reversible effect. Serde exists so ledger events can
/// carry the effect they concern (R3, R6; authorized M1-P7 additive delta).
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct EffectId(pub u64);

/// Stable identity of a profile entry across reconciliations.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum FiberState {
    Pending,
    Loading,
    Active,
    Failed,
    Unloading,
    Disposed,
}

/// Why a fiber's desired activation changed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum TransitionCause {
    InitialLoad,
    DependencyChanged,
    ConfigChanged,
    ExplicitRestart,
    ExplicitDispose,
    ParentDisposed,
}

/// One committed transition recorded for observation and the ledger.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Transition {
    pub fiber: FiberId,
    pub from: FiberState,
    pub to: FiberState,
    pub cause: TransitionCause,
}

/// Stable error classes exposed by the kernel boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum ErrorCode {
    InactiveContext,
    MissingDependency,
    DependencyCycle,
    PluginFailed,
    EffectFailed,
    ListenerFailed,
    InvalidProfile,
    /// A provision for an occupied (service, realm) slot from a different
    /// provider was refused: replacement is never silent (paper Def 23, R9).
    /// The same provider superseding its own generation — the hot-swap lane —
    /// is not a duplicate. (Authorized M1-P6c additive delta.)
    DuplicateProvision,
}

/// Structured error value. Plugin panics are converted before crossing this boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
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
pub struct Activation<'a, D> {
    pub context: ContextId,
    pub fiber: FiberId,
    pub dependencies: &'a D,
    /// Teardown-effect registrar charged to this activation's fiber
    /// (authorized M1-P7 additive delta: I2 teardown-time observation).
    pub effects: &'a dyn EffectHost,
}

impl<D: Debug> Debug for Activation<'_, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Activation")
            .field("context", &self.context)
            .field("fiber", &self.fiber)
            .field("dependencies", &self.dependencies)
            .finish_non_exhaustive()
    }
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
#[derive(Clone, Debug, Default, Eq, PartialEq)]
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

    /// Registers `undo` as a child of the live effect `parent`: the tree
    /// preserves the parent-child shape, and withdrawing the parent withdraws
    /// its children first, LIFO (R5; authorized M1-P7 additive delta per the
    /// nested-effect registration gap — the `cordis_dispose` "yield dispose"
    /// case's surface).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::EffectFailed`] when `parent` names no live effect.
    fn register_child_effect(
        &self,
        parent: EffectId,
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
    /// move, and the profile becomes the committed document of record. Entry
    /// configs are compared under the equality attestation their package
    /// registration captured (`C`'s own `PartialEq`, where the type is
    /// statically known); without one a change is assumed, never ignored (R9)
    /// — so this pre-existing surface keeps its exact bound (R12).
    fn reconcile<C: Clone + Debug + Send + Sync + 'static>(
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
    /// [`ErrorCode::PluginFailed`] when the plugin's dependency declaration
    /// panics: the declaration is plugin-owned code, its panic is contained at
    /// this boundary, and the package never registers (R11).
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

    /// A runtime-originated config change: the entry's fiber validates and
    /// stages the new config first, the committed document is then written
    /// back atomically, and only then does the fiber reload to observe it. A
    /// rejected or unpersistable change leaves both views at the prior state
    /// (LAW §3 bidirectional persistence; authorized M1-P6 additive delta).
    fn update_entry<C: Clone + Debug + PartialEq + Send + Sync + 'static>(
        &self,
        entry: &EntryId,
        config: C,
    ) -> KernelFuture<'_, ()>;

    /// A runtime-originated disposal: the entry's fiber is withdrawn first,
    /// then the document persists the entry as disabled, config retained. A
    /// refused disposal persists nothing (authorized M1-P6 additive delta).
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

    /// Begins one forward effect on `context`: the id is minted immediately,
    /// the actions run behind the boundary, and the inverse installs per the
    /// effect's atomicity contract — a plain effect all-or-none, a stepwise
    /// effect with the staleness guard at every yield boundary (paper Def
    /// 51/52 + Alg 1; authorized M1-P7 additive delta; R5).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InactiveContext`] for a context this kernel did not mint.
    fn begin_effect(
        &self,
        context: ContextId,
        label: String,
        forward: ForwardEffect,
    ) -> Result<EffectId, KernelError>;

    /// Resolves when `effect`'s forward walk settled: `Ok` when the effect is
    /// installed or was cleanly diverted, the original error when a forward
    /// action failed — in which case nothing was installed (authorized M1-P7
    /// additive delta). `Ok` immediately for an effect id not begun through
    /// [`Kernel::begin_effect`].
    fn effect_outcome(&self, effect: EffectId) -> KernelFuture<'_, ()>;

    /// Withdraws one live effect by id, with its whole subtree, running its
    /// inverse exactly once; an in-flight forward effect is diverted at its
    /// next yield boundary — the launched action lands first — and its
    /// yielded prefix rolls back. Idempotent: an unknown or already-withdrawn
    /// id is a no-op (authorized M1-P7 additive delta; R5).
    fn dispose_effect(&self, effect: EffectId) -> KernelFuture<'_, ()>;

    /// Reads the append-only ledger (R6, Law 2): every kernel-boundary event,
    /// in monotonic sequence order, filtered by `query` (authorized M1-P7
    /// additive delta: ledger read surface).
    fn ledger_events(&self, query: LedgerQuery) -> KernelFuture<'_, Vec<LedgerRecord>>;

    /// Reverts one settled effect under the keyed exactly-once protocol
    /// (constitution 03, Law 3): intent is durably recorded before the
    /// inverse runs, and `witness` is checked before completion is recorded.
    /// A same-key retry of a branch whose completion is recorded returns the
    /// recorded outcome without re-running the inverse; a same-key retry
    /// that finds a durable intent with no recorded completion — a reopen
    /// after a crash — resumes the branch and runs the inverse to completion
    /// under that key (exactly-once is durable at-least-once intent plus
    /// idempotent same-key completion). A witness failure leaves the branch
    /// `PendingRevert`, visibly (authorized M1-P7 additive delta).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::EffectFailed`] for an unknown effect, an in-flight
    /// forward effect, or a distinct key against an existing branch.
    fn revert_effect(
        &self,
        effect: EffectId,
        key: RevertKey,
        witness: Witness,
    ) -> KernelFuture<'_, RevertResolution>;

    /// The recorded resolution state of `effect`'s revert branch, if one
    /// exists (authorized M1-P7 additive delta: resolution observation).
    fn revert_resolution(&self, effect: EffectId) -> Option<RevertResolution>;

    /// Runs an operator-confirmed declared compensator against a
    /// `PendingRevert` branch. The branch resolves `Compensated`, never
    /// `Reverted`; unless the compensation satisfies the branch's original
    /// witness it stays marked unclean (constitution 03; authorized M1-P7
    /// additive delta).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::EffectFailed`] without operator confirmation, for an
    /// unknown branch, or for a branch not in `PendingRevert`.
    fn compensate_effect(
        &self,
        effect: EffectId,
        key: RevertKey,
        compensator: Box<dyn Undo>,
        operator_confirmed: bool,
    ) -> KernelFuture<'_, RevertResolution>;

    /// Registers a package lane that both injects `P`'s declared dependencies
    /// and provides `S` from each activation — the lane shape a dependency
    /// cycle is expressed through (I3; authorized M1-P7 additive delta per
    /// the invariant_progress IOU). `build` maps an entry's config to the
    /// plugin, its typed config, and the provided value.
    ///
    /// # Errors
    ///
    /// As [`Kernel::register_package`].
    fn register_providing_package<C, S, P, F>(
        &self,
        package: &str,
        build: F,
    ) -> Result<EffectId, KernelError>
    where
        C: Clone + Debug + PartialEq + Send + Sync + 'static,
        S: ServiceContract,
        P: PluginContract,
        F: Fn(C) -> Result<(P, P::Config, Arc<S>), KernelError> + Send + Sync + 'static;

    /// Attaches the raw persisted document: `baseline` is parsed as the
    /// document being taken over — its opaque raw entries and unknown fields
    /// survive every later write-back byte-for-byte — and `path` is where the
    /// document of record persists (LAW §3 bidirectional persistence;
    /// authorized M1-P7 additive delta per the loader_reconcile IOU).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] for an unparseable baseline.
    fn attach_document<C>(
        &self,
        path: std::path::PathBuf,
        baseline: &str,
    ) -> Result<(), KernelError>
    where
        C: Clone + Debug + PartialEq + serde::Serialize + Send + Sync + 'static;

    /// The persisted raw document text, exactly as last written back; `None`
    /// before any write-back or when no document is attached (authorized
    /// M1-P7 additive delta: raw persistence observation).
    fn document_text(&self) -> Option<String>;
}
