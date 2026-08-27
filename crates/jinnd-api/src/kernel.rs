//! The conformance-harness kernel surface (pre-work extraction, M1-P8).

use crate::*;
use std::fmt::Debug;
use std::sync::Arc;

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
