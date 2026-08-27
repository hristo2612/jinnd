//! The [`Kernel`] trait impl: every facade entry point, delegating any body
//! of substance to `run.rs`/`support.rs` (hygiene split; semantics unchanged).

use std::fmt::Debug;
use std::sync::Arc;

use jinnd_api::{
    ContextId, DispatchReport, EffectDescriptor, EffectId, EntryId, ErrorCode, Event,
    EventListener, FiberId, FiberState, ForwardEffect, IsolationBinding, Kernel, KernelError,
    KernelFuture, LedgerQuery, LedgerRecord, PluginContract, Profile, Realm, ReconcileReport,
    RevertKey, RevertResolution, ServiceContract, ServiceHandle, Transition, Undo, Witness,
};

use crate::{Adapter, boundary, error, lock, providing, wiring};

impl Kernel for Adapter {
    fn root_context(&self) -> ContextId {
        self.root.id()
    }

    fn derive_context(&self, parent: ContextId, isolation: Vec<IsolationBinding>) -> ContextId {
        self.derive_at(parent, &isolation)
    }

    fn spawn<P: PluginContract>(
        &self,
        context: ContextId,
        plugin: P,
        config: P::Config,
    ) -> KernelFuture<'_, FiberId> {
        Box::pin(self.spawn_fiber(context, plugin, config))
    }

    fn update<P: PluginContract>(&self, fiber: FiberId, config: P::Config) -> KernelFuture<'_, ()> {
        Box::pin(self.restate_config::<P>(fiber, config))
    }

    fn restart(&self, fiber: FiberId) -> KernelFuture<'_, ()> {
        Box::pin(async move {
            let entry = self.entry(fiber)?;
            entry
                .fiber
                .restart(jinnd_api::TransitionCause::ExplicitRestart);
            entry.fiber.quiesce().await;
            self.sync_transitions(fiber);
            Ok(())
        })
    }

    fn dispose(&self, fiber: FiberId) -> KernelFuture<'_, ()> {
        Box::pin(async move {
            let entry = self.entry(fiber)?;
            entry.fiber.dispose().await;
            self.sync_transitions(fiber);
            Ok(())
        })
    }

    fn state(&self, fiber: FiberId) -> FiberState {
        lock(&self.fibers)
            .get(&fiber)
            .map_or(FiberState::Disposed, |entry| entry.fiber.state())
    }

    fn transitions(&self, fiber: FiberId) -> Vec<Transition> {
        self.fiber_transitions(fiber)
    }

    fn wait_for_quiescence(&self) -> KernelFuture<'_, ()> {
        Box::pin(self.settle_quiescence())
    }

    fn provide<S: ServiceContract>(
        &self,
        context: ContextId,
        realm: Realm,
        value: Arc<S>,
    ) -> KernelFuture<'_, EffectId> {
        Box::pin(self.provide_value(context, realm, value))
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
        self.register_undo(context, label, undo)
    }

    fn register_child_effect(
        &self,
        parent: EffectId,
        label: String,
        undo: Box<dyn Undo>,
    ) -> Result<EffectId, KernelError> {
        self.register_child_undo(parent, label, undo)
    }

    fn effect_tree(&self, fiber: FiberId) -> Vec<EffectDescriptor> {
        self.live_effects(fiber)
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
        self.withdraw_listener(effect)
    }

    fn dispatch<E: Event>(&self, context: ContextId, event: E) -> KernelFuture<'_, Vec<E::Output>> {
        Box::pin(self.dispatch_first_failure(context, event))
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
        profile: Profile<C>,
    ) -> KernelFuture<'_, ReconcileReport> {
        Box::pin(self.reconcile_runtime(profile))
    }

    fn register_package<C, P, F>(&self, package: &str, build: F) -> Result<EffectId, KernelError>
    where
        C: Clone + Debug + PartialEq + Send + Sync + 'static,
        P: PluginContract,
        F: Fn(C) -> Result<(P, P::Config), KernelError> + Send + Sync + 'static,
    {
        let lane = wiring::plugin_lane(Arc::clone(&self.fibers), self.registry.clone(), build)?;
        self.register_lane_effect::<C>(package, lane)
    }

    fn register_provider_package<C, S, F>(
        &self,
        package: &str,
        provide: F,
    ) -> Result<EffectId, KernelError>
    where
        C: Clone + Debug + PartialEq + Send + Sync + 'static,
        S: ServiceContract,
        F: Fn(C) -> Result<Arc<S>, KernelError> + Send + Sync + 'static,
    {
        let lane = wiring::provider_lane(Arc::clone(&self.fibers), self.registry.clone(), provide);
        self.register_lane_effect::<C>(package, lane)
    }

    fn entry_fiber(&self, entry: &EntryId) -> Option<FiberId> {
        self.loader.entry_fiber(entry)
    }

    fn update_entry<C: Clone + Debug + PartialEq + Send + Sync + 'static>(
        &self,
        entry: &EntryId,
        config: C,
    ) -> KernelFuture<'_, ()> {
        let entry = entry.clone();
        Box::pin(async move {
            let run = self.loader.update_entry(&entry, config);
            self.record_amended(&entry, "update", run).await
        })
    }

    fn dispose_entry<C: Clone + Debug + PartialEq + Send + Sync + 'static>(
        &self,
        entry: &EntryId,
    ) -> KernelFuture<'_, ()> {
        let entry = entry.clone();
        Box::pin(async move {
            let run = self.loader.dispose_entry::<C>(&entry);
            self.record_amended(&entry, "dispose", run).await
        })
    }

    fn persisted_profile<C: Clone + Debug + PartialEq + Send + Sync + 'static>(
        &self,
    ) -> Option<Profile<C>> {
        self.loader.persisted::<C>()
    }

    fn begin_effect(
        &self,
        context: ContextId,
        label: String,
        forward: ForwardEffect,
    ) -> Result<EffectId, KernelError> {
        self.context(context)?;
        boundary::begin(
            &self.kernel_scope,
            &self.pending,
            &self.ledger,
            label,
            forward,
        )
    }

    fn effect_outcome(&self, effect: EffectId) -> KernelFuture<'_, ()> {
        Box::pin(boundary::outcome(&self.pending, effect))
    }

    fn dispose_effect(&self, effect: EffectId) -> KernelFuture<'_, ()> {
        Box::pin(boundary::dispose(
            &self.kernel_scope,
            &self.pending,
            &self.ledger,
            effect,
        ))
    }

    fn ledger_events(&self, query: LedgerQuery) -> KernelFuture<'_, Vec<LedgerRecord>> {
        Box::pin(async move {
            self.ledger
                .events(query)
                .await
                .map_err(|failure| error(ErrorCode::EffectFailed, &failure.to_string()))
        })
    }

    fn revert_effect(
        &self,
        effect: EffectId,
        key: RevertKey,
        witness: Witness,
    ) -> KernelFuture<'_, RevertResolution> {
        Box::pin(self.run_revert(effect, key, witness))
    }

    fn revert_resolution(&self, effect: EffectId) -> Option<RevertResolution> {
        self.revert.resolution(effect)
    }

    fn compensate_effect(
        &self,
        effect: EffectId,
        key: RevertKey,
        compensator: Box<dyn Undo>,
        operator_confirmed: bool,
    ) -> KernelFuture<'_, RevertResolution> {
        Box::pin(self.run_compensation(effect, key, compensator, operator_confirmed))
    }

    fn register_providing_package<C, S, P, F>(
        &self,
        package: &str,
        build: F,
    ) -> Result<EffectId, KernelError>
    where
        C: Clone + Debug + PartialEq + Send + Sync + 'static,
        S: ServiceContract,
        P: PluginContract,
        F: Fn(C) -> Result<(P, P::Config, Arc<S>), KernelError> + Send + Sync + 'static,
    {
        let lane =
            providing::providing_lane(Arc::clone(&self.fibers), self.registry.clone(), build)?;
        self.register_lane_effect::<C>(package, lane)
    }

    fn attach_document<C>(
        &self,
        path: std::path::PathBuf,
        baseline: &str,
    ) -> Result<(), KernelError>
    where
        C: Clone + Debug + PartialEq + serde::Serialize + Send + Sync + 'static,
    {
        self.take_over_document::<C>(path, baseline)
    }

    fn document_text(&self) -> Option<String> {
        let path = lock(&self.document_path).clone()?;
        std::fs::read_to_string(path).ok()
    }
}
