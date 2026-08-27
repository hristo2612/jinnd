//! The larger facade method bodies, one inherent method per trait method
//! (`facade.rs` holds the thin trait impl; splitting is hygiene only — the
//! semantics live here unchanged).

use std::fmt::Debug;
use std::sync::Arc;

use jinnd_api::{
    ContextId, EffectId, ErrorCode, FiberId, FiberState, KernelError, LedgerEventKind,
    PluginContract, Profile, Realm, ReconcileReport, RevertKey, RevertResolution, ServiceContract,
    Transition, TransitionCause, Undo, Witness,
};
use jinnd_effects::Disposer;
use jinnd_registry::Injection;

use crate::body::FacadeBody;
use crate::{Adapter, FiberEntry, KERNEL_SCOPE, boundary, error, lock};

impl Adapter {
    pub(crate) async fn spawn_fiber<P: PluginContract>(
        &self,
        context: ContextId,
        plugin: P,
        config: P::Config,
    ) -> Result<FiberId, KernelError> {
        let at = self.context(context)?;
        // The declaration runs before any fiber exists: its panic is this
        // plugin's failure, charged to no live fiber (R11; wiring::declared).
        let services = crate::wiring::declared::<P>()?;
        // Reactive availability (R1): the fiber activates only when every
        // declared service has an Active, checked provider, and any provider
        // change moves the epoch and forces a clean reload (R9).
        let readiness = self.registry.readiness(&at, Injection { services });
        let body = Arc::new(FacadeBody::new(plugin, at, self.registry.clone(), config));
        let entry = crate::wiring::track(&self.fibers, body, readiness);
        let id = entry.fiber.id();
        entry.fiber.quiesce().await;
        self.sync_transitions(id);
        Ok(id)
    }

    pub(crate) async fn restate_config<P: PluginContract>(
        &self,
        fiber: FiberId,
        config: P::Config,
    ) -> Result<(), KernelError> {
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
        self.sync_transitions(fiber);
        Ok(())
    }

    pub(crate) async fn settle_quiescence(&self) -> Result<(), KernelError> {
        // Quiesce passes until two consecutive passes observe the same
        // states: cross-fiber propagation (a provider activating waking a
        // consumer) settles between passes. Termination is I3's promise
        // for acyclic dependency precedence.
        let mut previous: Option<Vec<(FiberId, FiberState)>> = None;
        loop {
            let entries: Vec<Arc<FiberEntry>> = lock(&self.fibers).values().cloned().collect();
            for entry in &entries {
                entry.fiber.quiesce().await;
            }
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            let mut snapshot: Vec<(FiberId, FiberState)> = entries
                .iter()
                .map(|entry| (entry.fiber.id(), entry.fiber.state()))
                .collect();
            snapshot.sort_by_key(|(id, _)| *id);
            if previous.as_ref() == Some(&snapshot) {
                return Ok(());
            }
            previous = Some(snapshot);
        }
    }

    pub(crate) async fn provide_value<S: ServiceContract>(
        &self,
        context: ContextId,
        realm: Realm,
        value: Arc<S>,
    ) -> Result<EffectId, KernelError> {
        let at = self.context(context)?;
        let provision = self.registry.provide::<S, ()>(
            &at,
            &realm,
            KERNEL_SCOPE,
            value,
            &self.kernel_vitality,
        )?;
        // The kernel scope's provisions carry the same drain-then-undo
        // shape (I2); every facade provision shares the kernel
        // pseudo-fiber, so re-providing here is the hot-swap lane.
        let registered = lock(&self.kernel_scope).register_draining(
            format!("provide {}", S::NAME),
            provision.drain,
            provision.undo,
        )?;
        self.record_scope(LedgerEventKind::ServiceProvided {
            service: S::NAME.to_owned(),
        });
        Ok(registered)
    }

    /// Registers `undo` at the kernel scope's root, ledger-recorded (R5, R6).
    pub(crate) fn register_undo(
        &self,
        context: ContextId,
        label: String,
        undo: Box<dyn Undo>,
    ) -> Result<EffectId, KernelError> {
        self.context(context)?;
        let registered = lock(&self.kernel_scope).register(label.clone(), Disposer::Whole(undo))?;
        self.record_scope(LedgerEventKind::EffectRegistered { label });
        Ok(registered)
    }

    /// Registers `undo` nested under the live effect `parent` (the authorized
    /// nested-effect registration surface): the tree keeps the parent-child
    /// shape and the parent's withdrawal replays children first, LIFO.
    pub(crate) fn register_child_undo(
        &self,
        parent: EffectId,
        label: String,
        undo: Box<dyn Undo>,
    ) -> Result<EffectId, KernelError> {
        let registered = lock(&self.kernel_scope).register_child(
            parent,
            label.clone(),
            Disposer::Whole(undo),
        )?;
        self.record_scope(LedgerEventKind::EffectRegistered { label });
        Ok(registered)
    }

    /// Records one kernel-scope event on the ordered, unreceipted lane (R6).
    fn record_scope(&self, kind: LedgerEventKind) {
        self.ledger.record(kind, None, Some(KERNEL_SCOPE));
    }

    pub(crate) fn live_effects(&self, fiber: FiberId) -> Vec<jinnd_api::EffectDescriptor> {
        if fiber == KERNEL_SCOPE {
            return lock(&self.kernel_scope).tree();
        }
        lock(&self.fibers)
            .get(&fiber)
            .map(|entry| entry.fiber.effects())
            .unwrap_or_default()
    }

    pub(crate) fn take_over_document<C>(
        &self,
        path: std::path::PathBuf,
        baseline: &str,
    ) -> Result<(), KernelError>
    where
        C: Clone + Debug + PartialEq + serde::Serialize + Send + Sync + 'static,
    {
        let document = jinnd_loader::Document::parse(baseline)?;
        self.loader.attach_store::<C>(path.clone(), document);
        *lock(&self.document_path) = Some(path);
        Ok(())
    }

    pub(crate) fn withdraw_listener(&self, effect: EffectId) -> Result<(), KernelError> {
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

    pub(crate) async fn reconcile_runtime<C: Clone + Debug + Send + Sync + 'static>(
        &self,
        profile: Profile<C>,
    ) -> Result<ReconcileReport, KernelError> {
        let report = self.loader.reconcile(profile).await?;
        self.ledger.record(
            LedgerEventKind::WriteBack {
                detail: format!(
                    "reconcile: {} created, {} restarted, {} disposed, {} unchanged",
                    report.created.len(),
                    report.restarted.len(),
                    report.disposed.len(),
                    report.unchanged.len()
                ),
            },
            None,
            None,
        );
        // The error→entry rule: every contained fault is reachable from
        // its entry's ledger events (cycle diagnostics included, I3).
        for fault in &report.errors {
            self.ledger.record(
                LedgerEventKind::ErrorRecorded {
                    error: fault.error.clone(),
                },
                Some(fault.entry.clone()),
                self.loader.entry_fiber(&fault.entry),
            );
        }
        self.sync_all_transitions();
        Ok(report)
    }

    pub(crate) async fn run_revert(
        &self,
        effect: EffectId,
        key: RevertKey,
        witness: Witness,
    ) -> Result<RevertResolution, KernelError> {
        boundary::revert_admissible(
            &self.kernel_scope,
            &self.pending,
            self.revert.resolution(effect),
            effect,
        )?;
        let inverse = boundary::revert_inverse(&self.kernel_scope, effect);
        // Facade effects are charged to the kernel pseudo-fiber; the
        // protocol's events say so (R6 attribution).
        self.revert
            .revert(effect, key, witness, inverse, None, Some(KERNEL_SCOPE))
            .await
    }

    pub(crate) async fn run_compensation(
        &self,
        effect: EffectId,
        key: RevertKey,
        compensator: Box<dyn Undo>,
        operator_confirmed: bool,
    ) -> Result<RevertResolution, KernelError> {
        self.revert
            .compensate(
                effect,
                key,
                boundary::compensator_inverse(compensator),
                operator_confirmed,
                None,
                Some(KERNEL_SCOPE),
            )
            .await
    }

    pub(crate) fn fiber_transitions(&self, fiber: FiberId) -> Vec<Transition> {
        lock(&self.fibers)
            .get(&fiber)
            .map(|entry| entry.fiber.record().transitions)
            .unwrap_or_default()
    }
}
