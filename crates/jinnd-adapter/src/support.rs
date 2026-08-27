//! Shared wiring the facade methods lean on: context and fiber lookup,
//! listener and package-lane registration, transition and amendment recording.

use std::fmt::Debug;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;

use jinnd_api::{
    ContextId, DispatchReport, EffectId, EntryId, ErrorCode, Event, EventListener, FiberId,
    KernelError, LedgerEventKind,
};
use jinnd_context::Context;
use jinnd_effects::Disposer;

use crate::{Adapter, FiberEntry, error, lock};

impl Adapter {
    pub(crate) fn context(&self, id: ContextId) -> Result<Context<()>, KernelError> {
        lock(&self.contexts).get(&id).cloned().ok_or_else(|| {
            error(
                ErrorCode::InactiveContext,
                "this kernel minted no such context",
            )
        })
    }

    /// Derives a child context bound per `isolation`. The facade cannot
    /// answer with an error here; a dead parent yields a fresh root child
    /// whose id is refused (`InactiveContext`) wherever it is used.
    pub(crate) fn derive_at(
        &self,
        parent: ContextId,
        isolation: &[jinnd_api::IsolationBinding],
    ) -> ContextId {
        match self.context(parent) {
            Ok(parent) => {
                let child = parent.derive().bind_all(isolation).build();
                lock(&self.contexts).insert(child.id(), child.clone());
                child.id()
            }
            Err(_) => self.root.derive().build().id(),
        }
    }

    pub(crate) fn entry(&self, id: FiberId) -> Result<Arc<FiberEntry>, KernelError> {
        lock(&self.fibers).get(&id).map(Arc::clone).ok_or_else(|| {
            error(
                ErrorCode::MissingDependency,
                "this kernel spawned no such fiber",
            )
        })
    }

    /// Registers a listener as an effect on the kernel scope (R5): the bus
    /// registration is the forward action, its idempotent removal is the undo.
    pub(crate) fn register_listener<E: Event, L: EventListener<E>>(
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

    /// Registers a loader lane as a kernel-scope effect (R5): withdrawing the
    /// effect unregisters the package. `C`'s `PartialEq` is the equality
    /// attestation reconcile-by-id diffs configs under.
    pub(crate) fn register_lane_effect<C: Clone + Debug + PartialEq + Send + Sync + 'static>(
        &self,
        package: &str,
        lane: jinnd_loader::PackageLane,
    ) -> Result<EffectId, KernelError> {
        self.loader.register_lane::<C>(package, lane)?;
        let loader = Arc::clone(&self.loader);
        let name = package.to_owned();
        let registered = lock(&self.kernel_scope).register(
            format!("package {package}"),
            Disposer::sync(move || {
                loader.unregister_lane::<C>(&name);
                Ok(())
            }),
        );
        if registered.is_err() {
            // A lane whose undo cannot be held may not outlive this call (R5).
            self.loader.unregister_lane::<C>(package);
        }
        registered
    }

    /// Emits every committed transition the ledger has not yet seen for
    /// `fiber` (R6: transitions are ledger events). No lock is held across an
    /// await or into plugin code — emission is the ordered, unreceipted lane.
    pub(crate) fn sync_transitions(&self, fiber: FiberId) {
        let transitions = lock(&self.fibers)
            .get(&fiber)
            .map(|entry| entry.fiber.record().transitions);
        let Some(transitions) = transitions else {
            return;
        };
        let mut seen = lock(&self.recorded_transitions);
        let count = seen.entry(fiber).or_insert(0);
        for transition in transitions.iter().skip(*count) {
            self.ledger.record(
                LedgerEventKind::FiberTransition(transition.clone()),
                None,
                Some(fiber),
            );
        }
        *count = transitions.len();
    }

    pub(crate) fn sync_all_transitions(&self) {
        let ids: Vec<FiberId> = lock(&self.fibers).keys().copied().collect();
        for id in ids {
            self.sync_transitions(id);
        }
    }

    /// Records one amendment attempt, accepted or refused, attributed to its
    /// entry (R6; constitution 02 family 4). An accepted amendment committed
    /// the document of record, so a write-back event follows it.
    pub(crate) fn record_amendment(
        &self,
        entry: &EntryId,
        verb: &str,
        outcome: &Result<(), KernelError>,
    ) {
        let detail = format!("{verb} {}", entry.0);
        match outcome {
            Ok(()) => {
                self.ledger.record(
                    LedgerEventKind::AmendmentAccepted {
                        detail: detail.clone(),
                    },
                    Some(entry.clone()),
                    self.loader.entry_fiber(entry),
                );
                self.ledger.record(
                    LedgerEventKind::WriteBack { detail },
                    Some(entry.clone()),
                    None,
                );
                self.sync_all_transitions();
            }
            Err(refusal) => {
                self.ledger.record(
                    LedgerEventKind::AmendmentRefused {
                        detail: format!("{verb} {}: {}", entry.0, refusal.message),
                    },
                    Some(entry.clone()),
                    self.loader.entry_fiber(entry),
                );
            }
        }
    }

    /// Validates the caller and runs one full mode walk on the bus.
    pub(crate) async fn report<E: Event>(
        &self,
        context: ContextId,
        event: E,
    ) -> Result<DispatchReport<E>, KernelError> {
        self.context(context)?;
        Ok(self.events.dispatch(context, event).await)
    }

    /// One full walk, answered with the outputs or the first contained
    /// failure. Every listener has settled by then: a failure is reported
    /// after the walk, never by aborting it (R9); the aggregate stays
    /// observable through `dispatch_report`.
    pub(crate) async fn dispatch_first_failure<E: Event>(
        &self,
        context: ContextId,
        event: E,
    ) -> Result<Vec<E::Output>, KernelError> {
        let report = self.report(context, event).await?;
        match report.failures.into_iter().next() {
            None => Ok(report.outputs),
            Some(failure) => Err(failure),
        }
    }

    /// Runs one runtime-led amendment and records the attempt — accepted AND
    /// refused — in the ledger (R6).
    pub(crate) async fn record_amended(
        &self,
        entry: &EntryId,
        verb: &str,
        run: impl std::future::Future<Output = Result<(), KernelError>>,
    ) -> Result<(), KernelError> {
        let outcome = run.await;
        self.record_amendment(entry, verb, &outcome);
        outcome
    }
}
