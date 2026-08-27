//! Plan application: one step at a time, cancellable between steps (R1),
//! per-entry failures contained (R11), no lock held across an await or a lane.

use std::any::TypeId;
use std::sync::Arc;

use jinnd_api::{
    EntryFault, EntryId, ErrorCode, KernelError, Profile, ProfileEntry, ReconcileReport,
    TransitionCause,
};
use jinnd_context::Context;
use jinnd_registry::Injection;
use tokio_util::sync::CancellationToken;

use crate::diff::{Plan, StepKind};
use crate::lanes::SpawnRequest;
use crate::loader::{LaneConfig, Loader};
use crate::proxy::ReadinessProxy;
use crate::state::{EntryRuntime, Live, error, lock};
use crate::tree::EntryIndex;

impl Loader {
    /// Applies one plan against the desired `profile`.
    pub(crate) async fn apply<C: LaneConfig>(
        &self,
        plan: Plan,
        profile: &Profile<C>,
        cancel: &CancellationToken,
    ) -> ReconcileReport {
        let index = EntryIndex::new(profile);
        let mut report = ReconcileReport {
            unchanged: plan.unchanged,
            errors: plan.faults,
            ..ReconcileReport::default()
        };
        for step in plan.steps {
            if cancel.is_cancelled() {
                break;
            }
            let entry = step.entry;
            let outcome = match step.kind {
                StepKind::Remove => self.dispose_step(&entry, &index, true).await,
                StepKind::Disable => self.dispose_step(&entry, &index, false).await,
                StepKind::Rebind => self.rebind_step(&entry, &index),
                StepKind::Restate => self.restate_step(&entry, &index),
                StepKind::Replace => {
                    let disposed = self.dispose_step(&entry, &index, true).await;
                    match disposed {
                        Ok(_) => self.spawn_step(&entry, &index).map(|_| Bucket::Restarted),
                        Err(error) => Err(error),
                    }
                }
                StepKind::Create | StepKind::Enable => self.spawn_step(&entry, &index),
                StepKind::Track => self.track_step(&entry, &index),
            };
            match outcome {
                Ok(Bucket::Created) => report.created.push(entry),
                Ok(Bucket::Restarted) => report.restarted.push(entry),
                Ok(Bucket::Disposed) => report.disposed.push(entry),
                Ok(Bucket::Unchanged) => report.unchanged.push(entry),
                Err(error) => report.errors.push(EntryFault { entry, error }),
            }
        }
        report
    }

    /// Disposes an entry's fiber; `remove` forgets the entry entirely.
    async fn dispose_step<C: LaneConfig>(
        &self,
        entry: &EntryId,
        index: &EntryIndex<'_, C>,
        remove: bool,
    ) -> Result<Bucket, KernelError> {
        let live = {
            let mut state = lock(&self.state);
            if remove && index.get(entry).is_none() {
                state.entries.remove(entry).and_then(|runtime| runtime.live)
            } else {
                let runtime = state
                    .entries
                    .get_mut(entry)
                    .ok_or_else(|| error(ErrorCode::InvalidProfile, "the entry has no runtime"))?;
                if let Some(spec) = index.get(entry) {
                    runtime.spec = Arc::new(spec.clone());
                }
                runtime.context = None;
                runtime.live.take()
            }
        };
        match live {
            Some(live) => {
                live.handle.dispose().await?;
                Ok(Bucket::Disposed)
            }
            // A group or an already-inert entry disposes nothing observable.
            None => Ok(Bucket::Unchanged),
        }
    }

    /// States an entry's new config and reloads its fiber to observe it.
    fn restate_step<C: LaneConfig>(
        &self,
        entry: &EntryId,
        index: &EntryIndex<'_, C>,
    ) -> Result<Bucket, KernelError> {
        let spec = index
            .get(entry)
            .ok_or_else(|| error(ErrorCode::InvalidProfile, "the entry left the document"))?;
        let handle = {
            let mut state = lock(&self.state);
            let runtime = state
                .entries
                .get_mut(entry)
                .ok_or_else(|| error(ErrorCode::InvalidProfile, "the entry has no runtime"))?;
            runtime.spec = Arc::new(spec.clone());
            runtime.live.as_ref().map(|live| Arc::clone(&live.handle))
        };
        match handle {
            Some(handle) => {
                handle.restate(&spec.config)?;
                handle.restart(TransitionCause::ConfigChanged);
                Ok(Bucket::Restarted)
            }
            None => Ok(Bucket::Unchanged),
        }
    }

    /// Builds an entry's context and spawns its fiber through its lane.
    fn spawn_step<C: LaneConfig>(
        &self,
        entry: &EntryId,
        index: &EntryIndex<'_, C>,
    ) -> Result<Bucket, KernelError> {
        let spec = index
            .get(entry)
            .ok_or_else(|| error(ErrorCode::InvalidProfile, "the entry left the document"))?;
        let context = self.derive_context(spec)?;
        if spec.plugin.package == jinnd_api::GROUP_PACKAGE {
            self.store_runtime(entry, spec, Some(context), None);
            return Ok(Bucket::Unchanged);
        }
        let lane = lock(&self.lanes)
            .get(&(spec.plugin.package.clone(), TypeId::of::<C>()))
            .map(Arc::clone)
            .ok_or_else(|| {
                error(
                    ErrorCode::InvalidProfile,
                    &format!(
                        "no lane is registered for package {:?}",
                        spec.plugin.package
                    ),
                )
            })?;
        let readiness = self.registry.readiness(
            &context,
            Injection {
                services: lane.injects.clone(),
            },
        );
        let proxy = ReadinessProxy::new(readiness);
        // The lane runs implementer and plugin-declaration code: no loader
        // lock is held here (R1).
        let handle = (lane.spawn)(SpawnRequest {
            entry,
            at: &context,
            config: &spec.config,
            signal: proxy.signal(),
        })?;
        self.store_runtime(
            entry,
            spec,
            Some(context),
            Some(Live {
                lane,
                handle,
                proxy,
            }),
        );
        Ok(Bucket::Created)
    }

    /// Tracks an effectively disabled entry without spawning anything.
    fn track_step<C: LaneConfig>(
        &self,
        entry: &EntryId,
        index: &EntryIndex<'_, C>,
    ) -> Result<Bucket, KernelError> {
        let spec = index
            .get(entry)
            .ok_or_else(|| error(ErrorCode::InvalidProfile, "the entry left the document"))?;
        self.store_runtime(entry, spec, None, None);
        Ok(Bucket::Unchanged)
    }

    /// Derives an entry's context under its parent's, reporting the mint.
    pub(crate) fn derive_context<C: LaneConfig>(
        &self,
        spec: &ProfileEntry<C>,
    ) -> Result<Context<()>, KernelError> {
        let parent = match &spec.parent {
            None => self.root.clone(),
            Some(parent) => lock(&self.state)
                .entries
                .get(parent)
                .and_then(|runtime| runtime.context.clone())
                .ok_or_else(|| {
                    error(
                        ErrorCode::InvalidProfile,
                        "the parent entry has no live context",
                    )
                })?,
        };
        let context = parent.derive().bind_all(&spec.isolation).build();
        (self.on_context)(context.clone());
        Ok(context)
    }

    fn store_runtime<C: LaneConfig>(
        &self,
        entry: &EntryId,
        spec: &ProfileEntry<C>,
        context: Option<Context<()>>,
        live: Option<Live>,
    ) {
        lock(&self.state).entries.insert(
            entry.clone(),
            EntryRuntime {
                spec: Arc::new(spec.clone()),
                context,
                live,
                fault: None,
            },
        );
    }
}

pub(crate) enum Bucket {
    Created,
    Restarted,
    Disposed,
    Unchanged,
}
