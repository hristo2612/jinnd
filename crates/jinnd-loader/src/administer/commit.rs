//! The commit half of an administration (split from `administer.rs` by
//! responsibility, R10): the M2-K8 #26 order — offer, write back, commit,
//! then STATE the runtime step, its landing scheduled on its own task with
//! the engagement held (R1); a failed write-back refuses with both views
//! prior; a step refused after the commit is a recorded divergence (LAW §3).

use std::any::Any;
use std::sync::Arc;

use jinnd_api::{EntryId, KernelError, Profile, TransitionCause};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::Staged;
use crate::diff::StepKind;
use crate::loader::{LaneConfig, Loader};
use crate::state::lock;
use crate::store::EncodedProfile;
use crate::tree::EntryIndex;

impl Loader {
    /// Commits one staged administration in the M2-K8 #26 order: the
    /// runtime is offered the change (a new config is restated), the
    /// document is written back and committed under the persist permit,
    /// then the runtime step is STATED — a reload, a spawn, or a disposal
    /// whose landing (and, for a replacement, whose successor's spawn)
    /// runs on the returned task with the engagement held until it lands.
    /// Nothing here awaits a fiber (R1).
    ///
    /// # Errors
    ///
    /// Whatever the lane answers for an unstatable config, or the store for
    /// a failed write-back — both views at their prior state then, the
    /// engagement released. A runtime step the lane refuses AFTER the
    /// commit is returned too, as the recorded divergence it is
    /// ([`Loader::entry_faults`]; the next document-led reconcile
    /// reconverges the two views — LAW §3, never dropped).
    pub async fn commit_administration<C: LaneConfig>(
        self: &Arc<Self>,
        staged: Staged<C>,
    ) -> Result<JoinHandle<()>, KernelError> {
        let Staged {
            engagement,
            entry,
            profile,
            plan,
            committed,
            encoded,
            ..
        } = staged;
        let index = EntryIndex::new(&profile);
        let spec = index.get(&entry).map(|spec| Arc::new(spec.clone()));
        let step = plan.steps.first().map(|step| step.kind);
        let handle = self.live_handle(&entry);
        let restated = match (step, &handle, &spec) {
            (Some(StepKind::Restate), Some(handle), Some(spec)) => {
                handle.restate(&spec.config)?;
                self.persisted::<C>()
                    .and_then(|old| old.entries.into_iter().find(|old| old.id == entry))
            }
            _ => None,
        };
        if let Err(fault) = self.persist_committed(&encoded, committed).await {
            if let (Some(previous), Some(handle)) = (restated, &handle) {
                handle.restate(&previous.config)?;
            }
            return Err(fault);
        }
        // Both views committed: the runtime step is stated.
        let chain = match step {
            Some(kind @ (StepKind::Remove | StepKind::Disable | StepKind::Replace)) => {
                Some((kind, handle.map(|handle| handle.dispose())))
            }
            Some(StepKind::Restate) => {
                self.restate_spec(&entry, spec);
                if let Some(handle) = handle {
                    handle.restart(TransitionCause::ConfigChanged);
                }
                None
            }
            Some(_) => {
                let report = self.apply(plan, &profile, &CancellationToken::new()).await;
                if let Some(fault) = report.errors.into_iter().find(|fault| fault.entry == entry) {
                    return Err(self.divergence(&entry, spec, &fault.error));
                }
                None
            }
            None => {
                self.restate_spec(&entry, spec);
                None
            }
        };
        let loader = Arc::clone(self);
        Ok(tokio::spawn(async move {
            let _engaged = engagement;
            if let Some((kind, disposal)) = chain {
                if let Some(disposal) = disposal {
                    // A failed withdrawal is the fiber's own trail (R11).
                    let _ = disposal.await;
                }
                loader.landed(&entry, kind, &profile);
            }
        }))
    }

    /// The write-back and commit, under the persist permit: nothing has
    /// moved yet, so a failed save refuses with both views prior.
    async fn persist_committed(
        &self,
        encoded: &Option<EncodedProfile>,
        committed: Arc<dyn Any + Send + Sync>,
    ) -> Result<(), KernelError> {
        let _permit = self.gate.persist_permit().await?;
        if let Some((persistence, values)) = encoded {
            persistence.save_committed(values).await?;
        }
        lock(&self.state).committed = Some(committed);
        Ok(())
    }

    /// The disposal landed: the runtime row follows the document — gone on
    /// `Remove`, inert on `Disable` — and a replacement's successor spawns
    /// now, never beside the old incarnation (one entry, one fiber).
    fn landed<C: LaneConfig>(&self, entry: &EntryId, kind: StepKind, profile: &Profile<C>) {
        let index = EntryIndex::new(profile);
        {
            let mut state = lock(&self.state);
            if kind == StepKind::Remove {
                state.entries.remove(entry);
            } else if let Some(runtime) = state.entries.get_mut(entry) {
                if let Some(spec) = index.get(entry) {
                    runtime.spec = Arc::new(spec.clone());
                }
                runtime.context = None;
                runtime.live = None;
            }
        }
        if kind == StepKind::Replace
            && let Err(fault) = self.spawn_step(entry, &index)
        {
            let spec = index.get(entry).map(|spec| Arc::new(spec.clone()));
            let _ = self.divergence(entry, spec, &fault);
        }
    }

    /// The applied spec follows the committed record when no fiber moves.
    fn restate_spec(&self, entry: &EntryId, spec: Option<Arc<impl Any + Send + Sync>>) {
        if let (Some(runtime), Some(spec)) = (lock(&self.state).entries.get_mut(entry), spec) {
            runtime.spec = spec;
        }
    }

    /// A runtime step refused after the document committed (LAW §3).
    fn divergence(
        &self,
        entry: &EntryId,
        spec: Option<Arc<impl Any + Send + Sync>>,
        fault: &KernelError,
    ) -> KernelError {
        let reality = spec.map_or_else(
            || Arc::new(()) as Arc<dyn Any + Send + Sync>,
            |spec| spec as Arc<dyn Any + Send + Sync>,
        );
        self.record_divergence(
            entry,
            reality,
            &format!(
                "the two views diverged: the document committed, the runtime refused the \
                 step ({}); the next reconcile reconverges them",
                fault.message
            ),
        )
    }
}
