//! Runtime-originated amendments to the document of record. Direction matters
//! for "two views of one truth" (LAW §3): a reconcile is document-led — the
//! document commits first and the runtime converges on it — while the
//! amendments here are runtime-led: the runtime is offered the change first,
//! and the document follows only after the runtime accepted, so a rejected
//! change leaves both views at their prior state. Disposal lives in
//! `dispose` (split by responsibility, R10).

use std::any::Any;
use std::sync::Arc;

use jinnd_api::{EntryId, ErrorCode, KernelError, Profile, ProfileEntry, TransitionCause};

use crate::lanes::EntryHandle;
use crate::loader::{LaneConfig, Loader};
use crate::state::{error, lock};
use crate::store::EncodedConfig;

/// One computed-but-uncommitted amendment of the committed document.
pub(crate) struct Amendment<C> {
    /// The amended document, ready to become the committed one.
    pub(crate) committed: Arc<dyn Any + Send + Sync>,
    /// The amended entry, ready to become the entry's applied spec.
    pub(crate) spec: Arc<dyn Any + Send + Sync>,
    /// The entry's config before the amendment, for withdrawing a staged
    /// config whose write-back failed.
    pub(crate) previous: C,
}

impl Loader {
    /// A runtime-originated config change: the entry's fiber validates and
    /// stages the new config first, the committed document is then written
    /// back atomically, and only then does the fiber reload to observe it.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] for an unknown or faulted entry, a
    /// foreign config type, an operation already in flight for this entry or
    /// the document, a call from within a fiber's teardown context, or —
    /// when the entry is live — a target fiber not at REST (mid-transition:
    /// the amendment would await a fiber that may be awaiting this very
    /// call, the self-deadlock class; M1-P6c round 2) or any tracked fiber's
    /// withdrawal replay in flight: the amendment would await the fiber, so
    /// it is refused retryably at the conflict point, never parked, from any
    /// task (R1, M1-P6b); whatever the config's own `Serialize` answers —
    /// contained, and run before anything is staged (R11, PLA-270);
    /// whatever the lane answers for a rejected or unstatable payload;
    /// whatever the attached store answers for a failed write-back. After
    /// any error exactly one of three states holds: both views at the prior
    /// state (the usual case — a staged config is withdrawn), both at the
    /// new state, or a recorded [`Loader::entry_faults`] divergence when the
    /// withdrawal itself failed (LAW §3: never dropped).
    pub async fn update_entry<C: LaneConfig>(
        &self,
        entry: &EntryId,
        config: C,
    ) -> Result<(), KernelError> {
        crate::refuse::refuse_teardown_context("the amendment")?;
        let _engaged = self.gate.engage_entry(entry)?;
        // Validation and the prior-config snapshot; the engagement keeps this
        // entry's slice of the document stable for the operation's span.
        let staged = self.amended::<C>(entry, |persisted| persisted.config = config.clone())?;
        // The new config's caller-authored `Serialize` runs here — outside
        // the persist permit, contained, and before the runtime is offered
        // anything: an unserializable or panicking config refuses the
        // amendment with nothing committed or staged anywhere (R1, R11,
        // PLA-270).
        let encoded = self.encode_config(&config)?;
        let handle = self.live_handle(entry);
        // A live entry's amendment will await its fiber, so it never begins
        // while that fiber is mid-transition — the REST gate, the round-2
        // mechanism (M1-P6c) behind the own-task fast path — nor amid a
        // withdrawal (round-4 law); every check precedes the restate — the
        // first side effect — so a refusal commits nothing anywhere.
        if let Some(handle) = &handle {
            crate::refuse::refuse_own_fiber(handle.as_ref(), "the amendment")?;
            crate::refuse::refuse_unrested(handle.as_ref(), "the amendment")?;
            self.refuse_amid_withdrawal("the amendment")?;
        }
        // The runtime is offered the change first: a rejection commits
        // nothing anywhere. No lock or permit is held here (R1).
        if let Some(handle) = &handle {
            handle.restate(&config)?;
        }
        // The document follows; a failed write-back withdraws the staged
        // config so both views stay at the prior state.
        if let Err(fault) = self
            .persist_amendment::<C>(entry, encoded, |persisted| persisted.config = config.clone())
            .await
        {
            let withdrawal = match &handle {
                Some(handle) => handle.restate(&staged.previous),
                None => Ok(()),
            };
            // A failed withdrawal is a divergence, recorded and loud: the
            // runtime staged the change the document never took.
            if let Err(withdrawal) = withdrawal {
                return Err(self.record_divergence(
                    entry,
                    staged.spec,
                    &format!(
                        "the two views diverged: the runtime staged {config:?}, the document \
                         holds {:?} (write-back failed: {}; withdrawal failed: {})",
                        staged.previous, fault.message, withdrawal.message
                    ),
                ));
            }
            return Err(fault);
        }
        // Both views committed: the fiber reloads to observe the config.
        // Only the engagement marker is held across the reload (R1).
        if let Some(handle) = handle {
            handle.restart(TransitionCause::ConfigChanged);
            handle.quiesce().await?;
        }
        Ok(())
    }

    /// Persists and commits one amendment under the persist permit. The
    /// amended document is re-derived from the committed one *inside* the
    /// permit, so a sibling entry's amendment committed meanwhile is never
    /// overwritten; the permit's span is mechanical — the new config value
    /// was encoded before it, the save rewrites plain values (R1, PLA-270).
    async fn persist_amendment<C: LaneConfig>(
        &self,
        entry: &EntryId,
        encoded: Option<EncodedConfig>,
        change: impl FnOnce(&mut ProfileEntry<C>),
    ) -> Result<(), KernelError> {
        let _permit = self.gate.persist_permit().await?;
        let amendment = self.amended::<C>(entry, change)?;
        if let Some((persistence, value)) = encoded {
            persistence.save_amendment(entry, Some(value), None).await?;
        }
        self.commit(entry, amendment)
    }

    /// Records one entry's divergence between the two views: the applied spec
    /// moves to the runtime's honest reality — so the next reconcile of the
    /// document of record plans the reconvergence — and the fault stays
    /// recorded until it lands (LAW §3: loud, never dropped).
    pub(crate) fn record_divergence(
        &self,
        entry: &EntryId,
        reality: Arc<dyn Any + Send + Sync>,
        message: &str,
    ) -> KernelError {
        let divergence = error(ErrorCode::InvalidProfile, message);
        let mut state = lock(&self.state);
        if let Some(runtime) = state.entries.get_mut(entry) {
            runtime.spec = reality;
            runtime.fault = Some(divergence.clone());
        }
        divergence
    }

    /// Computes one entry's amended committed document without committing
    /// anything: commitment is the caller's decision, after the runtime
    /// accepted the change.
    pub(crate) fn amended<C: LaneConfig>(
        &self,
        entry: &EntryId,
        change: impl FnOnce(&mut ProfileEntry<C>),
    ) -> Result<Amendment<C>, KernelError> {
        let state = lock(&self.state);
        let Some(runtime) = state.entries.get(entry) else {
            return Err(error(ErrorCode::InvalidProfile, "the entry has no runtime"));
        };
        // A faulted entry is honestly diverged: it refuses further amendments
        // until a reconcile of the document reconverges the two views.
        if runtime.fault.is_some() {
            return Err(error(
                ErrorCode::InvalidProfile,
                "the entry is faulted: reconcile the document to reconverge the two views",
            ));
        }
        let committed = state
            .committed
            .as_ref()
            .and_then(|committed| committed.downcast_ref::<Profile<C>>())
            .ok_or_else(|| error(ErrorCode::InvalidProfile, "foreign config type"))?;
        let mut profile = committed.clone();
        let persisted = profile
            .entries
            .iter_mut()
            .find(|candidate| candidate.id == *entry)
            .ok_or_else(|| error(ErrorCode::InvalidProfile, "no such entry"))?;
        let previous = persisted.config.clone();
        change(persisted);
        let spec = Arc::new(persisted.clone()) as Arc<dyn Any + Send + Sync>;
        Ok(Amendment {
            committed: Arc::new(profile) as Arc<dyn Any + Send + Sync>,
            spec,
            previous,
        })
    }

    /// Commits one accepted amendment: the document of record and the entry's
    /// applied spec move together, under the state lock alone (R1).
    pub(crate) fn commit<C: LaneConfig>(
        &self,
        entry: &EntryId,
        amendment: Amendment<C>,
    ) -> Result<(), KernelError> {
        let mut state = lock(&self.state);
        let runtime = state
            .entries
            .get_mut(entry)
            .ok_or_else(|| error(ErrorCode::InvalidProfile, "the entry has no runtime"))?;
        runtime.spec = amendment.spec;
        state.committed = Some(amendment.committed);
        Ok(())
    }

    /// The entry's live fiber handle, if any.
    pub(crate) fn live_handle(&self, entry: &EntryId) -> Option<Arc<dyn EntryHandle>> {
        lock(&self.state)
            .entries
            .get(entry)?
            .live
            .as_ref()
            .map(|live| Arc::clone(&live.handle))
    }
}
