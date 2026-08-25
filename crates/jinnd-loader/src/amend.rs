//! Runtime-originated amendments to the document of record. Direction matters
//! for "two views of one truth" (LAW §3): a reconcile is document-led — the
//! document commits first and the runtime converges on it — while the
//! amendments here are runtime-led: the runtime is offered the change first,
//! and the document follows only after the runtime accepted, so a rejected
//! change leaves both views at their prior state.

use std::any::Any;
use std::sync::Arc;

use jinnd_api::{EntryId, ErrorCode, KernelError, Profile, ProfileEntry, TransitionCause};

use crate::lanes::EntryHandle;
use crate::loader::{LaneConfig, Loader};
use crate::state::{error, lock};

/// One computed-but-uncommitted amendment of the committed document.
struct Amendment<C> {
    /// The amended document, ready to become the committed one.
    committed: Arc<dyn Any + Send + Sync>,
    /// The amended entry, ready to become the entry's applied spec.
    spec: Arc<dyn Any + Send + Sync>,
    /// The entry's config before the amendment, for withdrawing a staged
    /// config whose write-back failed.
    previous: C,
}

impl Loader {
    /// A runtime-originated config change: the entry's fiber validates and
    /// stages the new config first, the committed document is then written
    /// back atomically, and only then does the fiber reload to observe it.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] for an unknown entry or a foreign config
    /// type; whatever the lane answers for a rejected or unstatable payload;
    /// whatever the attached store answers for a failed write-back. On every
    /// error both views stay at the prior state: nothing is committed and a
    /// staged config is withdrawn.
    pub async fn update_entry<C: LaneConfig>(
        &self,
        entry: &EntryId,
        config: C,
    ) -> Result<(), KernelError> {
        let _gate = self.gate.lock().await;
        let amendment = self.amended::<C>(entry, |persisted| persisted.config = config.clone())?;
        let handle = self.live_handle(entry);
        // The runtime is offered the change first: a rejection commits
        // nothing anywhere.
        if let Some(handle) = &handle {
            handle.restate(&config)?;
        }
        // The document follows; a failed write-back withdraws the staged
        // config so both views stay at the prior state.
        if let Err(fault) = self.persist(&amendment.committed).await {
            if let Some(handle) = &handle {
                let _ = handle.restate(&amendment.previous);
            }
            return Err(fault);
        }
        self.commit(entry, amendment)?;
        // Both views committed: the fiber reloads to observe the config.
        if let Some(handle) = handle {
            handle.restart(TransitionCause::ConfigChanged);
            handle.quiesce().await?;
        }
        Ok(())
    }

    /// A runtime-originated disposal: the entry's fiber is withdrawn first,
    /// then the document persists the entry as disabled, config retained.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] for an unknown entry or a foreign config
    /// type; whatever the handle answers for a failed disposal (nothing is
    /// persisted or committed then); whatever the attached store answers for
    /// a failed write-back — the disposed fiber is dropped from the runtime
    /// either way, but the committed document then stays at the prior state
    /// and the fault is reported, never swallowed.
    pub async fn dispose_entry<C: LaneConfig>(&self, entry: &EntryId) -> Result<(), KernelError> {
        let _gate = self.gate.lock().await;
        let amendment = self.amended::<C>(entry, |persisted| persisted.disabled = true)?;
        let handle = self.live_handle(entry);
        // The runtime moves first: a refused disposal commits nothing.
        if let Some(handle) = &handle {
            handle.dispose().await?;
        }
        let persisted = self.persist(&amendment.committed).await;
        {
            let mut state = lock(&self.state);
            let runtime = state
                .entries
                .get_mut(entry)
                .ok_or_else(|| error(ErrorCode::InvalidProfile, "the entry has no runtime"))?;
            // The fiber is disposed whatever the write-back said: the
            // bookkeeping reflects reality.
            runtime.context = None;
            runtime.live = None;
            if persisted.is_ok() {
                runtime.spec = amendment.spec;
                state.committed = Some(amendment.committed);
            }
        }
        persisted
    }

    /// Computes one entry's amended committed document without committing
    /// anything: commitment is the caller's decision, after the runtime
    /// accepted the change.
    fn amended<C: LaneConfig>(
        &self,
        entry: &EntryId,
        change: impl FnOnce(&mut ProfileEntry<C>),
    ) -> Result<Amendment<C>, KernelError> {
        let state = lock(&self.state);
        if !state.entries.contains_key(entry) {
            return Err(error(ErrorCode::InvalidProfile, "the entry has no runtime"));
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
    fn commit<C: LaneConfig>(
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
    fn live_handle(&self, entry: &EntryId) -> Option<Arc<dyn EntryHandle>> {
        lock(&self.state)
            .entries
            .get(entry)?
            .live
            .as_ref()
            .map(|live| Arc::clone(&live.handle))
    }
}
