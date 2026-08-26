//! Runtime-originated disposal of one entry (split from `amend` by
//! responsibility, R10): the runtime moves first — the fiber is withdrawn —
//! then the document persists the entry as disabled, config retained.

use jinnd_api::{EntryId, ErrorCode, KernelError, ProfileEntry};

use crate::loader::{LaneConfig, Loader};
use crate::state::{error, lock};

impl Loader {
    /// A runtime-originated disposal: the entry's fiber is withdrawn first,
    /// then the document persists the entry as disabled, config retained.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] for an unknown or faulted entry, a
    /// foreign config type, an operation already in flight for this entry or
    /// the document, a call from within a fiber's teardown context, or —
    /// when the entry is live — a target fiber not at REST (M1-P6c round 2)
    /// or any tracked fiber's withdrawal replay in flight (refused retryably
    /// at the conflict point, never parked, from any task; R1, M1-P6b);
    /// whatever the handle answers for a failed disposal (nothing is
    /// persisted or committed then). Disposal is irreversible at runtime, so
    /// a failed write-back is retried once; failing again, the divergence —
    /// runtime disposed, document enabled — is recorded in
    /// [`Loader::entry_faults`] and returned, so the next reconcile of the
    /// document reconverges the two views (LAW §3: never swallowed).
    pub async fn dispose_entry<C: LaneConfig>(&self, entry: &EntryId) -> Result<(), KernelError> {
        crate::refuse::refuse_teardown_context("the disposal")?;
        let _engaged = self.gate.engage_entry(entry)?;
        // Validation and the reality snapshot for a recorded divergence.
        let staged = self.amended::<C>(entry, |persisted| persisted.disabled = true)?;
        let handle = self.live_handle(entry);
        // A live entry's disposal awaits its fiber's withdrawal: it never
        // begins while that fiber is mid-transition (the REST gate, M1-P6c
        // round 2) nor amid another withdrawal already in flight (round-4
        // law).
        if let Some(handle) = &handle {
            crate::refuse::refuse_own_fiber(handle.as_ref(), "the disposal")?;
            crate::refuse::refuse_unrested(handle.as_ref(), "the disposal")?;
            self.refuse_amid_withdrawal("the disposal")?;
        }
        // The runtime moves first: a refused disposal commits nothing. The
        // teardown replays plugin-owned inverses on the fiber's own task with
        // only the engagement marker held — a teardown calling back into the
        // loader is refused honestly, never deadlocked (R1, M1-P6b).
        if let Some(handle) = &handle {
            handle.dispose().await?;
        }
        if let Err(fault) = self.persist_disposal::<C>(entry).await {
            return Err(self.record_divergence(
                entry,
                staged.spec,
                &format!(
                    "the two views diverged: the runtime is disposed, the document stays \
                     enabled (write-back failed: {})",
                    fault.message
                ),
            ));
        }
        Ok(())
    }

    /// The disposal's write-back and commit, under the persist permit. The
    /// amended document is re-derived inside the permit, so amendments
    /// another task landed meanwhile are never overwritten; the permit's
    /// span is mechanical — the disabled flag is a plain-value rewrite, the
    /// config is retained as persisted, no caller-supplied code anywhere
    /// (R1, PLA-270). The disposal cannot be taken back, so a failed
    /// write-back is retried once — and the applied view moves to the
    /// disposed reality whatever the write-back said.
    async fn persist_disposal<C: LaneConfig>(&self, entry: &EntryId) -> Result<(), KernelError> {
        let persistence = self.persistence();
        let _permit = self.gate.persist_permit().await?;
        let amendment = self.amended::<C>(entry, |persisted: &mut ProfileEntry<C>| {
            persisted.disabled = true;
        })?;
        let save = || async {
            match &persistence {
                None => Ok(()),
                Some(persistence) => persistence.save_amendment(entry, None, Some(true)).await,
            }
        };
        let mut persisted = save().await;
        if persisted.is_err() {
            persisted = save().await;
        }
        let mut state = lock(&self.state);
        {
            let runtime = state
                .entries
                .get_mut(entry)
                .ok_or_else(|| error(ErrorCode::InvalidProfile, "the entry has no runtime"))?;
            runtime.context = None;
            runtime.live = None;
            runtime.spec = std::sync::Arc::clone(&amendment.spec);
        }
        persisted?;
        state.committed = Some(amendment.committed);
        Ok(())
    }
}
