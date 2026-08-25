//! Race safety for loader operations without a lock held across plugin code
//! (R1, M1-P6b).
//!
//! Loader operations run plugin-facing work — lane constructors, restaters,
//! fiber teardown and settling — and that work runs on other tasks (a fiber's
//! teardown replays on the fiber's own task). Any wait a plugin can reach that
//! blocks on an in-flight operation is therefore a deadlock, whatever task it
//! comes from. The gate answers with two primitives, neither of which is ever
//! held across plugin-facing code:
//!
//! - **Engagement** — a refuse-not-wait busy marker. An operation engages its
//!   entry (a reconcile engages the whole document) for its full span,
//!   plugin-facing awaits included; a conflicting operation is refused
//!   honestly, never queued. Refusal cannot deadlock, so it is safe to hold
//!   while awaiting plugin work — it is a marker, not a lock.
//! - **The persist permit** — a one-permit semaphore serializing every
//!   write-back and commit of the document of record. Its critical section
//!   re-derives, persists, and commits — no plugin-facing call, no wait on
//!   engagement — so every holder finishes and every waiter is served.
//!
//! - **The withdrawal-conflict refusal** (the round-4 law, load-bearing) —
//!   the loader never *begins* a fiber-awaiting operation while any tracked
//!   fiber's withdrawal replay is in flight: refused at the conflict point,
//!   immediately and retryably, with no caller analysis and no timers
//!   ([`crate::loader::Loader::refuse_amid_withdrawal`]). Every deadlock in
//!   this class threads a kernel-owned wait through an in-progress teardown,
//!   and any call issued from within a teardown — on the fiber's task or on
//!   tasks it spawned — happens-after that withdrawal began, so it always
//!   observes the conflict. The wait the cycle needs is removed
//!   categorically, whichever crates it threads through.
//! - **The teardown marker** — a task-local fast path refusing amendments
//!   made directly from a fiber's teardown task, before engagement. No longer
//!   load-bearing (a plugin's own spawn escapes any task-local); kept because
//!   it answers the common shape earliest and cheapest.
//!
//! Deadlock freedom is structural: the only blocking wait a plugin can reach
//! is the permit, no permit holder waits on anything a plugin can hold up,
//! and no fiber-awaiting operation begins while a withdrawal could hold up
//! its waits.

use std::collections::HashSet;
use std::sync::Arc;

use jinnd_api::{EntryId, ErrorCode, KernelError};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::lanes::EntryHandle;
use crate::loader::Loader;
use crate::state::{error, lock};

/// Loom owns the model checker's primitives: the engagement cell is written
/// against this shim (`std` normally, `loom` under `--features loom`), exactly
/// as the fiber engine's steering cell is.
#[cfg(feature = "loom")]
use loom::sync::Mutex;
#[cfg(not(feature = "loom"))]
use std::sync::Mutex;

/// What is currently engaged.
#[derive(Default)]
struct Slots {
    /// A reconcile owns the whole document.
    document: bool,
    /// Entries with a runtime-led amendment in flight.
    entries: HashSet<EntryId>,
}

/// The refuse-not-wait exclusion cell. Modelled under loom in `models`.
pub(crate) struct Engagement {
    slots: Mutex<Slots>,
}

impl Default for Engagement {
    fn default() -> Self {
        Self {
            slots: Mutex::new(Slots::default()),
        }
    }
}

impl Engagement {
    /// Claims one entry unless the document or that entry is already engaged.
    pub(crate) fn engage_entry(&self, entry: &EntryId) -> bool {
        let mut slots = self
            .slots
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if slots.document || slots.entries.contains(entry) {
            return false;
        }
        slots.entries.insert(entry.clone());
        true
    }

    pub(crate) fn release_entry(&self, entry: &EntryId) {
        self.slots
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .entries
            .remove(entry);
    }

    /// Claims the document unless it, or any entry, is already engaged.
    pub(crate) fn engage_document(&self) -> bool {
        let mut slots = self
            .slots
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if slots.document || !slots.entries.is_empty() {
            return false;
        }
        slots.document = true;
        true
    }

    pub(crate) fn release_document(&self) {
        self.slots
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .document = false;
    }
}

impl Loader {
    /// Refuses to *begin* a fiber-awaiting operation while any tracked
    /// fiber's withdrawal replay is in flight (R1, M1-P6b round-4 law): the
    /// loader never takes a wait an in-progress teardown could hold up, so no
    /// deadlock cycle can close through a loader wait — whoever asks, from
    /// whatever task, with no caller analysis and no timers. The refusal is
    /// honest and retryable: amend again after quiescence.
    ///
    /// The check is causal, not racy: a call issued from within a withdrawal
    /// replay — directly or via tasks it spawned — happens-after that
    /// replay's begin, so it always observes the conflict here. A withdrawal
    /// that begins only *after* an operation was admitted cannot be waiting
    /// on that operation's outcome; its own re-entrant calls are refused by
    /// this same check, so it completes, releases its leases, and the
    /// admitted operation's waits resolve (I3).
    ///
    /// Handles are queried with no lock held (R1). Jurisdiction is honest
    /// too: the loader can only see fibers its document tracks — a
    /// harness-spawned fiber outside the document is outside this horizon.
    pub(crate) fn refuse_amid_withdrawal(&self, operation: &str) -> Result<(), KernelError> {
        let handles: Vec<Arc<dyn EntryHandle>> = lock(&self.state)
            .entries
            .values()
            .filter_map(|runtime| runtime.live.as_ref().map(|live| Arc::clone(&live.handle)))
            .collect();
        if handles.iter().any(|handle| handle.withdrawing()) {
            return Err(error(
                ErrorCode::InvalidProfile,
                &format!(
                    "{operation} refused: a fiber withdrawal is in flight; \
                     retry after quiescence"
                ),
            ));
        }
        Ok(())
    }
}

/// Refuses `operation` when the calling task IS the fiber the operation would
/// await (M1-P6c, extending the P6b conflict-point family): an entry's own
/// activation amending or disposing its entry makes the loader wait for a
/// fiber that cannot settle until this very call returns — a self-deadlock,
/// not a race. Refused honestly and retryably, with no caller analysis beyond
/// fiber identity ([`jinnd_fiber::current_fiber`]); the same operation against
/// a SIBLING entry from the same activation stays admissible.
pub(crate) fn refuse_own_fiber(
    handle: &dyn EntryHandle,
    operation: &str,
) -> Result<(), KernelError> {
    if jinnd_fiber::current_fiber() == Some(handle.id()) {
        return Err(error(
            ErrorCode::InvalidProfile,
            &format!(
                "{operation} refused: it would await the calling task's own fiber; \
                 amend from outside the fiber"
            ),
        ));
    }
    Ok(())
}

/// Refuses `operation` when invoked from within a fiber's teardown context
/// (M1-P6b): teardown is the wrong time to reshape the profile — I2 entitles
/// a dying plugin to call the services it leases while unloading, never to
/// amend the document — and admitting any such amendment reopens the
/// re-entrant deadlock class (R1).
pub(crate) fn refuse_teardown_context(operation: &str) -> Result<(), KernelError> {
    if jinnd_fiber::in_teardown() {
        return Err(error(
            ErrorCode::InvalidProfile,
            &format!("{operation} refused: the profile cannot be amended from a teardown context"),
        ));
    }
    Ok(())
}

/// The loader's gate: engagement plus the persist permit.
pub(crate) struct Gate {
    engagement: Engagement,
    persist: Semaphore,
}

/// An engaged entry; disengages on drop, error paths included.
pub(crate) struct EngagedEntry<'a> {
    gate: &'a Gate,
    entry: EntryId,
}

impl Drop for EngagedEntry<'_> {
    fn drop(&mut self) {
        self.gate.engagement.release_entry(&self.entry);
    }
}

/// The engaged document; disengages on drop, error paths included.
pub(crate) struct EngagedDocument<'a> {
    gate: &'a Gate,
}

impl Drop for EngagedDocument<'_> {
    fn drop(&mut self) {
        self.gate.engagement.release_document();
    }
}

impl Gate {
    pub(crate) fn new() -> Self {
        Self {
            engagement: Engagement::default(),
            persist: Semaphore::new(1),
        }
    }

    /// Engages `entry` for one runtime-led amendment.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] when a reconcile or another operation on
    /// this entry is in flight — which is what a plugin-facing callback
    /// re-entering the loader mid-operation observes, from any task: refused
    /// honestly, never deadlocked (R1).
    pub(crate) fn engage_entry<'a>(
        &'a self,
        entry: &EntryId,
    ) -> Result<EngagedEntry<'a>, KernelError> {
        if !self.engagement.engage_entry(entry) {
            return Err(error(
                ErrorCode::InvalidProfile,
                "operation refused: a loader operation is already in flight for this entry",
            ));
        }
        Ok(EngagedEntry {
            gate: self,
            entry: entry.clone(),
        })
    }

    /// Engages the document for one reconcile.
    ///
    /// # Errors
    ///
    /// As [`Gate::engage_entry`], for any in-flight operation.
    pub(crate) fn engage_document(&self) -> Result<EngagedDocument<'_>, KernelError> {
        if !self.engagement.engage_document() {
            return Err(error(
                ErrorCode::InvalidProfile,
                "reconcile refused: a loader operation is already in flight",
            ));
        }
        Ok(EngagedDocument { gate: self })
    }

    /// The permit under which every write-back and commit of the document of
    /// record runs. Waiting here is safe from any task: no holder runs
    /// plugin-facing code or waits on engagement, so every holder finishes.
    ///
    /// # Errors
    ///
    /// Unreachable — the gate is never closed — but answered honestly.
    pub(crate) async fn persist_permit(&self) -> Result<SemaphorePermit<'_>, KernelError> {
        self.persist
            .acquire()
            .await
            .map_err(|_closed| error(ErrorCode::InvalidProfile, "the loader gate is closed"))
    }
}
