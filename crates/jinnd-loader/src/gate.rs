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
//!   engagement — so every holder finishes and every waiter is served. The
//!   save it awaits is kernel-authored BY CONSTRUCTION: the store seam is
//!   sealed (`DocumentStore` is crate-internal; the public surface accepts a
//!   path), so no caller-authorable code can ever run under the permit
//!   (M1-P6c round 3).
//!
//! The conflict-point refusals live in [`crate::refuse`] (split by
//! responsibility, R10): the withdrawal-conflict refusal (the round-4 law),
//! the REST gate (the round-2 law: a fiber-awaiting amendment begins only
//! against a fiber at rest, decided from kernel-owned state), and the two
//! task-local fast paths (teardown marker, fiber identity) that are no
//! longer load-bearing.
//!
//! Deadlock freedom is structural: the only blocking wait a plugin can reach
//! is the permit, no permit holder waits on anything a plugin can hold up,
//! and no fiber-awaiting operation begins while a withdrawal — or a
//! transition of the target fiber itself — could hold up its waits.

use std::collections::HashSet;

use jinnd_api::{EntryId, ErrorCode, KernelError};
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::state::error;

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
