//! Runtime-led administration of the composition's SHAPE (M2-K23, harness
//! #37; constitution 04 §Write-back is confined): adding, removing,
//! enabling and re-pinning an entry as operator intent, applied by
//! reconcile-by-id — the sibling of `update_entry_deferred` and
//! `dispose_entry` (split by responsibility, R10). The runtime is offered
//! the change first, as EXACTLY the plan step the document-led diff would
//! produce for that entry and no other; the whole document is then written
//! back atomically (`save_committed`, so preserved raw entries and unknown
//! fields survive); the daemon treats the write-back as its own echo. A
//! refusal commits nothing anywhere; a failed write-back is retried once and
//! then returned loud, never swallowed (LAW §3, the `amend.rs` law).

use std::any::Any;
use std::sync::Arc;

use jinnd_api::{EntryId, ErrorCode, KernelError, PluginRef, Profile, ProfileEntry};
use tokio_util::sync::CancellationToken;

use crate::diff::Step;
use crate::loader::{LaneConfig, Loader};
use crate::state::{error, lock};
use crate::tree::EntryIndex;

#[cfg(all(test, not(feature = "loom")))]
mod tests;

/// One operator intent on the composition's shape. A grants change is a
/// config change and rides [`Loader::update_entry_deferred`]; a disable
/// rides [`Loader::dispose_entry`] — neither needs a second mechanism.
#[derive(Clone, Debug)]
pub enum Administration<C> {
    /// A new entry: `Create` (or `Track` when it is effectively disabled).
    Add(ProfileEntry<C>),
    /// An entry leaves the document: `Remove` — its fiber withdrawn, the
    /// entry forgotten. The caller refuses an entry with children (R10:
    /// no cascade through this seam).
    Remove(EntryId),
    /// `disabled: false`: `Enable` — a fresh incarnation, empty journal.
    Enable(EntryId),
    /// A plugin-identity change: `Replace` — the entry's fiber is replaced.
    Swap(EntryId, PluginRef),
}

impl<C> Administration<C> {
    fn entry(&self) -> &EntryId {
        match self {
            Self::Add(record) => &record.id,
            Self::Remove(id) | Self::Enable(id) | Self::Swap(id, _) => id,
        }
    }

    /// `Add` and `Remove` change the tree, so they engage the DOCUMENT;
    /// the others engage the entry.
    fn engages_document(&self) -> bool {
        matches!(self, Self::Add(_) | Self::Remove(_))
    }
}

impl Loader {
    /// Applies one shape-changing operator intent: runtime first (the one
    /// plan step reconcile-by-id would produce for the entry), then the
    /// document, atomically. The spawned fiber's activation is SCHEDULED
    /// and never awaited; a withdrawn fiber's disposal is awaited exactly
    /// as [`Loader::dispose_entry`] awaits it.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] for a conflict (an operation in flight
    /// on the entry or the document, a target fiber not at rest, a
    /// withdrawal replay in flight, a call from a teardown context), for a
    /// malformed change (an id that already exists on `Add` or does not on
    /// the others, a parent that is not a live entry — the same structural
    /// faults the document-led index records), for a runtime refusal of
    /// the step (nothing committed then), or for a write-back that failed
    /// twice (the runtime moved; the next reconcile of the document
    /// reconverges the two views — loud, LAW §3).
    pub async fn administer<C: LaneConfig>(
        &self,
        change: Administration<C>,
    ) -> Result<(), KernelError> {
        crate::refuse::refuse_teardown_context("the administration")?;
        let entry = change.entry().clone();
        let (_document, _entry) = if change.engages_document() {
            (Some(self.gate.engage_document()?), None)
        } else {
            (None, Some(self.gate.engage_entry(&entry)?))
        };
        self.refuse_amid_withdrawal("the administration")?;
        if let Some(handle) = self.live_handle(&entry) {
            crate::refuse::refuse_own_fiber(handle.as_ref(), "the administration")?;
            crate::refuse::refuse_unrested(handle.as_ref(), "the administration")?;
        }
        let Some(old) = self.persisted::<C>() else {
            return Err(error(ErrorCode::InvalidProfile, "no committed document"));
        };
        let profile = amended_profile(&old, change)?;
        let index = EntryIndex::new(&profile);
        if let Some(fault) = index.faults.iter().find(|fault| fault.entry == entry) {
            return Err(fault.error.clone());
        }
        // The one step: what the document-led diff would plan for this
        // entry, and nothing for any other (I4 converges the rest).
        let applied = self.applied_or_empty::<C>()?;
        let mut plan = crate::diff::plan(
            Some(&applied),
            &profile,
            self.attestation::<C>()
                .as_ref()
                .map(|eq| eq as &dyn Fn(&C, &C) -> bool),
        );
        plan.steps.retain(|step: &Step| step.entry == entry);
        plan.faults.clear();
        plan.unchanged.clear();
        let committed = Arc::new(profile.clone()) as Arc<dyn Any + Send + Sync>;
        // The caller-authored `Serialize` runs before anything moves,
        // contained (R1, R11, PLA-270).
        let encoded = self.encode_committed(&committed)?;
        let report = self.apply(plan, &profile, &CancellationToken::new()).await;
        if let Some(fault) = report.errors.into_iter().find(|fault| fault.entry == entry) {
            return Err(fault.error);
        }
        // The document follows, under the one persist permit; the runtime
        // already moved, so a failed write-back is retried once and then
        // returned loud — the disposal precedent (LAW §3).
        let _permit = self.gate.persist_permit().await?;
        if let Some((persistence, values)) = &encoded {
            let mut saved = persistence.save_committed(values).await;
            if saved.is_err() {
                saved = persistence.save_committed(values).await;
            }
            saved?;
        }
        lock(&self.state).committed = Some(committed);
        Ok(())
    }

    /// The applied document, or the empty document before any reconcile.
    fn applied_or_empty<C: LaneConfig>(&self) -> Result<Profile<C>, KernelError> {
        Ok(self.applied::<C>()?.unwrap_or(Profile {
            entries: Vec::new(),
        }))
    }

    /// The config type's registered equality attestation, erased once.
    fn attestation<C: LaneConfig>(&self) -> Option<impl Fn(&C, &C) -> bool> {
        lock(&self.eqs)
            .get(&std::any::TypeId::of::<C>())
            .cloned()
            .map(|eq| {
                move |a: &C, b: &C| eq(a as &(dyn Any + Send + Sync), b as &(dyn Any + Send + Sync))
            })
    }

    /// Whether a lane is registered for `package` under config type `C`:
    /// an admin write naming a package no reconcile ever admitted refuses
    /// with the Law-5 reason rather than spawning into nothing.
    #[must_use]
    pub fn has_lane<C: LaneConfig>(&self, package: &str) -> bool {
        lock(&self.lanes).contains_key(&(package.to_owned(), std::any::TypeId::of::<C>()))
    }

    /// The document of record as the attached store last rendered it —
    /// byte-for-byte what is on disk — or `None` without a store. A digest
    /// of this is a digest of the file (Law 2: checkable with nothing but
    /// the file).
    #[must_use]
    pub fn rendered_document(&self) -> Option<String> {
        self.persistence().map(|persistence| persistence.rendered())
    }

    /// The ids of entries preserved RAW (undecodable, re-emitted verbatim;
    /// R11): their record cannot be captured as a typed prior.
    #[must_use]
    pub fn raw_entry_ids(&self) -> Vec<EntryId> {
        self.persistence()
            .map(|persistence| persistence.raw_ids())
            .unwrap_or_default()
    }
}

/// The committed document with one change applied — pure, so a malformed
/// change refuses before anything is engaged with the runtime.
fn amended_profile<C: LaneConfig>(
    old: &Profile<C>,
    change: Administration<C>,
) -> Result<Profile<C>, KernelError> {
    let mut profile = old.clone();
    let position = |profile: &Profile<C>, id: &EntryId| {
        profile
            .entries
            .iter()
            .position(|candidate| candidate.id == *id)
            .ok_or_else(|| {
                error(
                    ErrorCode::InvalidProfile,
                    "no such entry in the document of record",
                )
            })
    };
    match change {
        Administration::Add(record) => {
            if position(&profile, &record.id).is_ok() {
                return Err(error(
                    ErrorCode::InvalidProfile,
                    "the entry id already exists",
                ));
            }
            profile.entries.push(record);
        }
        Administration::Remove(id) => {
            let at = position(&profile, &id)?;
            profile.entries.remove(at);
        }
        Administration::Enable(id) => {
            let at = position(&profile, &id)?;
            profile.entries[at].disabled = false;
        }
        Administration::Swap(id, plugin) => {
            let at = position(&profile, &id)?;
            profile.entries[at].plugin = plugin;
        }
    }
    Ok(profile)
}
