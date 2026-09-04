//! Runtime-led administration of the composition's SHAPE (M2-K23, harness
//! #37; constitution 04 §Write-back is confined): adding, removing,
//! disabling, enabling, re-granting and re-pinning an entry as operator
//! intent, applied by reconcile-by-id — the sibling of `update_entry` and
//! `dispose_entry` (split by responsibility, R10). Two phases, the M2-K8
//! #26 order: [`Loader::stage`] decides every refusal with nothing moved
//! and renders the document the commit will write (so a caller can record
//! the intent FIRST, Law 2); [`Loader::commit_administration`] offers the
//! runtime the change, writes the whole document back atomically
//! (`save_committed`, so preserved raw entries and unknown fields survive),
//! commits, and then STATES the runtime step — a restart, a spawn, or a
//! disposal whose landing is scheduled on its own task holding the
//! engagement, never awaited inside the caller's call (R1). A failed
//! write-back refuses with both views at their prior state; a runtime step
//! refused after the commit is a recorded divergence, loud (LAW §3).

use std::any::Any;
use std::sync::Arc;

use crate::diff::{Plan, Step};
use crate::gate::OwnedEngagement;
use crate::loader::{LaneConfig, Loader};
use crate::state::{error, lock};
use crate::store::EncodedProfile;
use crate::tree::EntryIndex;
use jinnd_api::{EntryId, ErrorCode, KernelError, PluginRef, Profile, ProfileEntry};

mod commit;
mod reads;
#[cfg(all(test, not(feature = "loom")))]
mod tests;

/// One operator intent on the composition's shape: the five writes of
/// `jinn:profile-admin`, as the plan step each applies.
#[derive(Clone, Debug)]
pub enum Administration<C> {
    /// A new entry: `Create` (or `Track` when it is effectively disabled).
    Add(ProfileEntry<C>),
    /// An entry leaves the document: `Remove` — its fiber withdrawn, the
    /// entry forgotten. The caller refuses an entry with children (R10:
    /// no cascade through this seam).
    Remove(EntryId),
    /// `disabled: true`: `Disable` — the fiber disposed, the entry kept.
    Disable(EntryId),
    /// `disabled: false`: `Enable` — a fresh incarnation, empty journal.
    Enable(EntryId),
    /// A new config (a grants change): `Restate` — a reload, never live.
    Configure(EntryId, C),
    /// A plugin-identity change: `Replace` — the entry's fiber is replaced.
    Swap(EntryId, PluginRef),
}

impl<C> Administration<C> {
    fn entry(&self) -> &EntryId {
        match self {
            Self::Add(record) => &record.id,
            Self::Remove(id)
            | Self::Disable(id)
            | Self::Enable(id)
            | Self::Configure(id, _)
            | Self::Swap(id, _) => id,
        }
    }

    /// `Add` and `Remove` change the tree, so they engage the DOCUMENT;
    /// the others engage the entry.
    fn engages_document(&self) -> bool {
        matches!(self, Self::Add(_) | Self::Remove(_))
    }
}

/// One administration with every refusal decided and nothing moved: the
/// entry (or document) engaged until the runtime step lands, the amended
/// document, its one plan step, and its rendering.
pub struct Staged<C> {
    engagement: OwnedEngagement,
    entry: EntryId,
    profile: Profile<C>,
    plan: Plan,
    committed: Arc<dyn Any + Send + Sync>,
    encoded: Option<EncodedProfile>,
    rendered: Option<String>,
}

impl<C> Staged<C> {
    /// The document of record as the commit will write it, byte-for-byte —
    /// `None` without a store. Its digest is the digest the file WILL have
    /// (Law 2: the row can name `after` before the write).
    #[must_use]
    pub fn rendered(&self) -> Option<&str> {
        self.rendered.as_deref()
    }
}

impl Loader {
    /// Stages one shape-changing operator intent: every refusal decided,
    /// nothing moved, the entry (or the document) engaged from here until
    /// the committed step lands or the stage is dropped.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] for a conflict (an operation in flight
    /// on the entry or the document, a target fiber not at rest, a
    /// withdrawal replay in flight, a call from a teardown context), for a
    /// malformed change (an id that already exists on `Add` or does not on
    /// the others, a parent that is not a live entry — the same structural
    /// faults the document-led index records), or for a config the store's
    /// `Serialize` refuses (contained, R11).
    pub fn stage<C: LaneConfig>(
        self: &Arc<Self>,
        change: Administration<C>,
    ) -> Result<Staged<C>, KernelError> {
        crate::refuse::refuse_teardown_context("the administration")?;
        let entry = change.entry().clone();
        let engagement = if change.engages_document() {
            OwnedEngagement::document(self)?
        } else {
            OwnedEngagement::entry(self, &entry)?
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
        let rendered = encoded
            .as_ref()
            .map(|(persistence, values)| persistence.merged(values).render());
        Ok(Staged {
            engagement,
            entry,
            profile,
            plan,
            committed,
            encoded,
            rendered,
        })
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
        Administration::Disable(id) => {
            let at = position(&profile, &id)?;
            profile.entries[at].disabled = true;
        }
        Administration::Enable(id) => {
            let at = position(&profile, &id)?;
            profile.entries[at].disabled = false;
        }
        Administration::Configure(id, config) => {
            let at = position(&profile, &id)?;
            profile.entries[at].config = config;
        }
        Administration::Swap(id, plugin) => {
            let at = position(&profile, &id)?;
            profile.entries[at].plugin = plugin;
        }
    }
    Ok(profile)
}
