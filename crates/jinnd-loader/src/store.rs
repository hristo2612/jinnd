//! Atomic profile persistence: write-temp + fsync + rename, so the document on
//! disk is always whole (LAW §3 bidirectional persistence; v0.1 constitution
//! bounds: local-only profiles, no destructive compaction — saves replace, they
//! never rewrite history).
//!
//! The save pipeline has two halves (M1-P6c round 2; R1, PLA-270): ENCODING —
//! the only place a config type's caller-authored `Serialize` runs — happens
//! before the persist permit is acquired, behind panic containment (R11);
//! SAVING — a mechanical merge of plain values over the baseline document plus
//! the write — is all the permit ever spans. No caller-supplied code can run
//! inside the permit, by construction.

use std::any::Any;
use std::sync::Arc;

use jinnd_api::{EntryId, ErrorCode, KernelError, KernelFuture, Profile};

use crate::document::Document;
use crate::loader::{LaneConfig, Loader};
use crate::state::{error, lock};

/// Where the committed document persists. Kernel-internal BY DESIGN (M1-P6c
/// round 3): the persist permit awaits `save`, so every implementation must
/// be kernel-authored for the permit's no-caller-code guarantee to be
/// structural rather than conventional. [`crate::FileStore`] is the standard
/// medium; the only other impls are this crate's own test doubles. The
/// public surface accepts a path, never a store implementation
/// ([`Loader::attach_store`]).
pub(crate) trait DocumentStore: Send + Sync + 'static {
    /// Saves one whole document; atomic per the implementation's medium.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] when the document cannot be saved.
    fn save<'a>(&'a self, document: &'a Document) -> KernelFuture<'a, ()>;
}

/// Encodes one type-erased committed profile into its value form: the only
/// caller-authored `Serialize` in the pipeline runs behind this, contained.
type EncodeProfile = Box<
    dyn Fn(&(dyn Any + Send + Sync)) -> Result<Profile<serde_json::Value>, KernelError>
        + Send
        + Sync,
>;

/// Encodes one type-erased config payload likewise.
type EncodeConfig =
    Box<dyn Fn(&(dyn Any + Send + Sync)) -> Result<serde_json::Value, KernelError> + Send + Sync>;

/// One coherent encode outcome: the persistence snapshot the encoding must be
/// saved through, paired with what was encoded outside the permit.
pub(crate) type EncodedProfile = (Arc<Persistence>, Profile<serde_json::Value>);
/// As [`EncodedProfile`], for one amended config payload.
pub(crate) type EncodedConfig = (Arc<Persistence>, serde_json::Value);

/// The attached store, the kernel-owned encoders, and the committed document —
/// the persistence unit (M1-P6c): raw entries and unknown fields live in the
/// baseline and survive every write-back.
pub(crate) struct Persistence {
    store: Box<dyn DocumentStore>,
    encode_profile: EncodeProfile,
    encode_config: EncodeConfig,
    baseline: std::sync::Mutex<Document>,
    /// The pending write-back echo (M2-K5 #17): the exact text of the last
    /// save this persistence made — what the loader WROTE, remembered at the
    /// save, never re-read from a file another writer may have replaced
    /// since. ONE-SHOT (round 2): consumed by the one delivery that matches
    /// it, superseded by the next save.
    written: std::sync::Mutex<Option<String>>,
}

impl Persistence {
    /// The baseline rendered: the bytes the last save wrote (M2-K23).
    pub(crate) fn rendered(&self) -> String {
        lock(&self.baseline).render()
    }

    /// The ids of the baseline's preserved raw entries (M2-K23).
    pub(crate) fn raw_ids(&self) -> Vec<EntryId> {
        lock(&self.baseline)
            .raw
            .iter()
            .map(crate::raw::RawEntry::entry_id)
            .collect()
    }

    /// Saves one pre-encoded committed profile over the baseline document.
    /// Mechanical throughout — plain values merged and written, no
    /// caller-supplied code — so the persist permit may span it (R1). The
    /// saved document becomes the next save's baseline, so preserved raw
    /// entries and unknown fields survive every consecutive write-back.
    pub(crate) async fn save_committed(
        &self,
        values: &Profile<serde_json::Value>,
    ) -> Result<(), KernelError> {
        let document = {
            let baseline = lock(&self.baseline);
            Document::merge_profile(values, &baseline)
        };
        self.store.save(&document).await?;
        *lock(&self.written) = Some(document.render());
        *lock(&self.baseline) = document;
        Ok(())
    }

    /// Saves one entry's runtime-led amendment by rewriting the committed
    /// document in place: sibling amendments landed meanwhile are read from
    /// the baseline, never overwritten. Mechanical throughout (R1); the new
    /// config value, if any, was encoded before the permit was taken.
    pub(crate) async fn save_amendment(
        &self,
        entry: &EntryId,
        config: Option<serde_json::Value>,
        disabled: Option<bool>,
    ) -> Result<(), KernelError> {
        let document = {
            let baseline = lock(&self.baseline);
            baseline.amended(&entry.0, config, disabled)?
        };
        self.store.save(&document).await?;
        *lock(&self.written) = Some(document.render());
        *lock(&self.baseline) = document;
        Ok(())
    }
}

impl Loader {
    /// Attaches the persistence store (LAW §3): every commit of the document
    /// of record — reconcile, update, dispose — writes back atomically
    /// through `store`. A document-led reconcile persists before the runtime
    /// converges on the document; a runtime-led amendment persists after the
    /// runtime accepted the change (see `amend`).
    ///
    /// `baseline` is the loaded document being taken over (or
    /// [`Document::default`] for a fresh store): its raw entries and unknown
    /// fields are carried through every save verbatim (M1-P6c). `C`'s
    /// `Serialize` — caller-authored code — runs only inside the encoders
    /// captured here: outside the persist permit, behind panic containment
    /// (R1, R11, PLA-270). The surface accepts a path, never a store
    /// implementation: every [`DocumentStore`] is kernel-authored, so the
    /// permit structurally cannot span caller-authorable code (round 3).
    /// Re-attaching replaces the previous store.
    pub fn attach_store<C: LaneConfig + serde::Serialize>(
        &self,
        path: std::path::PathBuf,
        baseline: Document,
    ) {
        self.attach_store_with::<C>(Box::new(crate::FileStore::new(path)), baseline);
    }

    /// The sealed lane [`Loader::attach_store`] narrows to: `store` must be
    /// one of this crate's own [`DocumentStore`] impls — [`crate::FileStore`]
    /// or a crate-owned test double (M1-P6c round 3).
    pub(crate) fn attach_store_with<C: LaneConfig + serde::Serialize>(
        &self,
        store: Box<dyn DocumentStore>,
        baseline: Document,
    ) {
        let encode_profile: EncodeProfile = Box::new(|committed| {
            // A committed profile of another config type is an honest
            // failure, never a silent skip: the disk may not drift.
            let Some(profile) = committed.downcast_ref::<Profile<C>>() else {
                return Err(foreign());
            };
            value_profile(profile)
        });
        let encode_config: EncodeConfig = Box::new(|config| {
            let Some(config) = config.downcast_ref::<C>() else {
                return Err(foreign());
            };
            contained_value("the amended entry", config)
        });
        *lock(&self.persist) = Some(Arc::new(Persistence {
            store,
            encode_profile,
            encode_config,
            baseline: std::sync::Mutex::new(baseline),
            written: std::sync::Mutex::new(None),
        }));
    }

    /// Retires the pending write-back echo (M2-K5 #17, round 2): `true`
    /// exactly once, for the delivery whose text is the bytes the attached
    /// store last WROTE — the signature is consumed on the match. Any later
    /// delivery of the same bytes is an operator rewrite: `false`, and the
    /// caller reconciles it (the diff answers `unchanged`). A newer save
    /// supersedes an unretired signature — the bytes it named left the
    /// disk — so no signature lies in wait for a later identical rewrite.
    /// `false` without a store or before the first save.
    #[must_use]
    pub fn retire_echo(&self, delivered: &str) -> bool {
        let Some(persistence) = self.persistence() else {
            return false;
        };
        let mut written = lock(&persistence.written);
        if written.as_deref() == Some(delivered) {
            *written = None;
            return true;
        }
        false
    }

    /// The attached persistence, if any: one coherent snapshot per operation.
    pub(crate) fn persistence(&self) -> Option<Arc<Persistence>> {
        lock(&self.persist).clone()
    }

    /// Encodes the committed profile for persistence — the caller-authored
    /// `Serialize` runs HERE, outside the persist permit and contained (R1,
    /// R11, PLA-270) — paired with the persistence it must be saved through.
    /// `None` when no store is attached.
    pub(crate) fn encode_committed(
        &self,
        committed: &Arc<dyn Any + Send + Sync>,
    ) -> Result<Option<EncodedProfile>, KernelError> {
        match self.persistence() {
            None => Ok(None),
            Some(persistence) => {
                let values = (persistence.encode_profile)(committed.as_ref())?;
                Ok(Some((persistence, values)))
            }
        }
    }

    /// Encodes one amended config payload likewise; `None` when no store is
    /// attached.
    pub(crate) fn encode_config(
        &self,
        config: &(dyn Any + Send + Sync),
    ) -> Result<Option<EncodedConfig>, KernelError> {
        match self.persistence() {
            None => Ok(None),
            Some(persistence) => {
                let value = (persistence.encode_config)(config)?;
                Ok(Some((persistence, value)))
            }
        }
    }
}

fn foreign() -> KernelError {
    error(
        ErrorCode::InvalidProfile,
        "the attached store encodes a different config type",
    )
}

/// Converts a typed profile to its value form, running each entry's
/// caller-authored `Serialize` behind containment.
fn value_profile<C: LaneConfig + serde::Serialize>(
    profile: &Profile<C>,
) -> Result<Profile<serde_json::Value>, KernelError> {
    let mut entries = Vec::with_capacity(profile.entries.len());
    for entry in &profile.entries {
        entries.push(jinnd_api::ProfileEntry {
            id: entry.id.clone(),
            plugin: entry.plugin.clone(),
            config: contained_value(&format!("entry {:?}", entry.id.0), &entry.config)?,
            disabled: entry.disabled,
            parent: entry.parent.clone(),
            isolation: entry.isolation.clone(),
        });
    }
    Ok(Profile { entries })
}

/// Runs one config's caller-authored `Serialize` behind panic containment: a
/// failing or panicking serializer is an honest recorded error, never an
/// escape across the kernel boundary (R11) — and never inside the persist
/// permit's span (R1).
fn contained_value<C: serde::Serialize>(
    subject: &str,
    config: &C,
) -> Result<serde_json::Value, KernelError> {
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        serde_json::to_value(config)
    }));
    match outcome {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(cause)) => Err(error(
            ErrorCode::InvalidProfile,
            &format!("the config of {subject} does not serialize: {cause}"),
        )),
        Err(panic) => Err(error(
            ErrorCode::InvalidProfile,
            &format!(
                "the config of {subject} panicked while serializing: {}",
                panic
                    .downcast_ref::<&str>()
                    .copied()
                    .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                    .unwrap_or("non-string panic payload")
            ),
        )),
    }
}
