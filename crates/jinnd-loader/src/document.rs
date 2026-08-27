//! The serde-typed profile document (R3): a tree of plugin entries with stable
//! ids, per-entry config payloads, and realm directives — local `#entryId`,
//! shared `@label`, root `/`.
//!
//! Config is data, mechanically (R9): the model holds `serde_json::Value`
//! payloads and nothing here evaluates anything. A malformed directive is a
//! contained per-entry fault (R11): good entries load, bad entries surface
//! recorded errors. What the kernel does not understand — unknown fields,
//! undecodable entries — keeps its source *bytes* (see [`crate::raw`]) and
//! re-emits verbatim: "verbatim" means bytes, not value-equivalence (v0.1
//! bounds).

use std::collections::BTreeMap;

use jinnd_api::{
    EntryFault, EntryId, ErrorCode, IsolationBinding, KernelError, PluginRef, Profile,
    ProfileEntry, Realm,
};

pub use crate::raw::RawEntry;

/// One profile entry as persisted.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DocumentEntry {
    pub id: String,
    pub package: String,
    pub version: String,
    pub hash: String,
    pub config: serde_json::Value,
    pub disabled: bool,
    pub parent: Option<String>,
    pub isolate: BTreeMap<String, String>,
    /// Fields this kernel does not understand, as `(key, verbatim value
    /// bytes)` in source order, re-emitted unchanged on every save (v0.1: no
    /// destructive compaction — a write-back never erases, and never
    /// rewrites, what it did not understand).
    pub extra: Vec<(String, String)>,
}

/// An ordered profile document.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Document {
    pub entries: Vec<DocumentEntry>,
    /// Entries that did not decode, contained per entry (R11) and preserved
    /// verbatim for write-back.
    pub raw: Vec<RawEntry>,
}

impl Document {
    /// Parses a persisted document. Decoding is lenient per entry (R11): a
    /// malformed entry becomes one preserved [`RawEntry`] — reported by
    /// [`Document::resolve`] — and its siblings still load.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] only when the text is not a document at
    /// all: not JSON, or without an `entries` array.
    pub fn parse(text: &str) -> Result<Self, KernelError> {
        let shape = |message: String| KernelError {
            code: ErrorCode::InvalidProfile,
            message,
            fiber: None,
        };
        let value: serde_json::Value = serde_json::from_str(text)
            .map_err(|error| shape(format!("the profile document does not parse: {error}")))?;
        if value
            .get("entries")
            .and_then(serde_json::Value::as_array)
            .is_none()
        {
            return Err(shape(
                "the profile document has no `entries` array".to_owned(),
            ));
        }
        // The shell re-reads the same text with entry bytes intact: unknown
        // fields and undecodable entries keep their source bytes, so a later
        // save re-emits them verbatim rather than value-normalized.
        let shell: crate::raw::RawShell = serde_json::from_str(text)
            .map_err(|error| shape(format!("the profile document does not parse: {error}")))?;
        let mut entries = Vec::with_capacity(shell.entries.len());
        let mut raw = Vec::new();
        for (index, item) in shell.entries.iter().enumerate() {
            match crate::raw::decode_entry(item.get()) {
                Ok(entry) => entries.push(entry),
                Err(reason) => raw.push(RawEntry {
                    index,
                    text: item.get().to_owned(),
                    error: shape(format!("the entry does not decode: {reason}")),
                }),
            }
        }
        Ok(Self { entries, raw })
    }

    /// Renders the document for persistence: known fields pretty-printed,
    /// unknown fields and preserved raw entries re-emitted byte-for-byte at
    /// their recorded positions.
    #[must_use]
    pub fn render(&self) -> String {
        let mut items: Vec<String> = self.entries.iter().map(crate::raw::render_entry).collect();
        // Ascending recorded indexes: each insert restores one original slot.
        for raw in &self.raw {
            items.insert(raw.index.min(items.len()), raw.text.clone());
        }
        let mut text = String::from("{\n  \"entries\": [");
        for (position, item) in items.iter().enumerate() {
            if position > 0 {
                text.push(',');
            }
            text.push_str("\n    ");
            text.push_str(item);
        }
        if !items.is_empty() {
            text.push_str("\n  ");
        }
        text.push_str("]\n}");
        text
    }

    /// Resolves the document into the typed profile plus per-entry faults —
    /// entries that did not decode and entries whose directives do not parse
    /// (R11: the rest still load).
    #[must_use]
    pub fn resolve(&self) -> (Profile<serde_json::Value>, Vec<EntryFault>) {
        let mut entries = Vec::with_capacity(self.entries.len());
        let mut faults: Vec<EntryFault> = self
            .raw
            .iter()
            .map(|raw| EntryFault {
                entry: raw.entry_id(),
                error: raw.error.clone(),
            })
            .collect();
        for entry in &self.entries {
            match resolve_entry(entry) {
                Ok(resolved) => entries.push(resolved),
                Err(error) => faults.push(EntryFault {
                    entry: EntryId(entry.id.clone()),
                    error,
                }),
            }
        }
        (Profile { entries }, faults)
    }

    /// Rebuilds the persistable document from a typed profile, unparsing realm
    /// bindings back into their directive syntax. Starts from an empty
    /// baseline: no raw entries, no unknown fields. The save path uses
    /// [`Document::merge_profile`] instead, which preserves both.
    #[must_use]
    pub fn from_profile(profile: &Profile<serde_json::Value>) -> Self {
        Self::merge_profile(profile, &Self::default())
    }

    /// Renders a value-form profile over `baseline`, the committed document
    /// being replaced: the kernel-owned raw-merge (M1-P6c). Mechanical by
    /// type — configs arrive as plain `serde_json::Value`, so no
    /// caller-authored code can run here, and the persist permit may span
    /// this safely (R1, PLA-270). Unknown fields of a baseline entry with
    /// the same id are carried over verbatim, and the baseline's raw entries
    /// re-enter at their recorded positions — a save never erases what it
    /// did not understand (LAW §3; v0.1 bounds).
    #[must_use]
    pub fn merge_profile(profile: &Profile<serde_json::Value>, baseline: &Document) -> Self {
        let entries = profile
            .entries
            .iter()
            .map(|entry| DocumentEntry {
                id: entry.id.0.clone(),
                package: entry.plugin.package.clone(),
                version: entry.plugin.version.clone(),
                hash: entry.plugin.artifact_hash.clone(),
                config: entry.config.clone(),
                disabled: entry.disabled,
                parent: entry.parent.as_ref().map(|parent| parent.0.clone()),
                isolate: entry
                    .isolation
                    .iter()
                    .map(|binding| (binding.service.clone(), directive(&binding.realm)))
                    .collect(),
                extra: baseline
                    .entries
                    .iter()
                    .find(|persisted| persisted.id == entry.id.0)
                    .map(|persisted| persisted.extra.clone())
                    .unwrap_or_default(),
            })
            .collect();
        Self {
            entries,
            raw: baseline.raw.clone(),
        }
    }

    /// Rewrites one persisted entry — a new config value and/or the disabled
    /// flag — leaving every other byte of the document as it was: the
    /// runtime-led amendment's save path, mechanical by construction
    /// (M1-P6c; R1). Sibling amendments already saved stay untouched.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] when the document holds no such entry:
    /// the disk may never silently drift from the committed view.
    pub(crate) fn amended(
        &self,
        entry: &str,
        config: Option<serde_json::Value>,
        disabled: Option<bool>,
    ) -> Result<Self, KernelError> {
        let mut document = self.clone();
        let Some(persisted) = document
            .entries
            .iter_mut()
            .find(|candidate| candidate.id == entry)
        else {
            return Err(KernelError {
                code: ErrorCode::InvalidProfile,
                message: format!("entry {entry:?} is not in the persisted document"),
                fiber: None,
            });
        };
        if let Some(config) = config {
            persisted.config = config;
        }
        if let Some(disabled) = disabled {
            persisted.disabled = disabled;
        }
        Ok(document)
    }
}

fn resolve_entry(entry: &DocumentEntry) -> Result<ProfileEntry<serde_json::Value>, KernelError> {
    let mut isolation = Vec::with_capacity(entry.isolate.len());
    for (service, directive) in &entry.isolate {
        isolation.push(IsolationBinding {
            service: service.clone(),
            realm: realm(directive)?,
        });
    }
    Ok(ProfileEntry {
        id: EntryId(entry.id.clone()),
        plugin: PluginRef {
            package: entry.package.clone(),
            version: entry.version.clone(),
            artifact_hash: entry.hash.clone(),
        },
        config: entry.config.clone(),
        disabled: entry.disabled,
        parent: entry.parent.clone().map(EntryId),
        isolation,
    })
}

/// Parses one realm directive: `#entryId` local, `@label` shared, `/` root.
fn realm(directive: &str) -> Result<Realm, KernelError> {
    match directive.split_at_checked(1) {
        Some(("#", entry)) if !entry.is_empty() => Ok(Realm::Local(EntryId(entry.to_owned()))),
        Some(("@", label)) if !label.is_empty() => Ok(Realm::Shared(label.to_owned())),
        _ if directive == "/" => Ok(Realm::Root),
        _ => Err(KernelError {
            code: ErrorCode::InvalidProfile,
            message: format!(
                "realm directive {directive:?} is neither `#entryId`, `@label`, nor `/`"
            ),
            fiber: None,
        }),
    }
}

/// Unparses one realm back into its directive.
fn directive(realm: &Realm) -> String {
    match realm {
        Realm::Root => "/".to_owned(),
        Realm::Local(entry) => format!("#{}", entry.0),
        Realm::Shared(label) => format!("@{label}"),
    }
}
