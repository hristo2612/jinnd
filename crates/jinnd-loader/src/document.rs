//! The serde-typed profile document (R3): a tree of plugin entries with stable
//! ids, per-entry config payloads, and realm directives — local `#entryId`,
//! shared `@label`, root `/`.
//!
//! Config is data, mechanically (R9): the model holds `serde_json::Value`
//! payloads and nothing here evaluates anything. A malformed directive is a
//! contained per-entry fault (R11): good entries load, bad entries surface
//! recorded errors.

use std::collections::BTreeMap;

use jinnd_api::{
    EntryFault, EntryId, ErrorCode, IsolationBinding, KernelError, PluginRef, Profile,
    ProfileEntry, Realm,
};
use serde::{Deserialize, Serialize};

/// One profile entry as persisted.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DocumentEntry {
    pub id: String,
    pub package: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hash: String,
    #[serde(default)]
    pub config: serde_json::Value,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub disabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub isolate: BTreeMap<String, String>,
}

/// One entry that did not decode, preserved verbatim so a later write-back
/// re-emits it unchanged (v0.1: no destructive compaction — a save never
/// erases what it did not understand).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawEntry {
    /// The entry's position in the persisted `entries` array.
    pub index: usize,
    /// The verbatim value [`Document::render`] re-emits.
    pub value: serde_json::Value,
    /// Why the entry did not decode.
    pub error: KernelError,
}

impl RawEntry {
    /// The faulted entry's best identity: its `id` when one is legible, its
    /// position otherwise.
    #[must_use]
    pub fn entry_id(&self) -> EntryId {
        match self.value.get("id").and_then(serde_json::Value::as_str) {
            Some(id) => EntryId(id.to_owned()),
            None => EntryId(format!("entries[{}]", self.index)),
        }
    }
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
        let Some(items) = value.get("entries").and_then(serde_json::Value::as_array) else {
            return Err(shape(
                "the profile document has no `entries` array".to_owned(),
            ));
        };
        let mut entries = Vec::with_capacity(items.len());
        let mut raw = Vec::new();
        for (index, item) in items.iter().enumerate() {
            match serde_json::from_value::<DocumentEntry>(item.clone()) {
                Ok(entry) => entries.push(entry),
                Err(error) => raw.push(RawEntry {
                    index,
                    value: item.clone(),
                    error: shape(format!("the entry does not decode: {error}")),
                }),
            }
        }
        Ok(Self { entries, raw })
    }

    /// Renders the document for persistence. Preserved raw entries re-enter at
    /// their recorded positions, verbatim.
    #[must_use]
    pub fn render(&self) -> String {
        // A struct of plain data serializes; there is no failing path.
        let mut items: Vec<serde_json::Value> = self
            .entries
            .iter()
            .map(|entry| serde_json::to_value(entry).unwrap_or_default())
            .collect();
        // Ascending recorded indexes: each insert restores one original slot.
        for raw in &self.raw {
            items.insert(raw.index.min(items.len()), raw.value.clone());
        }
        serde_json::to_string_pretty(&serde_json::json!({ "entries": items })).unwrap_or_default()
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
    /// bindings back into their directive syntax.
    #[must_use]
    pub fn from_profile(profile: &Profile<serde_json::Value>) -> Self {
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
            })
            .collect();
        Self {
            entries,
            raw: Vec::new(),
        }
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
