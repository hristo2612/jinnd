//! Byte-preserving document decoding (v0.1 bounds: a write-back never
//! rewrites what it did not understand — "verbatim" means bytes, not
//! value-equivalence).
//!
//! Entries arrive as raw JSON slices; known fields decode out of them and
//! everything else keeps its source bytes: unknown fields as verbatim
//! `key:value` units, undecodable entries whole. Nothing here evaluates
//! anything (R9); a malformed entry is a contained per-entry fault (R11).

use serde::de::{Deserialize, Deserializer, MapAccess, Visitor};
use serde_json::value::RawValue;

use jinnd_api::{EntryId, KernelError};

use crate::document::DocumentEntry;

/// One entry that did not decode, preserved verbatim so a later write-back
/// re-emits it unchanged (v0.1: no destructive compaction — a save never
/// erases what it did not understand).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawEntry {
    /// The entry's position in the persisted `entries` array.
    pub index: usize,
    /// The verbatim source bytes [`crate::Document::render`] re-emits.
    pub text: String,
    /// Why the entry did not decode.
    pub error: KernelError,
}

impl RawEntry {
    /// The faulted entry's best identity: its `id` when one is legible, its
    /// position otherwise.
    #[must_use]
    pub fn entry_id(&self) -> EntryId {
        let value: Option<serde_json::Value> = serde_json::from_str(&self.text).ok();
        match value
            .as_ref()
            .and_then(|value| value.get("id"))
            .and_then(serde_json::Value::as_str)
        {
            Some(id) => EntryId(id.to_owned()),
            None => EntryId(format!("entries[{}]", self.index)),
        }
    }
}

/// The document shell: entry slices with their source bytes intact.
#[derive(serde::Deserialize)]
pub(crate) struct RawShell {
    pub(crate) entries: Vec<Box<RawValue>>,
}

/// One JSON object as ordered `(key, raw value)` pairs, bytes intact.
pub(crate) struct RawPairs(Vec<(String, Box<RawValue>)>);

impl<'de> Deserialize<'de> for RawPairs {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct PairVisitor;

        impl<'de> Visitor<'de> for PairVisitor {
            type Value = RawPairs;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
                let mut pairs = Vec::new();
                while let Some(pair) = access.next_entry::<String, Box<RawValue>>()? {
                    pairs.push(pair);
                }
                Ok(RawPairs(pairs))
            }
        }

        deserializer.deserialize_map(PairVisitor)
    }
}

/// Decodes one entry from its source bytes: known fields become typed values,
/// unknown fields keep their verbatim value bytes in source order.
pub(crate) fn decode_entry(text: &str) -> Result<DocumentEntry, String> {
    let pairs: RawPairs = serde_json::from_str(text).map_err(|error| error.to_string())?;
    let mut entry = DocumentEntry::default();
    let mut id = None;
    let mut package = None;
    for (key, value) in pairs.0 {
        let raw = value.get();
        match key.as_str() {
            "id" => id = Some(field("id", raw)?),
            "package" => package = Some(field("package", raw)?),
            "version" => entry.version = field("version", raw)?,
            "hash" => entry.hash = field("hash", raw)?,
            "config" => entry.config = field("config", raw)?,
            "disabled" => entry.disabled = field("disabled", raw)?,
            "parent" => entry.parent = field("parent", raw)?,
            "isolate" => entry.isolate = field("isolate", raw)?,
            _ => entry.extra.push((key, raw.to_owned())),
        }
    }
    entry.id = id.ok_or("missing field `id`")?;
    entry.package = package.ok_or("missing field `package`")?;
    Ok(entry)
}

fn field<T: serde::de::DeserializeOwned>(name: &str, raw: &str) -> Result<T, String> {
    serde_json::from_str(raw).map_err(|error| format!("field `{name}`: {error}"))
}

/// Renders one decodable entry: known fields pretty-printed in declaration
/// order, then each unknown field spliced in as its verbatim `"key":bytes`
/// unit — the value bytes are exactly the source's (v0.1: a save never
/// rewrites what it did not understand).
pub(crate) fn render_entry(entry: &DocumentEntry) -> String {
    let mut known = serde_json::Map::new();
    known.insert("id".to_owned(), serde_json::json!(entry.id));
    known.insert("package".to_owned(), serde_json::json!(entry.package));
    if !entry.version.is_empty() {
        known.insert("version".to_owned(), serde_json::json!(entry.version));
    }
    if !entry.hash.is_empty() {
        known.insert("hash".to_owned(), serde_json::json!(entry.hash));
    }
    known.insert("config".to_owned(), entry.config.clone());
    if entry.disabled {
        known.insert("disabled".to_owned(), serde_json::json!(true));
    }
    if let Some(parent) = &entry.parent {
        known.insert("parent".to_owned(), serde_json::json!(parent));
    }
    if !entry.isolate.is_empty() {
        known.insert("isolate".to_owned(), serde_json::json!(entry.isolate));
    }
    // A map of plain data serializes; there is no failing path.
    let text = serde_json::to_string_pretty(&serde_json::Value::Object(known)).unwrap_or_default();
    if entry.extra.is_empty() {
        return text;
    }
    let mut spliced = text
        .strip_suffix("\n}")
        .map_or_else(|| text.clone(), str::to_owned);
    for (key, value) in &entry.extra {
        spliced.push_str(",\n  ");
        spliced.push_str(&serde_json::Value::String(key.clone()).to_string());
        spliced.push(':');
        spliced.push_str(value);
    }
    spliced.push_str("\n}");
    spliced
}
