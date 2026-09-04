//! The `jinn:profile-admin` wire (split from `writes` by the R10 per-file
//! cap): u32-LE length-prefixed UTF-8 segments in the M2-K7 form, decoded
//! into one typed write; a payload that does not decode is `malformed`.

use jinnd_api::{EntryId, PluginRef, ProfileEntry, ProfileWrite};

use super::super::wire::Reader;
use super::writes::{Change, Refusal, Write};

/// Decodes one write off the wire: u32-LE length-prefixed UTF-8 segments
/// (the M2-K7 form) — `add-entry`: the entry record JSON; `remove-entry`:
/// id; `set-disabled`: id, `true`/`false`; `set-grants`: id, grants JSON;
/// `swap-plugin`: id, package, version, hash.
pub(super) fn decode(operation: &str, payload: &[u8]) -> Result<Write, Refusal> {
    let mut reader = Reader::new(payload, "profile-admin");
    let mut segment = || {
        reader
            .text()
            .map_err(|error| Refusal::malformed(&error.message))
    };
    let (entry, write, change) = match operation {
        "add-entry" => {
            let record = decode_record(&segment()?)?;
            (record.id.clone(), ProfileWrite::Add, Change::Add(record))
        }
        "remove-entry" => (EntryId(segment()?), ProfileWrite::Remove, Change::Remove),
        "set-disabled" => {
            let id = EntryId(segment()?);
            let disabled = match segment()?.as_str() {
                "true" => true,
                "false" => false,
                other => {
                    return Err(Refusal::malformed(&format!(
                        "disabled must be true or false, wrote {other:?}"
                    )));
                }
            };
            (id, ProfileWrite::SetDisabled, Change::SetDisabled(disabled))
        }
        "set-grants" => {
            let id = EntryId(segment()?);
            let grants: serde_json::Value = serde_json::from_str(&segment()?)
                .map_err(|bad| Refusal::malformed(&format!("grants do not parse: {bad}")))?;
            (id, ProfileWrite::SetGrants, Change::SetGrants(grants))
        }
        "swap-plugin" => {
            let id = EntryId(segment()?);
            let plugin = PluginRef {
                package: segment()?,
                version: segment()?,
                artifact_hash: segment()?,
            };
            (id, ProfileWrite::SwapPlugin, Change::Swap(plugin))
        }
        other => return Err(Refusal::malformed(&format!("no such write {other:?}"))),
    };
    Ok(Write {
        entry,
        write,
        change,
    })
}

/// The 0.2.0 `entry` record as a typed entry: `config` an object, `grants`
/// (when written) mirrored into `config.grants`, `disabled` and `parent`
/// optional. Isolation directives are not among this card's writes.
fn decode_record(text: &str) -> Result<ProfileEntry<serde_json::Value>, Refusal> {
    let record: serde_json::Value = serde_json::from_str(text)
        .map_err(|bad| Refusal::malformed(&format!("the entry record does not parse: {bad}")))?;
    let field = |name: &str| {
        record
            .get(name)
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned)
            .ok_or_else(|| Refusal::malformed(&format!("the entry record has no string {name:?}")))
    };
    let mut config = record
        .get("config")
        .cloned()
        .unwrap_or(serde_json::json!({}));
    if !config.is_object() {
        return Err(Refusal::malformed("config is not an object"));
    }
    if let Some(grants) = record.get("grants") {
        config["grants"] = grants.clone();
    }
    let parent = match record.get("parent") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(parent)) => Some(EntryId(parent.clone())),
        Some(_) => return Err(Refusal::malformed("parent is not an entry id")),
    };
    Ok(ProfileEntry {
        id: EntryId(field("id")?),
        plugin: PluginRef {
            package: field("package")?,
            version: field("version").unwrap_or_default(),
            artifact_hash: field("hash")?,
        },
        config,
        disabled: record
            .get("disabled")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        parent,
        isolation: Vec::new(),
    })
}
