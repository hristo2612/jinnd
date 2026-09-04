//! The pure half of `jinn:profile-admin` (split by responsibility, R10):
//! the wire decoding of the five writes, the caller-relative authorization
//! (self and ancestors), and every check the daemon can decide BEFORE the
//! loader is offered the change — a malformed record, a duplicate id, an
//! entry with children, a raw entry, a shared-package re-pin — so that a
//! refusal writes nothing anywhere. Nothing here touches kernel state.

use jinnd_api::{EntryId, PluginRef, Profile, ProfileEntry, ProfileWrite};
use jinnd_loader::Loader;

use super::super::profile_cap::patch::validate;
use super::super::profile_read::entry_record;
use super::super::wire::Reader;

/// The refusal classes on the wire (one byte after tag 1; R3).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Class {
    /// Outside the caller's scope, or the caller's own entry / an ancestor.
    Unauthorized = 1,
    /// A record that does not decode, an id or parent that is wrong, a
    /// config that is not an object, grants that would refuse, no pin.
    Malformed = 2,
    /// Retryable: an operation in flight, a fiber not at rest, a pin held
    /// by a sibling under another hash.
    Conflict = 3,
    /// No inverse can be recorded as one write: children, a raw entry.
    Irreversible = 4,
}

impl Class {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Unauthorized => "unauthorized",
            Self::Malformed => "malformed",
            Self::Conflict => "conflict",
            Self::Irreversible => "irreversible",
        }
    }
}

pub(super) struct Refusal {
    pub(super) class: Class,
    pub(super) reason: String,
}

impl Refusal {
    fn new(class: Class, reason: &str) -> Self {
        Self {
            class,
            reason: reason.to_owned(),
        }
    }
    fn malformed(reason: &str) -> Self {
        Self::new(Class::Malformed, reason)
    }
    pub(super) fn conflict(reason: &str) -> Self {
        Self::new(Class::Conflict, reason)
    }
}

/// What one write changes, decoded.
pub(super) enum Change {
    Add(ProfileEntry<serde_json::Value>),
    Remove,
    SetDisabled(bool),
    SetGrants(serde_json::Value),
    Swap(PluginRef),
}

pub(super) struct Write {
    pub(super) entry: EntryId,
    pub(super) write: ProfileWrite,
    pub(super) change: Change,
}

impl Write {
    /// The parent an `add-entry` names — inside the scope too.
    pub(super) fn parent(&self) -> Option<&EntryId> {
        match &self.change {
            Change::Add(record) => record.parent.as_ref(),
            _ => None,
        }
    }
}

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

fn find<'a>(
    profile: &'a Profile<serde_json::Value>,
    id: &EntryId,
) -> Option<&'a ProfileEntry<serde_json::Value>> {
    profile.entries.iter().find(|entry| entry.id == *id)
}

/// The caller may not administer ITSELF or an ANCESTOR (Law 1: self-widening
/// is escalation; self-removal awaits its own teardown from inside its own
/// call — the K7 nested-dispatch class, extended to the cascade). A kernel
/// peer has no entry and no ancestors.
pub(super) fn authorize_target(
    profile: &Profile<serde_json::Value>,
    request: &Write,
    caller: Option<&EntryId>,
) -> Result<(), Refusal> {
    let Some(caller) = caller else {
        return Ok(());
    };
    let mut cursor = Some(caller);
    let mut hops = 0;
    while let Some(id) = cursor {
        if *id == request.entry || request.parent() == Some(id) {
            return Err(Refusal::new(
                Class::Unauthorized,
                "an entry cannot administer itself or an ancestor",
            ));
        }
        hops += 1;
        if hops > profile.entries.len() {
            break;
        }
        cursor = find(profile, id).and_then(|entry| entry.parent.as_ref());
    }
    Ok(())
}

/// Every refusal decidable from the document of record before the loader
/// moves; on success, the entry's record BEFORE the write (`None` on add).
pub(super) fn check(
    profile: &Profile<serde_json::Value>,
    loader: &Loader,
    request: &Write,
) -> Result<Option<serde_json::Value>, Refusal> {
    let id = &request.entry;
    if loader.raw_entry_ids().contains(id) {
        return Err(Refusal::new(
            Class::Irreversible,
            "the entry is preserved raw: its record cannot be captured as a prior",
        ));
    }
    let existing = find(profile, id);
    let lane_for = |plugin: &PluginRef| -> Result<(), Refusal> {
        if plugin.artifact_hash.is_empty() {
            return Err(Refusal::malformed(
                "the entry names no artifact pin (Law 5)",
            ));
        }
        if !loader.has_lane::<serde_json::Value>(&plugin.package) {
            return Err(Refusal::malformed(&format!(
                "package {:?} has no admitted artifact under any pin (Law 5): a \
                 document-led reconcile admits it first",
                plugin.package
            )));
        }
        Ok(())
    };
    match &request.change {
        Change::Add(record) => {
            if existing.is_some() {
                return Err(Refusal::malformed("the entry id already exists"));
            }
            if let Some(parent) = &record.parent
                && find(profile, parent).is_none()
            {
                return Err(Refusal::malformed("the parent is not an entry"));
            }
            validate(&record.config, &record.config)
                .map_err(|reason| Refusal::malformed(&reason))?;
            lane_for(&record.plugin)?;
            return Ok(None);
        }
        Change::Remove => {
            if profile
                .entries
                .iter()
                .any(|entry| entry.parent.as_ref() == Some(id))
            {
                return Err(Refusal::new(
                    Class::Irreversible,
                    "the entry has children: the inverse would be a subtree; remove leaves first",
                ));
            }
        }
        Change::SetGrants(grants) => {
            let mut config = existing
                .map(|entry| entry.config.clone())
                .unwrap_or_default();
            config["grants"] = grants.clone();
            validate(&config, &config).map_err(|reason| Refusal::malformed(&reason))?;
        }
        Change::Swap(plugin) => {
            lane_for(plugin)?;
            let held_elsewhere = profile.entries.iter().any(|entry| {
                entry.id != *id
                    && !entry.disabled
                    && entry.plugin.package == plugin.package
                    && entry.plugin.artifact_hash != plugin.artifact_hash
            });
            if held_elsewhere {
                return Err(Refusal::conflict(
                    "another entry holds this package under a different pin (one pin per \
                     package, R9): swap them together or name a distinct package",
                ));
            }
        }
        Change::SetDisabled(_) => {}
    }
    let Some(existing) = existing else {
        return Err(Refusal::malformed(
            "no such entry in the document of record",
        ));
    };
    Ok(Some(entry_record(existing)))
}
