//! The `jinn:profile` reads (0.2.0, M2-K8, harness #25; split from
//! `profile_cap.rs` by responsibility, R10): `entry(id)` and `document()`
//! answer the document of record's authority fields for the entries the
//! caller's `entry-ids` scope admits — read-only viewers hold
//! `ops: [entry, document]` — each a ledgered contract call; a read
//! outside the scope is a ledgered grant refusal, an error on the wire.

use jinnd_api::{ErrorCode, KernelError, LedgerEventKind, ProfileEntry, RefusalReason};
use jinnd_wasm::{GrantScope, PROFILE_CONTRACT, PeerId};

use super::profile_cap::HostProfile;
use super::wire::{Reader, json};
use crate::support::error;

impl HostProfile {
    /// The entries the caller's scope admits, from the document of record.
    fn admitted_entries(&self, caller: PeerId) -> Vec<ProfileEntry<serde_json::Value>> {
        let scope = self.callers.policy(caller);
        self.loader
            .persisted::<serde_json::Value>()
            .map(|profile| {
                profile
                    .entries
                    .into_iter()
                    .filter(|entry| {
                        scope
                            .as_ref()
                            .is_some_and(|scope| scope.admits_entry(&entry.id.0))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// One read's scope refusal (M2-K8 #25): ledgered, and an error on
    /// the wire — a read has no `refused` outcome to answer.
    fn refuse_read(&self, caller: PeerId, what: &str) -> KernelError {
        let (fiber, by) = self.callers.attribution(caller);
        let reason = format!("grant refused: {PROFILE_CONTRACT} scope does not admit {what}");
        self.ledger.record(
            LedgerEventKind::GrantRefused {
                contract: PROFILE_CONTRACT.to_owned(),
                reason: RefusalReason::ScopeMismatch,
                detail: Some(reason.clone()),
            },
            by,
            fiber,
        );
        error(ErrorCode::EffectFailed, reason)
    }

    /// `entry(id)` (0.2.0, #25): the entry's authority fields, or the JSON
    /// `null` for an unknown entry; an entry outside the scope refuses.
    pub(super) fn entry(&self, caller: PeerId, payload: &[u8]) -> Result<Vec<u8>, KernelError> {
        let id = Reader::new(payload, "profile entry").text()?;
        if !self
            .callers
            .policy(caller)
            .is_some_and(|scope| scope.admits_entry(&id))
        {
            return Err(self.refuse_read(caller, &format!("entry {id:?}")));
        }
        let found = self
            .admitted_entries(caller)
            .into_iter()
            .find(|entry| entry.id.0 == id)
            .map_or(serde_json::Value::Null, |entry| entry_record(&entry));
        Ok(json(&found))
    }

    /// `document()` (0.2.0, #25): the document of record's entries the
    /// scope admits; a grant admitting nothing refuses.
    pub(super) fn document(&self, caller: PeerId) -> Result<Vec<u8>, KernelError> {
        let admits_something = matches!(
            self.callers.policy(caller),
            Some(GrantScope::Entries(ids)) if !ids.is_empty()
        );
        if !admits_something {
            return Err(self.refuse_read(caller, "the document"));
        }
        let entries: Vec<serde_json::Value> = self
            .admitted_entries(caller)
            .iter()
            .map(entry_record)
            .collect();
        Ok(json(&serde_json::json!({ "entries": entries })))
    }
}

/// The bundle's `entry` record (0.2.0, #25): the document's authority
/// fields — identity, pinned package, grants as written — and the config.
pub(super) fn entry_record(entry: &ProfileEntry<serde_json::Value>) -> serde_json::Value {
    serde_json::json!({
        "id": entry.id.0,
        "package": entry.plugin.package,
        "version": entry.plugin.version,
        "hash": entry.plugin.artifact_hash,
        "grants": entry.config.get("grants").cloned().unwrap_or(serde_json::json!([])),
        "config": entry.config,
        "disabled": entry.disabled,
        "parent": entry.parent.as_ref().map(|parent| parent.0.clone()),
    })
}
