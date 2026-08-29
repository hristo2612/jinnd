//! The `jinn:profile` provider (M2-K7, harness #21; 0.2.0 M2-K8, harness
//! #25/#26; contract bundle `contracts/jinn-profile`, constitution 04): a
//! profile patch as OPERATOR INTENT. `patch-entry(id, merge-patch)` is
//! applied BY THE LOADER — the runtime-led amendment: validated, written
//! back atomically (stage + fsync + rename), the patched fiber's restart
//! SCHEDULED — and RECORDED as a typed `ProfilePatched` ledger event with
//! no fs inverse and no fiber journal entry: the profile's history is the
//! ledger's, not a fiber's contribution, so disposing the editor never
//! touches the document. The answer is `accepted(seq)` — the receipt of
//! that record — as soon as the document committed: the restart is never
//! awaited inside the caller's host call (#26, the Algorithm-5 deferred
//! amendment), so a provider may patch its own consumer from a handler.
//! `entry(id)` / `document()` answer the document's authority fields
//! (#25) for viewers holding a read-only grant. Authority is the caller's
//! `entry-ids` scope, fail-closed (a bare grant reads and patches nothing;
//! `*` only when written); every refusal is on the record.

use std::sync::Arc;

use jinnd_api::{EntryId, KernelError, KernelFuture, LedgerEventKind, RefusalReason};
use jinnd_ledger::Ledger;
use jinnd_loader::Loader;
use jinnd_wasm::{Broker, PROFILE_CONTRACT, Peer, PeerId, grant_refusals};

use super::storage;
use super::wire::{Callers, Reader, unknown};
use crate::seat::seat_config;
use crate::support::{SharedFibers, sync_transitions};

/// Answer tag: refused; the reason's UTF-8 follows.
const TAG_REFUSED: u8 = 1;
/// Answer tag (0.2.0): accepted — the document committed and the restart
/// is scheduled; the `ProfilePatched` receipt's u64-LE sequence follows.
const TAG_ACCEPTED: u8 = 2;

pub(crate) struct HostProfile {
    pub(super) loader: Arc<Loader>,
    pub(super) ledger: Ledger,
    fibers: SharedFibers,
    pub(super) callers: Callers,
}

/// RFC 7396 merge-patch: an object patch merges key by key (`null`
/// removes), anything else replaces the target whole.
pub(crate) fn merge_patch(target: &mut serde_json::Value, patch: &serde_json::Value) {
    let serde_json::Value::Object(fields) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = serde_json::Value::Object(serde_json::Map::new());
    }
    if let serde_json::Value::Object(existing) = target {
        for (key, value) in fields {
            if value.is_null() {
                existing.remove(key);
            } else {
                merge_patch(
                    existing
                        .entry(key.clone())
                        .or_insert(serde_json::Value::Null),
                    value,
                );
            }
        }
    }
}

/// The profile schema the daemon can decide before committing (04): a
/// config is an object whose `grants` read as grants and would ADMIT at
/// activation — a patch that would only fault the entry is refused whole,
/// nothing written.
fn validate(config: &serde_json::Value) -> Result<(), String> {
    if !config.is_object() {
        return Err("the patched config is not an object".to_owned());
    }
    let seat = seat_config(config);
    if let Some(fault) = seat.faults.first() {
        return Err(format!("grant entry refused: {fault}"));
    }
    if let Some(refused) = grant_refusals(&seat.grants).first() {
        return Err(refused.message.clone());
    }
    Ok(())
}

impl HostProfile {
    /// Registers the provider as a broker peer holding and providing the
    /// contract (providing is authority).
    ///
    /// # Errors
    ///
    /// The broker's refusal of the provision.
    pub(crate) fn register(
        broker: &Arc<Broker>,
        loader: Arc<Loader>,
        ledger: Ledger,
        fibers: SharedFibers,
    ) -> Result<(), KernelError> {
        let peer = broker.register_peer(None);
        broker.grant(peer, PROFILE_CONTRACT);
        let provider = Arc::new(Self {
            loader,
            ledger,
            fibers,
            callers: Callers::new(broker, PROFILE_CONTRACT),
        });
        broker.provide(peer, PROFILE_CONTRACT, Arc::new(ProfilePeer(provider)))
    }

    /// One patch: authorize against the caller's scope (a ledgered grant
    /// refusal otherwise), merge onto the committed config, validate, then
    /// hand the loader the amendment — it persists atomically and restarts
    /// exactly the patched fiber. Every refusal answers as the bundle's
    /// `refused(reason)` on the wire, on the record.
    async fn patch_entry(&self, caller: PeerId, payload: Vec<u8>) -> Result<Vec<u8>, KernelError> {
        let (fiber, by) = self.callers.attribution(caller);
        let mut reader = Reader::new(&payload, "profile patch-entry");
        let id = reader.text()?;
        let entry = EntryId(id.clone());
        let allowed = self
            .callers
            .policy(caller)
            .is_some_and(|scope| scope.admits_entry(&id));
        if !allowed {
            let reason =
                format!("grant refused: {PROFILE_CONTRACT} scope does not admit entry {id:?}");
            self.ledger.record(
                LedgerEventKind::GrantRefused {
                    contract: PROFILE_CONTRACT.to_owned(),
                    reason: RefusalReason::ScopeMismatch,
                    detail: Some(reason.clone()),
                },
                by,
                fiber,
            );
            return Ok(refused_wire(&reason));
        }
        // An entry patching itself would await its own restart from
        // inside its own host call (the nested-dispatch class): refused.
        if by.as_ref() == Some(&entry) {
            return Ok(self.refuse(&entry, by, fiber, "an entry cannot patch itself"));
        }
        let patch: serde_json::Value = match serde_json::from_slice(reader.rest()) {
            Ok(patch) => patch,
            Err(bad) => {
                return Ok(self.refuse(
                    &entry,
                    by,
                    fiber,
                    &format!("merge-patch does not parse: {bad}"),
                ));
            }
        };
        let Some(mut config) = self
            .loader
            .persisted::<serde_json::Value>()
            .and_then(|profile| {
                profile
                    .entries
                    .into_iter()
                    .find(|candidate| candidate.id == entry)
            })
            .map(|candidate| candidate.config)
        else {
            return Ok(self.refuse(&entry, by, fiber, "no such entry in the document of record"));
        };
        merge_patch(&mut config, &patch);
        if let Err(reason) = validate(&config) {
            return Ok(self.refuse(&entry, by, fiber, &reason));
        }
        // Deferred amendment (M2-K8 #26): both views commit and the
        // restart is stated; nothing here awaits the patched fiber.
        if let Err(refused) = self.loader.update_entry_deferred(&entry, config).await {
            return Ok(self.refuse(&entry, by, fiber, &refused.message));
        }
        let by_name = by
            .as_ref()
            .map_or_else(|| format!("peer:{caller}"), |by| by.0.clone());
        let receipt = self
            .ledger
            .append(
                LedgerEventKind::ProfilePatched {
                    entry: entry.clone(),
                    by: by_name,
                },
                by,
                fiber,
            )
            .await
            .map_err(storage)?;
        // The restart's outcome lands on the ledger when it settles — from
        // a task of its own, never from inside this host call (R1).
        let (loader, fibers, ledger) = (
            Arc::clone(&self.loader),
            Arc::clone(&self.fibers),
            self.ledger.clone(),
        );
        tokio::spawn(async move {
            loader.quiesce_entry(&entry).await;
            sync_transitions(&fibers, &ledger);
        });
        let mut wire = vec![TAG_ACCEPTED];
        wire.extend(receipt.sequence.to_le_bytes());
        Ok(wire)
    }

    /// A refusal on the wire AND on the record (Law 2: a refused patch is
    /// history too), attributed to the editor. The scope refusal is
    /// recorded as the grant refusal it is, in `patch_entry`.
    fn refuse(
        &self,
        entry: &EntryId,
        by: Option<EntryId>,
        fiber: Option<jinnd_api::FiberId>,
        reason: &str,
    ) -> Vec<u8> {
        self.ledger.record(
            LedgerEventKind::AmendmentRefused {
                detail: format!("patch-entry {:?} refused: {reason}", entry.0),
            },
            by,
            fiber,
        );
        refused_wire(reason)
    }
}

/// The bundle's `refused(reason)` outcome as the wire carries it: every
/// refusal, the scope's included, is this answer — never an outer error.
fn refused_wire(reason: &str) -> Vec<u8> {
    let mut wire = vec![TAG_REFUSED];
    wire.extend(reason.as_bytes());
    wire
}

/// The provider's broker face.
struct ProfilePeer(Arc<HostProfile>);

impl Peer for ProfilePeer {
    fn call(
        &self,
        caller: PeerId,
        _contract: &str,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let provider = Arc::clone(&self.0);
        let operation = operation.to_owned();
        Box::pin(async move {
            match operation.as_str() {
                "patch-entry" => provider.patch_entry(caller, payload).await,
                "entry" => provider.entry(caller, &payload),
                "document" => provider.document(caller),
                other => Err(unknown(PROFILE_CONTRACT, other)),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{merge_patch, validate};

    /// RFC 7396: nested merge, null removes, scalars replace, a non-object
    /// patch replaces whole.
    #[test]
    fn merge_patch_follows_rfc_7396() {
        let mut target = serde_json::json!({ "grants": ["jinn:fs"], "data": { "a": 1, "b": 2 } });
        merge_patch(
            &mut target,
            &serde_json::json!({ "data": { "b": null, "c": 3 }, "extra": "x" }),
        );
        assert_eq!(
            target,
            serde_json::json!({ "grants": ["jinn:fs"], "data": { "a": 1, "c": 3 }, "extra": "x" })
        );
        merge_patch(&mut target, &serde_json::json!("plain"));
        assert_eq!(target, serde_json::json!("plain"));
    }

    /// The decidable schema: an object whose grants would admit; a grant
    /// that would refuse at activation refuses the patch whole.
    #[test]
    fn validation_refuses_what_activation_would_refuse() {
        assert!(validate(&serde_json::json!({ "grants": ["jinn:fs"], "data": "noop" })).is_ok());
        assert!(validate(&serde_json::json!("noop")).is_err());
        assert!(validate(&serde_json::json!({ "grants": [7] })).is_err());
        assert!(
            validate(&serde_json::json!({ "grants": [{ "contract": "jinn:fs", "scope": 9 }] }))
                .is_err()
        );
    }
}
