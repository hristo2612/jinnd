//! The `jinn:keystore` operations and inverse machinery (M2-K8; split from
//! `hostkeystore.rs` by responsibility, R10): the two reads, the two
//! revertible effects in the one order — authorize, capture the prior
//! SEALED, make it durable or refuse on the record, mutate, commit
//! atomically, ledger — and the seams that consume a retained inverse:
//! keyed revert, reclaim, teardown withdrawal.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use jinnd_api::{EffectId, ErrorCode, KernelError, KernelFuture, LedgerEventKind, Witness};

use super::{HostKeystore, Retained, keystore_label, vault};
use crate::broker_state::refusal;
use crate::hostfs::retention::{Header, Prior, Record};
use crate::hostwire::{Reader, encode_handle, put_segment};
use crate::lane::lock;
use crate::peer::PeerId;

fn not_found(key: &str) -> KernelError {
    refusal(
        ErrorCode::NotFound,
        format!("keystore key {key:?}: not found"),
    )
}

fn key_payload(payload: Vec<u8>) -> Result<String, KernelError> {
    String::from_utf8(payload).map_err(|_| {
        refusal(
            ErrorCode::PluginFailed,
            "malformed keystore payload".to_owned(),
        )
    })
}

/// Routes one broker call to its operation (wit/plugin.wit
/// `interface keystore`).
pub(super) async fn dispatch(
    provider: &HostKeystore,
    caller: PeerId,
    operation: &str,
    payload: Vec<u8>,
) -> Result<Vec<u8>, KernelError> {
    match operation {
        "get" => {
            let key = key_payload(payload)?;
            provider.authorized(caller, &key)?;
            let value = lock(&provider.vault).get(&key).map(<[u8]>::to_vec);
            provider.accessed(caller, "get", &key, value.as_deref());
            value.ok_or_else(|| not_found(&key))
        }
        "list" => {
            let prefixes = provider.prefixes(caller).unwrap_or_default();
            let mut wire = Vec::new();
            for name in lock(&provider.vault).names() {
                if prefixes.iter().any(|prefix| name.starts_with(prefix)) {
                    put_segment(&mut wire, name.as_bytes());
                }
            }
            Ok(wire)
        }
        "put" => {
            let mut reader = Reader::new(&payload, "keystore put");
            let key = reader.text()?;
            let value = reader.rest().to_vec();
            provider.authorized(caller, &key)?;
            provider.mutate(caller, "put", &key, Some(value)).await
        }
        "delete" => {
            let key = key_payload(payload)?;
            provider.authorized(caller, &key)?;
            provider.mutate(caller, "delete", &key, None).await
        }
        other => Err(refusal(
            ErrorCode::PluginFailed,
            format!("unknown keystore operation {other:?}"),
        )),
    }
}

impl HostKeystore {
    /// One revertible effect (Law 3, R5): the sealed prior is retained
    /// durably — or the effect is refused on the record — then the map
    /// mutates and the sealed document commits atomically; a failed commit
    /// restores the map and releases the retention. Answers the effect id.
    async fn mutate(
        &self,
        caller: PeerId,
        operation: &str,
        key: &str,
        value: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, KernelError> {
        let _serial = self.serial.lock().await;
        let attribution = self.attribution(caller);
        let prior = {
            let vault = lock(&self.vault);
            match vault.get(key) {
                Some(current) => Prior::Content(vault.seal(current)?),
                None if value.is_none() => return Err(not_found(key)),
                None => Prior::Absent,
            }
        };
        let header = Header {
            label: key.to_owned(),
            key: String::new(),
            owner: attribution.map_or(0, |fiber| fiber.0),
            entry: self.entry_of(caller).unwrap_or_default(),
            operation: operation.to_owned(),
        };
        let id = match self.retain(header, prior.clone()).await {
            Ok(id) => id,
            Err(refused) => {
                let error = refusal(
                    ErrorCode::PluginFailed,
                    format!(
                        "keystore {operation} {key:?} refused: inverse not durable ({})",
                        refused.message
                    ),
                );
                self.sink.append(
                    LedgerEventKind::ErrorRecorded {
                        error: error.clone(),
                    },
                    attribution,
                );
                return Err(error);
            }
        };
        let digest_of = value.clone();
        let (target, sealed) = {
            let mut vault = lock(&self.vault);
            vault.set(key, value);
            vault.sealed()?
        };
        if let Err(refused) = vault::commit(&target, &sealed).await {
            let restored = match &prior {
                Prior::Content(sealed) => Some(lock(&self.vault).unseal(sealed)?),
                _ => None,
            };
            lock(&self.vault).set(key, restored);
            self.release(id).await;
            return Err(refused);
        }
        self.sink.append(
            LedgerEventKind::EffectRegistered {
                label: keystore_label(operation, key, id),
            },
            attribution,
        );
        self.accessed(caller, operation, key, digest_of.as_deref());
        tracing::info!(effect = id, operation, key, "keystore effect registered");
        Ok(encode_handle(id))
    }

    async fn retain(&self, header: Header, prior: Prior) -> Result<u64, KernelError> {
        let id = self.next.fetch_add(1, Ordering::SeqCst);
        let record = Record {
            header: header.clone(),
            prior,
        };
        self.store.persist(id, &record).await?;
        lock(&self.index).insert(
            id,
            Retained {
                header,
                consumed: false,
            },
        );
        Ok(id)
    }

    async fn release(&self, id: u64) {
        lock(&self.index).remove(&id);
        let _ = self.store.reclaim(id).await;
    }

    /// Replays one retained inverse: the key reads as the unsealed prior,
    /// or is absent; the sealed document commits atomically.
    async fn apply_inverse(&self, record: Record) -> Result<(), KernelError> {
        let _serial = self.serial.lock().await;
        let key = record.header.label;
        let (target, sealed) = {
            let mut vault = lock(&self.vault);
            let restored = match record.prior {
                Prior::Absent => None,
                Prior::Content(sealed) => Some(vault.unseal(&sealed)?),
                Prior::Length(_) => {
                    return Err(refusal(
                        ErrorCode::EffectFailed,
                        "a keystore inverse never records a length".to_owned(),
                    ));
                }
            };
            vault.set(&key, restored);
            vault.sealed()?
        };
        vault::commit(&target, &sealed).await
    }

    /// The executable witness: the key now reads as the retained prior.
    fn witness(&self, record: &Record) -> bool {
        let vault = lock(&self.vault);
        match &record.prior {
            Prior::Absent => vault.get(&record.header.label).is_none(),
            Prior::Content(sealed) => vault
                .unseal(sealed)
                .is_ok_and(|prior| vault.get(&record.header.label) == Some(prior.as_slice())),
            Prior::Length(_) => false,
        }
    }

    /// The keyed-revert action for one effect (Law 3): witness and
    /// inverse for the ledger's exactly-once protocol; a consumed effect
    /// answers an inverse that refuses to run again.
    #[must_use]
    pub fn undo_action(self: &Arc<Self>, effect: EffectId) -> Option<super::UndoAction> {
        let id = effect.0;
        let consumed = lock(&self.index).get(&id)?.consumed;
        if consumed {
            let witness: Witness = Arc::new(|| false);
            let inverse: Box<dyn FnOnce() -> KernelFuture<'static, ()> + Send> =
                Box::new(move || {
                    Box::pin(async move {
                        Err(refusal(
                            ErrorCode::EffectFailed,
                            format!("effect {id}'s inverse was already consumed"),
                        ))
                    })
                });
            return Some((witness, inverse));
        }
        let attesting = Arc::clone(self);
        let witness: Witness = Arc::new(move || {
            attesting
                .store
                .load_sync(id)
                .is_some_and(|record| attesting.witness(&record))
        });
        let provider = Arc::clone(self);
        let inverse: Box<dyn FnOnce() -> KernelFuture<'static, ()> + Send> = Box::new(move || {
            Box::pin(async move {
                let record = provider.store.load(id).await?;
                provider.apply_inverse(record).await
            })
        });
        Some((witness, inverse))
    }

    /// Consumes one reverted effect's inverse: reclaimed, off the live list.
    ///
    /// # Errors
    ///
    /// An effect this provider does not own, or a storage refusal.
    pub async fn reclaim(&self, effect: EffectId) -> Result<(), KernelError> {
        let id = effect.0;
        if !lock(&self.index).contains_key(&id) {
            return Err(refusal(
                ErrorCode::EffectFailed,
                format!("no revertible effect {id}"),
            ));
        }
        self.store.reclaim(id).await?;
        if let Some(retained) = lock(&self.index).get_mut(&id) {
            retained.consumed = true;
        }
        Ok(())
    }

    /// The teardown withdrawal of one live effect (R5, LIFO through the
    /// owning seat's journal): inverse from the spill, then reclaim. An
    /// already-consumed effect withdraws clean; an unknown one is refused.
    ///
    /// # Errors
    ///
    /// An unknown effect, a failing inverse, or a storage refusal.
    pub async fn withdraw(&self, effect: EffectId) -> Result<(), KernelError> {
        let id = effect.0;
        match lock(&self.index).get(&id).map(|retained| retained.consumed) {
            None => {
                return Err(refusal(
                    ErrorCode::EffectFailed,
                    format!("no revertible effect {id}"),
                ));
            }
            Some(true) => return Ok(()),
            Some(false) => {}
        }
        let record = self.store.load(id).await?;
        self.apply_inverse(record).await?;
        self.reclaim(effect).await
    }
}
