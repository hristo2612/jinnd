//! The base `jinn:keystore` host provider (R7; M2-K8, harness #5
//! remainder): a native peer behind the SAME broker choke point every
//! guest crosses — grant check → operation class → ledger append →
//! dispatch — answering the contract bundle `contracts/jinn-keystore`.
//! Authority is the caller's `key-prefix` scope (a bare grant admits no
//! key), decided per call on the key name; every refusal is on the record.
//!
//! Sensitivity class SECRET (Law 2, constitution 02 §Redaction): the
//! provider's own ledger line is `KeystoreAccessed { operation, key,
//! digest }` — the key NAME and the value's SHA-256, never the value; no
//! label, refusal, or error message carries one either. `put` and
//! `delete` are the revertible class: the prior value (SEALED under the
//! store's own cipher) or absence is retained durably BEFORE the mutation
//! commits, the effect answers its id for the caller's seat to journal,
//! and dispose withdraws it LIFO through [`Peer::withdraw`] (R5, I1).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use jinnd_api::{
    EffectId, ErrorCode, FiberId, KernelError, KernelFuture, LedgerEventKind, RefusalReason,
};

use crate::broker::Broker;
use crate::broker_state::refusal;
use crate::grants::GrantScope;
use crate::handle::HostRecord;
use crate::hostfs::retention::{Header, Retention};
use crate::lane::lock;
use crate::peer::{LedgerSink, Peer, PeerId};

mod effects;
mod master;
#[cfg(all(test, not(feature = "loom")))]
mod tests;
mod vault;

pub use master::{MasterKeySource, PASSPHRASE_ENV, PASSPHRASE_FILE_ENV};
use vault::Vault;

/// The provider's contract name.
pub const KEYSTORE_CONTRACT: &str = "jinn:keystore";

/// One effect's revert action, as `jinn:fs` shapes it (Law 3).
pub type UndoAction = crate::hostfs::UndoAction;

/// The Law-2 label one keystore effect registers and withdraws under —
/// the key NAME, never its value.
#[must_use]
pub fn keystore_label(operation: &str, key: &str, effect: u64) -> String {
    format!("keystore {operation} {key} [effect {effect}]")
}

/// One retained inverse's in-memory index: its header only.
struct Retained {
    header: Header,
    consumed: bool,
}

/// The `jinn:keystore` provider over one sealed store.
pub struct HostKeystore {
    sink: Arc<dyn LedgerSink>,
    /// The map and cipher, under a brief std lock (never across an await).
    vault: Mutex<Vault>,
    /// Serializes mutate-then-commit sequences (no plugin code inside).
    serial: tokio::sync::Mutex<()>,
    store: Retention,
    index: Mutex<BTreeMap<u64, Retained>>,
    broker: OnceLock<Weak<Broker>>,
    next: AtomicU64,
}

impl HostKeystore {
    /// Opens the provider under `dir` (the sealed store and its inverse
    /// spill; the master key comes from `master`, never from `dir`),
    /// appending its Law-2 events to `sink`. Blocking — construction only.
    ///
    /// # Errors
    ///
    /// The store cannot be created, read, or authenticated (an existing
    /// document whose key `master` cannot supply is refused whole), or
    /// its retention store is unreadable (fail-closed, as `jinn:fs`).
    pub fn open(
        dir: PathBuf,
        master: MasterKeySource,
        sink: Arc<dyn LedgerSink>,
    ) -> Result<Self, KernelError> {
        let vault = Vault::open(&dir, master)?;
        let (store, epoch, spilled) = Retention::open(dir.join("inverses"))?;
        let base = (epoch + 1) << 32;
        let next = spilled
            .iter()
            .map(|(id, _)| id + 1)
            .max()
            .unwrap_or(base)
            .max(base);
        let index = spilled
            .into_iter()
            .map(|(id, header)| {
                (
                    id,
                    Retained {
                        header,
                        consumed: false,
                    },
                )
            })
            .collect();
        Ok(Self {
            sink,
            vault: Mutex::new(vault),
            serial: tokio::sync::Mutex::new(()),
            store,
            index: Mutex::new(index),
            broker: OnceLock::new(),
            next: AtomicU64::new(next),
        })
    }

    /// Registers this provider as a broker peer holding and providing the
    /// contract (providing is authority). The broker is kept weakly.
    ///
    /// # Errors
    ///
    /// The broker's refusal of the provision.
    pub fn register(self: &Arc<Self>, broker: &Arc<Broker>) -> Result<(), KernelError> {
        let _ = self.broker.set(Arc::downgrade(broker));
        let peer = broker.register_peer(None);
        broker.grant(peer, KEYSTORE_CONTRACT);
        broker.provide(
            peer,
            KEYSTORE_CONTRACT,
            Arc::new(KeystorePeer(Arc::clone(self))),
        )
    }

    /// Resolves the master key before the store's first commit — the
    /// keychain call or the passphrase derivation runs off the async
    /// threads (R1) — or refuses typed when no source exists. Idempotent;
    /// callers hold `serial`, so the key resolves once.
    async fn unlock(&self) -> Result<(), KernelError> {
        let (source, dir) = {
            let vault = lock(&self.vault);
            if !vault.locked() {
                return Ok(());
            }
            vault.pending()
        };
        let key = tokio::task::spawn_blocking(move || source.resolve(&dir))
            .await
            .map_err(|joined| {
                refusal(
                    ErrorCode::EffectFailed,
                    format!("keystore master key: {joined}"),
                )
            })??;
        lock(&self.vault).install(key);
        Ok(())
    }

    fn broker(&self) -> Option<Arc<Broker>> {
        self.broker.get().and_then(Weak::upgrade)
    }

    fn attribution(&self, caller: PeerId) -> Option<FiberId> {
        self.broker().and_then(|broker| broker.attribution(caller))
    }

    fn entry_of(&self, caller: PeerId) -> Option<String> {
        self.broker().and_then(|broker| broker.entry_of(caller))
    }

    /// The key prefixes `caller` holds the contract under (empty for a
    /// bare grant: nothing), `None` when ungranted.
    fn prefixes(&self, caller: PeerId) -> Option<Vec<String>> {
        match self
            .broker()
            .and_then(|broker| broker.policy(caller, KEYSTORE_CONTRACT))?
        {
            GrantScope::Keys(prefixes) => Some(prefixes),
            _ => Some(Vec::new()),
        }
    }

    /// Authorizes one key name for one caller: a well-formed name under a
    /// granted prefix. A malformed name is the typed `invalid`; a name
    /// beside every prefix is a ledgered scope refusal (Law 2).
    fn authorized(&self, caller: PeerId, key: &str) -> Result<(), KernelError> {
        if key.is_empty() || key.len() > vault::KEY_NAME_CAP || key.contains('\0') {
            return Err(refusal(
                ErrorCode::InvalidProfile,
                "keystore key name must be 1..=512 bytes without NUL".to_owned(),
            ));
        }
        let scope = self.prefixes(caller).map(GrantScope::Keys);
        if scope.is_some_and(|scope| scope.admits_key(key)) {
            return Ok(());
        }
        let message = format!("keystore key {key:?} is outside the caller's granted prefixes");
        self.sink.append(
            LedgerEventKind::GrantRefused {
                contract: KEYSTORE_CONTRACT.to_owned(),
                reason: RefusalReason::ScopeMismatch,
                detail: Some(message.clone()),
            },
            self.attribution(caller),
        );
        Err(refusal(ErrorCode::EffectFailed, message))
    }

    /// The provider's Law-2 line for one crossing: name and digest only.
    fn accessed(&self, caller: PeerId, operation: &str, key: &str, value: Option<&[u8]>) {
        self.sink.append(
            LedgerEventKind::KeystoreAccessed {
                operation: operation.to_owned(),
                key: key.to_owned(),
                digest: value.map(crate::sha256::hex_digest),
            },
            self.attribution(caller),
        );
    }

    /// Every entry's retained journal (M2-K4): the live effects each
    /// profile entry still owns, in registration order, for a successor
    /// incarnation to inherit and the entry's dispose to withdraw.
    #[must_use]
    pub fn journals(&self) -> Vec<(String, Vec<HostRecord>)> {
        let mut journals: BTreeMap<String, Vec<HostRecord>> = BTreeMap::new();
        for (id, retained) in lock(&self.index).iter() {
            if retained.consumed || retained.header.entry.is_empty() {
                continue;
            }
            journals
                .entry(retained.header.entry.clone())
                .or_default()
                .push(HostRecord {
                    contract: KEYSTORE_CONTRACT.to_owned(),
                    label: keystore_label(&retained.header.operation, &retained.header.label, *id),
                    effect: *id,
                });
        }
        journals.into_iter().collect()
    }

    /// Every live (unconsumed) revertible effect, in id order: (id, key).
    #[must_use]
    pub fn effects(&self) -> Vec<(EffectId, String)> {
        lock(&self.index)
            .iter()
            .filter(|(_, retained)| !retained.consumed)
            .map(|(id, retained)| (EffectId(*id), retained.header.label.clone()))
            .collect()
    }
}

/// The provider's broker face.
struct KeystorePeer(Arc<HostKeystore>);

impl Peer for KeystorePeer {
    fn call(
        &self,
        caller: PeerId,
        _contract: &str,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let provider = Arc::clone(&self.0);
        let operation = operation.to_owned();
        Box::pin(async move { effects::dispatch(&provider, caller, &operation, payload).await })
    }

    fn withdraw(&self, effect: u64) -> KernelFuture<'static, ()> {
        let provider = Arc::clone(&self.0);
        Box::pin(async move { provider.withdraw(EffectId(effect)).await })
    }
}
