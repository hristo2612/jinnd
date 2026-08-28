//! The base `jinn:fs` host provider (R7; M2-K1, lifted from the daemon per
//! the PLA-283 ruling; M2-K3 finalized to its declared bundle): a native
//! peer behind the SAME broker choke point every guest crosses — grant
//! check → ledger append → dispatch. Scope is one root directory
//! (path-prefix containment, contract bundle `contracts/jinn-fs`).
//!
//! `read`, `list`, and `meta` are reads (call-ledger line only). `write`,
//! `append`, and `remove` are the revertible effect class (Law 3, R5): the
//! provider captures the inverse — prior content, prior absence, or prior
//! length — at the point of action and makes it DURABLE before the mutation
//! commits (retention store, M2-K3): if the inverse cannot be made durable
//! the effect is refused, on the record. The assembly's keyed revert replays
//! an inverse through the ledger's exactly-once protocol with an executable
//! witness (the [`HostFs::undo_action`] seam) and reclaims it after.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use jinnd_api::{EffectId, ErrorCode, FiberId, KernelError, KernelFuture, Witness};

use crate::broker::Broker;
use crate::broker_state::refusal;
use crate::lane::lock;
use crate::peer::{LedgerSink, Peer, PeerId};

mod ops;
mod retention;
#[cfg(all(test, not(feature = "loom")))]
mod tests;
pub mod wire;

use retention::{Prior, Record, Retention};

/// The provider's contract name.
pub const FS_CONTRACT: &str = "jinn:fs";

/// One effect's revert action (Law 3): the executable witness and the
/// inverse the assembly feeds to the ledger's exactly-once revert protocol.
pub type UndoAction = (
    Witness,
    Box<dyn FnOnce() -> KernelFuture<'static, ()> + Send>,
);

/// The in-memory index of one retained inverse: its label only — prior
/// contents live in the retention store, never here (finding 8 bound).
struct Retained {
    label: String,
    /// Consumed by a completed revert and reclaimed; kept so a replay of
    /// the same key still answers from the record, never "unknown effect".
    consumed: bool,
}

/// The `jinn:fs` provider over one containment root.
pub struct HostFs {
    root: PathBuf,
    sink: Arc<dyn LedgerSink>,
    store: Retention,
    index: Mutex<BTreeMap<u64, Retained>>,
    broker: OnceLock<Weak<Broker>>,
    next: AtomicU64,
}

/// Resolves one guest path inside the provider's root: rooted at `root`,
/// no parent traversal, no absolute escape (contract bundle: path-prefix
/// containment).
fn contained(root: &Path, path: &str) -> Result<PathBuf, KernelError> {
    let relative = path.trim_start_matches('/');
    let candidate = Path::new(relative);
    if candidate
        .components()
        .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err(refusal(
            ErrorCode::PluginFailed,
            format!("fs path escapes its scope: {path:?}"),
        ));
    }
    Ok(root.join(candidate))
}

impl HostFs {
    /// Opens the provider over `root`, spilling inverses under `inverses`
    /// (a directory OUTSIDE the root: guests must never reach the inverses)
    /// and appending its Law-2 events to `sink`. Blocking — construction
    /// only.
    ///
    /// # Errors
    ///
    /// The retention store cannot be opened or its spilled records are
    /// unreadable (fail-closed: revertibility is never silently weakened).
    pub fn open(
        root: PathBuf,
        inverses: PathBuf,
        sink: Arc<dyn LedgerSink>,
    ) -> Result<Self, KernelError> {
        let (store, epoch, spilled) = Retention::open(inverses)?;
        let base = (epoch + 1) << 32;
        let next = spilled
            .iter()
            .map(|(id, _)| id + 1)
            .max()
            .unwrap_or(base)
            .max(base);
        let index = spilled
            .into_iter()
            .map(|(id, label)| {
                (
                    id,
                    Retained {
                        label,
                        consumed: false,
                    },
                )
            })
            .collect();
        Ok(Self {
            root,
            sink,
            store,
            index: Mutex::new(index),
            broker: OnceLock::new(),
            next: AtomicU64::new(next),
        })
    }

    /// Registers this provider as a broker peer holding and providing the
    /// `jinn:fs` contract (providing is authority: the provider peer is
    /// granted what it provides). The broker is kept weakly for caller
    /// attribution (R4): it owns this peer, never the reverse.
    ///
    /// # Errors
    ///
    /// The broker's refusal of the provision.
    pub fn register(self: &Arc<Self>, broker: &Arc<Broker>) -> Result<(), KernelError> {
        let _ = self.broker.set(Arc::downgrade(broker));
        let peer = broker.register_peer(None);
        broker.grant(peer, FS_CONTRACT);
        broker.provide(peer, FS_CONTRACT, Arc::new(FsPeer(Arc::clone(self))))
    }

    /// The fiber attribution of one calling peer, through the broker.
    fn attribution(&self, caller: PeerId) -> Option<FiberId> {
        self.broker
            .get()
            .and_then(Weak::upgrade)
            .and_then(|broker| broker.attribution(caller))
    }

    /// Every live (unconsumed) revertible effect, in id order: (id, scoped
    /// path).
    #[must_use]
    pub fn effects(&self) -> Vec<(EffectId, String)> {
        lock(&self.index)
            .iter()
            .filter(|(_, retained)| !retained.consumed)
            .map(|(id, retained)| (EffectId(*id), retained.label.clone()))
            .collect()
    }

    /// The in-memory index's footprint in bytes — labels and bookkeeping
    /// only, whatever the prior contents weighed (finding 8 bound).
    #[must_use]
    pub fn index_bytes(&self) -> usize {
        lock(&self.index)
            .values()
            .map(|retained| retained.label.len() + std::mem::size_of::<Retained>() + 8)
            .sum()
    }

    /// How many inverses are spilled in the retention store right now.
    #[must_use]
    pub fn spilled(&self) -> usize {
        self.store.spilled()
    }

    /// The keyed-revert action for one effect this provider owns (Law 3):
    /// the witness reads the file back against the spilled prior; the
    /// inverse restores prior content, absence, or length from the spill.
    /// A consumed effect still answers — with an inverse that refuses to
    /// run again (the ledger answers its replay from the record).
    #[must_use]
    pub fn undo_action(&self, effect: EffectId) -> Option<UndoAction> {
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
        let (witness_root, witness_store) = (self.root.clone(), self.store.clone());
        let witness: Witness = Arc::new(move || {
            witness_store
                .load_sync(id)
                .is_some_and(|record| ops::witness(&witness_root, &record))
        });
        let (root, store) = (self.root.clone(), self.store.clone());
        let inverse: Box<dyn FnOnce() -> KernelFuture<'static, ()> + Send> = Box::new(move || {
            Box::pin(async move {
                let record = store.load(id).await?;
                ops::apply_inverse(&root, &record).await
            })
        });
        Some((witness, inverse))
    }

    /// Consumes one reverted effect's inverse: its spilled storage is
    /// reclaimed and it leaves the live effect list. The assembly calls
    /// this after the ledger records the branch `Reverted`.
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

    /// Registers one revertible effect: the inverse is made durable FIRST;
    /// only then may the caller mutate. Returns the effect id to label.
    async fn retain(&self, label: &str, prior: Prior) -> Result<u64, KernelError> {
        let id = self.next.fetch_add(1, Ordering::SeqCst);
        let record = Record {
            label: label.to_owned(),
            prior,
        };
        self.store.persist(id, &record).await?;
        lock(&self.index).insert(
            id,
            Retained {
                label: label.to_owned(),
                consumed: false,
            },
        );
        Ok(id)
    }

    /// Drops a retained inverse whose mutation never happened (the io
    /// refused after the spill): nothing to revert, nothing to keep.
    async fn release(&self, id: u64) {
        lock(&self.index).remove(&id);
        let _ = self.store.reclaim(id).await;
    }
}

/// The provider's broker face (a local wrapper: the `Peer` trait and `Arc`
/// are both foreign).
struct FsPeer(Arc<HostFs>);

impl Peer for FsPeer {
    fn call(
        &self,
        caller: PeerId,
        _contract: &str,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let provider = Arc::clone(&self.0);
        let operation = operation.to_owned();
        Box::pin(async move { ops::dispatch(&provider, caller, &operation, payload).await })
    }
}
