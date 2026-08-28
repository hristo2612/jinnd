//! The base `jinn:fs` host provider (R7; M2-K1, lifted from the daemon per
//! the PLA-283 ruling; M2-K3 finalized to its declared bundle): a native
//! peer behind the SAME broker choke point every guest crosses — grant
//! check → ledger append → dispatch. Scope is one root directory, and per
//! caller the granted path-prefix subtrees, decided on the fully resolved
//! path (contract bundle `contracts/jinn-fs`; `scope.rs`).
//!
//! `read`, `list`, and `meta` are reads (call-ledger line only). `write`,
//! `append`, and `remove` are the revertible effect class (Law 3, R5): the
//! provider captures the inverse — prior content, prior absence, or prior
//! length — at the point of action and makes it DURABLE before the mutation
//! commits (retention store, M2-K3): if the inverse cannot be made durable
//! the effect is refused, on the record. The forward op is keyed
//! exactly-once (03 §Act). Each effect answers its id, which the caller's
//! seat journals: teardown withdraws it through [`Peer::withdraw`] like
//! every other registration (LIFO; R5, M1-P9b). The assembly's keyed
//! revert replays an inverse through the ledger's exactly-once protocol
//! with an executable witness (the [`HostFs::undo_action`] seam).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use jinnd_api::{
    EffectId, ErrorCode, FiberId, KernelError, KernelFuture, LedgerEventKind, Witness,
};

use crate::broker::Broker;
use crate::broker_state::refusal;
use crate::peer::{LedgerSink, Peer, PeerId};

mod inverses;
mod ops;
mod retention;
pub(crate) mod scope;
#[cfg(all(test, not(feature = "loom")))]
mod tests;
pub mod wire;

use retention::{Header, Retention};

/// The provider's contract name.
pub const FS_CONTRACT: &str = "jinn:fs";

/// One effect's revert action (Law 3): the executable witness and the
/// inverse the assembly feeds to the ledger's exactly-once revert protocol.
pub type UndoAction = (
    Witness,
    Box<dyn FnOnce() -> KernelFuture<'static, ()> + Send>,
);

/// The Law-2 label one fs effect registers and withdraws under — shared by
/// the provider's ledger line and the caller seat's journal entry.
#[must_use]
pub fn effect_label(operation: &str, path: &str, effect: u64) -> String {
    format!(
        "fs {operation} {} [effect {effect}]",
        path.trim_start_matches('/')
    )
}

/// The in-memory index of one retained inverse: its header only — prior
/// contents live in the retention store, never here (finding 8 bound).
struct Retained {
    header: Header,
    /// Consumed by a completed revert or withdrawal and reclaimed; kept so
    /// a replay of the same key still answers from the record, never
    /// "unknown effect".
    consumed: bool,
}

/// The `jinn:fs` provider over one containment root.
pub struct HostFs {
    /// Canonical: containment compares resolved paths against it.
    root: PathBuf,
    sink: Arc<dyn LedgerSink>,
    store: Retention,
    index: Mutex<BTreeMap<u64, Retained>>,
    broker: OnceLock<Weak<Broker>>,
    next: AtomicU64,
}

/// Runs a blocking path resolution on the blocking pool (R1: the async
/// lanes never block on a metadata walk).
async fn blocking<T: Send + 'static>(
    job: impl FnOnce() -> Result<T, KernelError> + Send + 'static,
) -> Result<T, KernelError> {
    tokio::task::spawn_blocking(job).await.unwrap_or_else(|_| {
        Err(refusal(
            ErrorCode::PluginFailed,
            "fs resolution task failed".to_owned(),
        ))
    })
}

impl HostFs {
    /// Opens the provider over `root` (created, then canonicalized),
    /// spilling inverses under `inverses` (a directory OUTSIDE the root:
    /// guests must never reach the inverses) and appending its Law-2 events
    /// to `sink`. Blocking — construction only.
    ///
    /// # Errors
    ///
    /// The root cannot be created or resolved, the retention store cannot
    /// be opened, or its spilled records are unreadable (fail-closed:
    /// revertibility is never silently weakened).
    pub fn open(
        root: PathBuf,
        inverses: PathBuf,
        sink: Arc<dyn LedgerSink>,
    ) -> Result<Self, KernelError> {
        let root = std::fs::create_dir_all(&root)
            .and_then(|()| std::fs::canonicalize(&root))
            .map_err(|refused| refusal(ErrorCode::InvalidProfile, format!("fs root: {refused}")))?;
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
    /// attribution and scopes (R4): it owns this peer, never the reverse.
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

    fn broker(&self) -> Option<Arc<Broker>> {
        self.broker.get().and_then(Weak::upgrade)
    }

    /// The fiber attribution of one calling peer, through the broker.
    fn attribution(&self, caller: PeerId) -> Option<FiberId> {
        self.broker().and_then(|broker| broker.attribution(caller))
    }

    /// Authorizes and resolves one caller path: post-symlink containment
    /// under the root AND under the caller's granted path-prefix scopes
    /// (`scope.rs`). A scope refusal is a ledgered grant refusal with the
    /// caller's attribution (Law 2), exactly like the broker's own.
    async fn scoped(&self, caller: PeerId, path: &str) -> Result<PathBuf, KernelError> {
        let Some(scopes) = self
            .broker()
            .and_then(|broker| broker.scopes(caller, FS_CONTRACT))
        else {
            return Err(refusal(
                ErrorCode::EffectFailed,
                "fs caller holds no grant".to_owned(),
            ));
        };
        let (root, path) = (self.root.clone(), path.to_owned());
        let outcome = blocking(move || scope::authorized(&root, &scopes, &path)).await;
        if let Err(error) = &outcome
            && error.code == ErrorCode::EffectFailed
        {
            self.sink.append(
                LedgerEventKind::GrantRefused {
                    contract: FS_CONTRACT.to_owned(),
                },
                self.attribution(caller),
            );
        }
        outcome
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

    fn withdraw(&self, effect: u64) -> KernelFuture<'static, ()> {
        let provider = Arc::clone(&self.0);
        Box::pin(async move { provider.withdraw(EffectId(effect)).await })
    }
}
