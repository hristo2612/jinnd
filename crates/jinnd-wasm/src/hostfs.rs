//! The base `jinn:fs` host provider (R7; M2-K1, lifted from the daemon per
//! the PLA-283 ruling): a native peer behind the SAME broker choke point
//! every guest crosses — grant check → ledger append → dispatch. Scope is
//! one root directory (path-prefix containment, contract bundle
//! `contracts/jinn-fs`). A write is the revertible effect class (Law 3):
//! the provider captures the inverse — the prior content, or prior
//! absence — at the point of action, and the assembly's keyed revert
//! replays it through the ledger's exactly-once protocol with an
//! executable witness (the [`HostFs::undo_action`] seam).

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use jinnd_api::{EffectId, ErrorCode, KernelError, KernelFuture, LedgerEventKind, Witness};

use crate::broker::Broker;
use crate::broker_state::refusal;
use crate::lane::lock;
use crate::peer::{LedgerSink, Peer, PeerId};

/// The provider's contract name.
pub const FS_CONTRACT: &str = "jinn:fs";

/// One write effect's revert action (Law 3): the executable witness and the
/// inverse the assembly feeds to the ledger's exactly-once revert protocol.
pub type UndoAction = (
    Witness,
    Box<dyn FnOnce() -> KernelFuture<'static, ()> + Send>,
);

/// Provider-charged effect ids live far above the fiber scopes' range so a
/// ledger reader never conflates the two id spaces (v0.1 demo convention).
const FS_EFFECT_BASE: u64 = 1 << 32;

/// One write's inverse: the prior content, or `None` for prior absence.
#[derive(Clone)]
struct UndoRecord {
    path: PathBuf,
    prior: Option<Vec<u8>>,
}

#[derive(Default)]
struct FsState {
    undos: HashMap<u64, UndoRecord>,
    /// Registration order, for operator listing.
    order: Vec<(u64, String)>,
}

/// The `jinn:fs` provider over one containment root.
pub struct HostFs {
    root: PathBuf,
    sink: Arc<dyn LedgerSink>,
    state: Mutex<FsState>,
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

/// One scoped io refusal, worded exactly as the operation reports it.
fn io_refusal(operation: &str, path: &str, refused: &std::io::Error) -> KernelError {
    refusal(
        ErrorCode::PluginFailed,
        format!("fs {operation} {path:?}: {refused}"),
    )
}

/// Decodes the `write` wire: u32-LE path length, path bytes, then the data
/// (wit/plugin.wit `interface fs`).
fn split_write(payload: &[u8]) -> Result<(String, Vec<u8>), KernelError> {
    let malformed = || refusal(ErrorCode::PluginFailed, "malformed fs write payload".into());
    if payload.len() < 4 {
        return Err(malformed());
    }
    let mut length = [0u8; 4];
    length.copy_from_slice(&payload[..4]);
    let length = u32::from_le_bytes(length) as usize;
    let rest = &payload[4..];
    if rest.len() < length {
        return Err(malformed());
    }
    let path = String::from_utf8(rest[..length].to_vec()).map_err(|_| malformed())?;
    Ok((path, rest[length..].to_vec()))
}

impl HostFs {
    /// The provider over `root`, appending its Law-2 events to `sink`.
    #[must_use]
    pub fn new(root: PathBuf, sink: Arc<dyn LedgerSink>) -> Self {
        Self {
            root,
            sink,
            state: Mutex::new(FsState::default()),
            next: AtomicU64::new(0),
        }
    }

    /// Registers this provider as a broker peer holding and providing the
    /// `jinn:fs` contract (providing is authority: the provider peer is
    /// granted what it provides).
    ///
    /// # Errors
    ///
    /// The broker's refusal of the provision.
    pub fn register(self: &Arc<Self>, broker: &Broker) -> Result<(), KernelError> {
        let peer = broker.register_peer(None);
        broker.grant(peer, FS_CONTRACT);
        broker.provide(peer, FS_CONTRACT, Arc::new(FsPeer(Arc::clone(self))))
    }

    /// Every recorded write effect, in registration order: (id, scoped path).
    #[must_use]
    pub fn effects(&self) -> Vec<(EffectId, String)> {
        lock(&self.state)
            .order
            .iter()
            .map(|(id, path)| (EffectId(*id), path.clone()))
            .collect()
    }

    /// The keyed-revert action for one write effect this provider owns
    /// (Law 3): the witness reads the file back against the recorded prior;
    /// the inverse restores the prior content or absence. The assembly
    /// feeds both to the ledger's exactly-once revert protocol.
    #[must_use]
    pub fn undo_action(&self, effect: EffectId) -> Option<UndoAction> {
        let UndoRecord { path, prior } = lock(&self.state).undos.get(&effect.0).cloned()?;
        let (witness_path, witness_prior) = (path.clone(), prior.clone());
        let witness: Witness = Arc::new(move || match &witness_prior {
            Some(bytes) => std::fs::read(&witness_path)
                .map(|current| current == *bytes)
                .unwrap_or(false),
            None => !witness_path.exists(),
        });
        let failed =
            |refused: std::io::Error| refusal(ErrorCode::EffectFailed, refused.to_string());
        let inverse: Box<dyn FnOnce() -> KernelFuture<'static, ()> + Send> = Box::new(move || {
            Box::pin(async move {
                match prior {
                    Some(bytes) => tokio::fs::write(&path, bytes).await.map_err(failed),
                    None => match tokio::fs::remove_file(&path).await {
                        Ok(()) => Ok(()),
                        Err(refused) if refused.kind() == std::io::ErrorKind::NotFound => Ok(()),
                        Err(refused) => Err(failed(refused)),
                    },
                }
            })
        });
        Some((witness, inverse))
    }

    async fn read(&self, path: &str) -> Result<Vec<u8>, KernelError> {
        let file = contained(&self.root, path)?;
        tokio::fs::read(&file)
            .await
            .map_err(|refused| io_refusal("read", path, &refused))
    }

    async fn write(&self, path: &str, data: Vec<u8>) -> Result<(), KernelError> {
        let file = contained(&self.root, path)?;
        let prior = tokio::fs::read(&file).await.ok();
        if let Some(parent) = file.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|refused| io_refusal("write", path, &refused))?;
        }
        tokio::fs::write(&file, &data)
            .await
            .map_err(|refused| io_refusal("write", path, &refused))?;
        // The inverse is registered at the point of action (Law 3, R5), and
        // the registration is a ledger event (Law 2).
        let id = FS_EFFECT_BASE + self.next.fetch_add(1, Ordering::SeqCst);
        let label = path.trim_start_matches('/').to_owned();
        {
            let mut state = lock(&self.state);
            state.undos.insert(id, UndoRecord { path: file, prior });
            state.order.push((id, label.clone()));
        }
        self.sink.append(
            LedgerEventKind::EffectRegistered {
                label: format!("fs write {label} [effect {id}]"),
            },
            None,
        );
        tracing::info!(effect = id, path = %label, "fs write effect registered");
        Ok(())
    }
}

/// The provider's broker face (a local wrapper: the `Peer` trait and `Arc`
/// are both foreign).
struct FsPeer(Arc<HostFs>);

impl Peer for FsPeer {
    fn call(
        &self,
        _caller: PeerId,
        _contract: &str,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let provider = Arc::clone(&self.0);
        let operation = operation.to_owned();
        Box::pin(async move {
            match operation.as_str() {
                "read" => {
                    let path = String::from_utf8(payload).map_err(|_| {
                        refusal(ErrorCode::PluginFailed, "malformed fs read payload".into())
                    })?;
                    provider.read(&path).await
                }
                "write" => {
                    let (path, data) = split_write(&payload)?;
                    provider.write(&path, data).await?;
                    Ok(Vec::new())
                }
                other => Err(refusal(
                    ErrorCode::PluginFailed,
                    format!("unknown fs operation {other:?}"),
                )),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{contained, split_write};
    use std::path::Path;

    #[test]
    fn contained_scopes_paths_under_the_root() {
        let root = Path::new("/data");
        assert_eq!(
            contained(root, "journal.txt").unwrap_or_else(|error| panic!("scoped: {error:?}")),
            Path::new("/data/journal.txt")
        );
        assert_eq!(
            contained(root, "/nested/file").unwrap_or_else(|error| panic!("scoped: {error:?}")),
            Path::new("/data/nested/file")
        );
    }

    #[test]
    fn contained_refuses_parent_traversal() {
        let root = Path::new("/data");
        assert!(contained(root, "../escape").is_err());
        assert!(contained(root, "a/../../escape").is_err());
    }

    #[test]
    fn split_write_decodes_the_wire() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&5u32.to_le_bytes());
        payload.extend_from_slice(b"a.txt");
        payload.extend_from_slice(b"body");
        let (path, data) =
            split_write(&payload).unwrap_or_else(|error| panic!("well-formed: {error:?}"));
        assert_eq!(path, "a.txt");
        assert_eq!(data, b"body".to_vec());
    }

    #[test]
    fn split_write_refuses_truncation() {
        assert!(split_write(&[1, 0]).is_err());
        let mut payload = Vec::new();
        payload.extend_from_slice(&9u32.to_le_bytes());
        payload.extend_from_slice(b"short");
        assert!(split_write(&payload).is_err());
    }
}
