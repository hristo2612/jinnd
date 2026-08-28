//! The `jinn:fs` effect-retention store (M2-K3, harness finding 8): every
//! revertible fs effect's inverse is spilled to a durable file keyed by
//! effect id BEFORE the mutation commits, so provider memory holds an index
//! (id → header), never prior contents. Undo reads the inverse back from
//! the spill; reclaim removes it. No compaction daemon: retention is
//! event-driven — persist on register, reclaim on consumption (R10).
//!
//! Durable means durable: the staged file is fsynced, renamed into place,
//! and the PARENT DIRECTORY is fsynced — without the directory sync the
//! rename itself is not on disk (round-2 blocker 4).
//!
//! Ids never repeat across restarts: the store carries a boot epoch, and an
//! effect id is `(epoch + 1) << 32 | ordinal` — a reopened ledger's revert
//! receipts can never be mistaken for a fresh effect's (constitution 03
//! keyed exactly-once).

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use jinnd_api::{ErrorCode, KernelError};
use tokio::io::AsyncWriteExt;

use crate::broker_state::refusal;

mod wire;

use wire::corrupt;

/// What the inverse restores: prior absence, prior content, or prior length
/// (the append inverse: truncate-to-prior-length).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Prior {
    Absent,
    Content(Vec<u8>),
    Length(u64),
}

/// The part of a record the in-memory index keeps: the scoped path label,
/// the caller's idempotency key (03 §Act; empty = none), and the owning
/// fiber (0 = unattributed).
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Header {
    pub(crate) label: String,
    pub(crate) key: String,
    pub(crate) owner: u64,
    /// The profile entry the effect belongs to (M2-K4: the journal is
    /// entry-scoped and spans incarnations; empty = unattributed) and the
    /// operation, so the Law-2 label reconstructs across a restart.
    pub(crate) entry: String,
    pub(crate) operation: String,
}

/// One spilled inverse: its header and what to restore.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Record {
    pub(crate) header: Header,
    pub(crate) prior: Prior,
}

/// What opening a store yields: the store, its boot epoch, and the index
/// of every inverse still spilled as `(id, header)`.
pub(crate) type Opened = (Retention, u64, Vec<(u64, Header)>);

/// The spill directory and its boot epoch.
#[derive(Clone)]
pub(crate) struct Retention {
    dir: PathBuf,
}

const EPOCH_FILE: &str = "epoch";
const INVERSE_SUFFIX: &str = ".inverse";

fn io(detail: &str, refused: &std::io::Error) -> KernelError {
    refusal(
        ErrorCode::EffectFailed,
        format!("inverse store {detail}: {refused}"),
    )
}

/// Writes `bytes` durably at `target`: staged beside it, fsynced, renamed,
/// and the parent directory fsynced so the rename is on disk too.
fn commit_sync(dir: &Path, target: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let staged = target.with_extension("tmp");
    let mut file = std::fs::File::create(&staged)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&staged, target)?;
    std::fs::File::open(dir)?.sync_all()
}

impl Retention {
    /// Opens (creating) the store, bumps the boot epoch durably, and
    /// rehydrates the index of every inverse still spilled: `(id, header)`.
    /// Blocking — construction only, never an async path.
    ///
    /// # Errors
    ///
    /// The store cannot be created, the epoch cannot be made durable, or a
    /// spilled record is unreadable (fail-closed: an unreadable inverse is
    /// a lost revertibility guarantee, reported, never skipped).
    pub(crate) fn open(dir: PathBuf) -> Result<Opened, KernelError> {
        std::fs::create_dir_all(&dir).map_err(|refused| io("create", &refused))?;
        let epoch_path = dir.join(EPOCH_FILE);
        let epoch = std::fs::read_to_string(&epoch_path)
            .ok()
            .and_then(|text| text.trim().parse::<u64>().ok())
            .map_or(0, |prior| prior + 1);
        commit_sync(&dir, &epoch_path, epoch.to_string().as_bytes())
            .map_err(|refused| io("epoch", &refused))?;
        let mut index = Vec::new();
        for entry in std::fs::read_dir(&dir).map_err(|refused| io("scan", &refused))? {
            let entry = entry.map_err(|refused| io("scan", &refused))?;
            let name = entry.file_name();
            let Some(id) = name
                .to_str()
                .and_then(|name| name.strip_suffix(INVERSE_SUFFIX))
                .and_then(|id| id.parse::<u64>().ok())
            else {
                continue;
            };
            index.push((id, read_header(id, &entry.path())?));
        }
        index.sort_unstable_by_key(|(id, _)| *id);
        Ok((Self { dir }, epoch, index))
    }

    fn path(&self, id: u64) -> PathBuf {
        self.dir.join(format!("{id}{INVERSE_SUFFIX}"))
    }

    /// Makes `record` durable under `id` BEFORE the caller mutates: staged,
    /// fsynced, renamed into place, parent directory fsynced.
    ///
    /// # Errors
    ///
    /// Any storage refusal — the caller must then REFUSE its effect.
    pub(crate) async fn persist(&self, id: u64, record: &Record) -> Result<(), KernelError> {
        let target = self.path(id);
        let staged = target.with_extension("tmp");
        let write = async {
            let mut file = tokio::fs::File::create(&staged).await?;
            file.write_all(&record.encode()).await?;
            file.sync_all().await?;
            tokio::fs::rename(&staged, &target).await?;
            tokio::fs::File::open(&self.dir).await?.sync_all().await
        };
        write.await.map_err(|refused| io("persist", &refused))
    }

    /// Reads the inverse back (the undo path).
    pub(crate) async fn load(&self, id: u64) -> Result<Record, KernelError> {
        let bytes = tokio::fs::read(self.path(id))
            .await
            .map_err(|refused| io("load", &refused))?;
        Record::decode(id, &bytes)
    }

    /// The witness's synchronous read of the inverse.
    pub(crate) fn load_sync(&self, id: u64) -> Option<Record> {
        Record::decode(id, &std::fs::read(self.path(id)).ok()?).ok()
    }

    /// Reclaims the spilled inverse's storage; an already-reclaimed record
    /// is not an error.
    pub(crate) async fn reclaim(&self, id: u64) -> Result<(), KernelError> {
        match tokio::fs::remove_file(self.path(id)).await {
            Ok(()) => Ok(()),
            Err(refused) if refused.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(refused) => Err(io("reclaim", &refused)),
        }
    }

    /// How many inverses are spilled right now (operator/test observation).
    pub(crate) fn spilled(&self) -> usize {
        std::fs::read_dir(&self.dir)
            .map(|entries| {
                entries
                    .flatten()
                    .filter(|entry| {
                        entry
                            .file_name()
                            .to_str()
                            .is_some_and(|name| name.ends_with(INVERSE_SUFFIX))
                    })
                    .count()
            })
            .unwrap_or(0)
    }
}

/// Reads only a record's header — rehydration never loads prior contents.
fn read_header(id: u64, path: &Path) -> Result<Header, KernelError> {
    let mut file = std::fs::File::open(path).map_err(|refused| io("scan", &refused))?;
    // Tag + two prefixed strings + owner: read generously, decode the
    // header alone (the body may be megabytes; it stays on disk).
    let mut head = vec![0u8; 4096];
    let read = file.read(&mut head).map_err(|_| corrupt(id, "header"))?;
    let (_, header, _) = Header::decode(id, head.get(..read).unwrap_or(&[]))?;
    Ok(header)
}
