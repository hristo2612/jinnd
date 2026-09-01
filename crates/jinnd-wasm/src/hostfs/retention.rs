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

/// One spilled inverse: its header and what to restore. The keystore
/// provider spills its (sealed) priors through the same store (M2-K8).
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
/// The staged-file suffix: never parses as an inverse id, and a guest's
/// own `<name>` and `<name>.jinnd-stage` never collide with each other.
const STAGE_SUFFIX: &str = ".jinnd-stage";

fn io(detail: &str, refused: &std::io::Error) -> KernelError {
    refusal(
        ErrorCode::EffectFailed,
        format!("inverse store {detail}: {refused}"),
    )
}

/// The staging name beside `target` (M2-K8, harness #22): a sibling in the
/// same directory, so the rename is one atomic replace.
fn staged_beside(target: &Path) -> PathBuf {
    let mut name = target
        .file_name()
        .map(std::ffi::OsStr::to_os_string)
        .unwrap_or_default();
    name.push(STAGE_SUFFIX);
    target.with_file_name(name)
}

/// Removes every staged file left under `dir` by a crash inside the commit
/// window (M2-K19; I4, Law 3). Answers how many went.
///
/// The rename IS the commit point, so a staged file whose rename never came
/// has no claim on any state: the target still holds its prior content, and
/// the effect either never registered or has a durable inverse restoring
/// exactly that prior. It is never ADOPTED — `create` opens the window and
/// `sync_all` closes it, so the bytes may be a torn prefix that was never
/// durable — only deleted, so a crash leaves no trace a clean shutdown does
/// not. `<name>.jinnd-stage` is the staging name of `<name>` by contract
/// (`contracts/jinn-fs` bundle, `commit = "stage-fsync-rename"`), which is
/// what makes deleting it the kernel's business and not a guest's loss.
///
/// Blocking, and an iterative walk (no recursion into a guest-shaped tree):
/// callers run it at open, never on an async path. Symlinks are not
/// followed — `read_dir`'s file type does not stat through them — so the
/// walk stays inside `dir`. A directory that cannot be read is skipped: a
/// sweep is best-effort cleanup and must never fail an open that would
/// otherwise succeed (R11).
pub(crate) fn sweep_staged(dir: &Path) -> usize {
    let mut swept = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in std::fs::read_dir(&next).into_iter().flatten().flatten() {
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            if kind.is_dir() {
                stack.push(entry.path());
            } else if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.ends_with(STAGE_SUFFIX))
                && std::fs::remove_file(entry.path()).is_ok()
            {
                swept += 1;
            }
        }
    }
    swept
}

/// Writes `bytes` durably at `target`: staged beside it, fsynced, renamed,
/// and the parent directory fsynced so the rename is on disk too.
fn commit_sync(dir: &Path, target: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let staged = staged_beside(target);
    let mut file = std::fs::File::create(&staged)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    std::fs::rename(&staged, target)?;
    std::fs::File::open(dir)?.sync_all()
}

/// The async twin of [`commit_sync`] (M2-K8, harness #22): the ONE commit
/// shape the retention store AND the data-plane `write`/`append` share —
/// a reader racing the commit sees the old document or the new one, never
/// a prefix. The parent directory must exist. A failed commit leaves no
/// staged file behind.
pub(crate) async fn commit_atomic(target: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let staged = staged_beside(target);
    let write = async {
        let mut file = tokio::fs::File::create(&staged).await?;
        file.write_all(bytes).await?;
        file.sync_all().await?;
        tokio::fs::rename(&staged, target).await?;
        match target.parent() {
            Some(dir) => tokio::fs::File::open(dir).await?.sync_all().await,
            None => Ok(()),
        }
    };
    let outcome = write.await;
    if outcome.is_err() {
        let _ = tokio::fs::remove_file(&staged).await;
    }
    outcome
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
        // A crash inside the spill's own commit window left staged files
        // here too (M2-K19): they are swept before anything is indexed.
        sweep_staged(&dir);
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
        commit_atomic(&self.path(id), &record.encode())
            .await
            .map_err(|refused| io("persist", &refused))
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
