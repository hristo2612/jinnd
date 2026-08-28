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

const TAG_ABSENT: u8 = 0;
const TAG_CONTENT: u8 = 1;
const TAG_LENGTH: u8 = 2;
/// Tag flag (M2-K4, additive): the header carries entry and operation after
/// the owner. Records without it (M2-K3) still decode, unattributed.
const TAG_ENTRY: u8 = 0x80;

fn corrupt(id: u64, detail: &str) -> KernelError {
    refusal(
        ErrorCode::EffectFailed,
        format!("inverse record {id} unreadable: {detail}"),
    )
}

fn push_prefixed(bytes: &mut Vec<u8>, segment: &[u8]) {
    bytes.extend(
        u32::try_from(segment.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    bytes.extend(segment);
}

fn take_prefixed(bytes: &[u8]) -> Option<(String, &[u8])> {
    let (length, rest) = bytes.split_at_checked(4)?;
    let mut prefix = [0u8; 4];
    prefix.copy_from_slice(length);
    let (segment, rest) = rest.split_at_checked(u32::from_le_bytes(prefix) as usize)?;
    Some((String::from_utf8(segment.to_vec()).ok()?, rest))
}

impl Header {
    /// Wire: one tag byte, then u32-LE-prefixed label and key, then the
    /// u64-LE owner. The tag names the body that follows.
    fn encode(&self, tag: u8) -> Vec<u8> {
        let mut bytes = vec![tag | TAG_ENTRY];
        push_prefixed(&mut bytes, self.label.as_bytes());
        push_prefixed(&mut bytes, self.key.as_bytes());
        bytes.extend(self.owner.to_le_bytes());
        push_prefixed(&mut bytes, self.entry.as_bytes());
        push_prefixed(&mut bytes, self.operation.as_bytes());
        bytes
    }

    fn decode(id: u64, bytes: &[u8]) -> Result<(u8, Self, &[u8]), KernelError> {
        let (&tag, rest) = bytes.split_first().ok_or_else(|| corrupt(id, "empty"))?;
        let (label, rest) = take_prefixed(rest).ok_or_else(|| corrupt(id, "label"))?;
        let (key, rest) = take_prefixed(rest).ok_or_else(|| corrupt(id, "key"))?;
        let (owner, mut body) = rest
            .split_at_checked(8)
            .ok_or_else(|| corrupt(id, "owner"))?;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(owner);
        let (mut entry, mut operation) = (String::new(), String::new());
        if tag & TAG_ENTRY != 0 {
            let (read_entry, rest) = take_prefixed(body).ok_or_else(|| corrupt(id, "entry"))?;
            let (read_op, rest) = take_prefixed(rest).ok_or_else(|| corrupt(id, "operation"))?;
            (entry, operation, body) = (read_entry, read_op, rest);
        }
        let header = Self {
            label,
            key,
            owner: u64::from_le_bytes(bytes),
            entry,
            operation,
        };
        Ok((tag & !TAG_ENTRY, header, body))
    }
}

impl Record {
    /// Wire: the header, then the body (nothing / the content / an 8-byte
    /// LE length).
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut bytes = self.header.encode(match self.prior {
            Prior::Absent => TAG_ABSENT,
            Prior::Content(_) => TAG_CONTENT,
            Prior::Length(_) => TAG_LENGTH,
        });
        match &self.prior {
            Prior::Absent => {}
            Prior::Content(content) => bytes.extend(content),
            Prior::Length(length) => bytes.extend(length.to_le_bytes()),
        }
        bytes
    }

    pub(crate) fn decode(id: u64, bytes: &[u8]) -> Result<Self, KernelError> {
        let (tag, header, body) = Header::decode(id, bytes)?;
        let prior = match tag {
            TAG_ABSENT => Prior::Absent,
            TAG_CONTENT => Prior::Content(body.to_vec()),
            TAG_LENGTH => {
                let mut length = [0u8; 8];
                let taken = body.get(..8).ok_or_else(|| corrupt(id, "length"))?;
                length.copy_from_slice(taken);
                Prior::Length(u64::from_le_bytes(length))
            }
            _ => return Err(corrupt(id, "tag")),
        };
        Ok(Self { header, prior })
    }
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
