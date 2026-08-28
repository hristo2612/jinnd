//! The `jinn:fs` operations behind the provider's broker face (M2-K3): the
//! three reads, the three revertible effects, and the inverse/witness the
//! retention records replay. Every effect follows one order — authorize
//! and resolve the path, answer a keyed replay from the record, capture
//! the prior, make it durable, mutate, ledger — and refuses on the record
//! when the inverse cannot be made durable (Law 3; fail-closed like K2's
//! grant admission).

use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use jinnd_api::{ErrorCode, KernelError, LedgerEventKind};
use tokio::io::AsyncWriteExt;

use super::retention::{Header, Prior, Record};
use super::wire::{FileMeta, encode_metas, path_payload, split_keyed};
use super::{HostFs, blocking, effect_label, scope};
use crate::broker_state::refusal;
use crate::peer::PeerId;

/// One scoped io refusal, worded exactly as the operation reports it. A
/// missing path is the TYPED not-found (finding 3): callers classify it by
/// code, never by folding a message.
fn io_refusal(operation: &str, path: &str, refused: &std::io::Error) -> KernelError {
    let code = if refused.kind() == std::io::ErrorKind::NotFound {
        ErrorCode::NotFound
    } else {
        ErrorCode::PluginFailed
    };
    refusal(code, format!("fs {operation} {path:?}: {refused}"))
}

fn meta_of(path: String, metadata: &std::fs::Metadata) -> FileMeta {
    let modified_ms = metadata
        .modified()
        .ok()
        .and_then(|instant| instant.duration_since(UNIX_EPOCH).ok())
        .and_then(|since| u64::try_from(since.as_millis()).ok())
        .unwrap_or(0);
    FileMeta {
        path,
        size: metadata.len(),
        modified_ms,
        is_dir: metadata.is_dir(),
    }
}

/// Routes one broker call to its operation (wit/plugin.wit `interface fs`).
pub(super) async fn dispatch(
    provider: &HostFs,
    caller: PeerId,
    operation: &str,
    payload: Vec<u8>,
) -> Result<Vec<u8>, KernelError> {
    match operation {
        "read" | "list" | "meta" => {
            let path = path_payload(payload, operation)?;
            let file = provider.scoped(caller, &path).await?;
            match operation {
                "read" => read(&file, &path).await,
                "list" => list(&file, &path).await,
                _ => meta(&file, &path).await,
            }
        }
        "write" | "append" | "remove" => {
            let (path, key, data) = split_keyed(&payload)?;
            let file = provider.scoped(caller, &path).await?;
            let owner = provider.attribution(caller);
            // Keyed exactly-once (03 §Act): the recorded outcome answers a
            // replay; the mutation never applies twice.
            if let Some(recorded) = provider.recorded(owner, &key) {
                return Ok(recorded.0.to_le_bytes().to_vec());
            }
            let effect = Effect {
                provider,
                caller,
                operation,
                header: Header {
                    label: path.trim_start_matches('/').to_owned(),
                    key,
                    owner: owner.map_or(0, |fiber| fiber.0),
                    entry: provider.entry_of(caller).unwrap_or_default(),
                    operation: operation.to_owned(),
                },
                path: &path,
            };
            match operation {
                "write" => effect.write(&file, data).await,
                "append" => effect.append(&file, data).await,
                _ => effect.remove(&file).await,
            }
        }
        other => Err(refusal(
            ErrorCode::PluginFailed,
            format!("unknown fs operation {other:?}"),
        )),
    }
}

async fn read(file: &Path, path: &str) -> Result<Vec<u8>, KernelError> {
    tokio::fs::read(file)
        .await
        .map_err(|refused| io_refusal("read", path, &refused))
}

async fn list(dir: &Path, path: &str) -> Result<Vec<u8>, KernelError> {
    let mut entries = tokio::fs::read_dir(dir)
        .await
        .map_err(|refused| io_refusal("list", path, &refused))?;
    let mut metas = Vec::new();
    while let Some(entry) = entries
        .next_entry()
        .await
        .map_err(|refused| io_refusal("list", path, &refused))?
    {
        let metadata = entry
            .metadata()
            .await
            .map_err(|refused| io_refusal("list", path, &refused))?;
        metas.push(meta_of(
            entry.file_name().to_string_lossy().into_owned(),
            &metadata,
        ));
    }
    metas.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(encode_metas(&metas))
}

async fn meta(file: &Path, path: &str) -> Result<Vec<u8>, KernelError> {
    let metadata = tokio::fs::metadata(file)
        .await
        .map_err(|refused| io_refusal("meta", path, &refused))?;
    Ok(encode_metas(&[meta_of(
        path.trim_start_matches('/').to_owned(),
        &metadata,
    )]))
}

/// One revertible effect in flight: who is acting, on what, under which
/// header (label, key, owner).
struct Effect<'a> {
    provider: &'a HostFs,
    caller: PeerId,
    operation: &'a str,
    header: Header,
    path: &'a str,
}

impl Effect<'_> {
    /// The one order (Law 3): retain the inverse durably — or refuse on the
    /// record — then mutate, then ledger the registration with the
    /// caller's attribution (Law 2). Answers the 8-byte LE effect id.
    async fn commit(
        self,
        prior: Prior,
        mutate: impl Future<Output = std::io::Result<()>>,
    ) -> Result<Vec<u8>, KernelError> {
        let (provider, operation, path) = (self.provider, self.operation, self.path);
        let attribution = provider.attribution(self.caller);
        let id = match provider.retain(self.header, prior).await {
            Ok(id) => id,
            Err(refused) => {
                let error = refusal(
                    ErrorCode::PluginFailed,
                    format!(
                        "fs {operation} {path:?} refused: inverse not durable ({})",
                        refused.message
                    ),
                );
                provider.sink.append(
                    LedgerEventKind::ErrorRecorded {
                        error: error.clone(),
                    },
                    attribution,
                );
                return Err(error);
            }
        };
        if let Err(refused) = mutate.await {
            provider.release(id).await;
            return Err(io_refusal(operation, path, &refused));
        }
        provider.sink.append(
            LedgerEventKind::EffectRegistered {
                label: effect_label(operation, path, id),
            },
            attribution,
        );
        tracing::info!(effect = id, operation, path, "fs effect registered");
        Ok(id.to_le_bytes().to_vec())
    }

    async fn write(self, file: &Path, data: Vec<u8>) -> Result<Vec<u8>, KernelError> {
        let prior = match tokio::fs::read(file).await {
            Ok(bytes) => Prior::Content(bytes),
            Err(refused) if refused.kind() == std::io::ErrorKind::NotFound => Prior::Absent,
            Err(refused) => return Err(io_refusal("write", self.path, &refused)),
        };
        let mutate = async {
            if let Some(parent) = file.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::write(file, &data).await
        };
        self.commit(prior, mutate).await
    }

    async fn append(self, file: &Path, data: Vec<u8>) -> Result<Vec<u8>, KernelError> {
        let prior = match tokio::fs::metadata(file).await {
            Ok(metadata) => Prior::Length(metadata.len()),
            Err(refused) if refused.kind() == std::io::ErrorKind::NotFound => Prior::Absent,
            Err(refused) => return Err(io_refusal("append", self.path, &refused)),
        };
        let mutate = async {
            if let Some(parent) = file.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let mut handle = tokio::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(file)
                .await?;
            handle.write_all(&data).await?;
            handle.flush().await
        };
        self.commit(prior, mutate).await
    }

    async fn remove(self, file: &Path) -> Result<Vec<u8>, KernelError> {
        // A missing path is the typed not-found, and no effect: there is
        // nothing to withdraw.
        let prior = tokio::fs::read(file)
            .await
            .map_err(|refused| io_refusal("remove", self.path, &refused))?;
        self.commit(Prior::Content(prior), tokio::fs::remove_file(file))
            .await
    }
}

fn failed(refused: std::io::Error) -> KernelError {
    refusal(ErrorCode::EffectFailed, refused.to_string())
}

/// Replays one spilled inverse against the root (Law 3), on the resolved
/// path — a link planted under the label after the fact cannot redirect
/// the inverse outside the root.
pub(super) async fn apply_inverse(root: PathBuf, record: Record) -> Result<(), KernelError> {
    let label = record.header.label;
    let file = blocking(move || scope::resolve(&root, &label)).await?;
    match record.prior {
        Prior::Absent => match tokio::fs::remove_file(&file).await {
            Ok(()) => Ok(()),
            Err(refused) if refused.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(refused) => Err(failed(refused)),
        },
        Prior::Content(bytes) => {
            if let Some(parent) = file.parent() {
                tokio::fs::create_dir_all(parent).await.map_err(failed)?;
            }
            tokio::fs::write(&file, bytes).await.map_err(failed)
        }
        Prior::Length(length) => {
            let handle = tokio::fs::OpenOptions::new()
                .write(true)
                .open(&file)
                .await
                .map_err(failed)?;
            handle.set_len(length).await.map_err(failed)
        }
    }
}

/// The executable witness: the file now reads as the spilled prior
/// (content, absence, or length — the bundle's declared equality).
pub(super) fn witness(root: &Path, record: &Record) -> bool {
    let Ok(file) = scope::resolve(root, &record.header.label) else {
        return false;
    };
    match &record.prior {
        Prior::Absent => !file.exists(),
        Prior::Content(bytes) => std::fs::read(&file).is_ok_and(|current| current == *bytes),
        Prior::Length(length) => std::fs::metadata(&file).is_ok_and(|meta| meta.len() == *length),
    }
}
