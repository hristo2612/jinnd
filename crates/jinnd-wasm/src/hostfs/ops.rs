//! The `jinn:fs` operations behind the provider's broker face (M2-K3): the
//! three reads, the three revertible effects, and the inverse/witness the
//! retention records replay. Every effect follows one order — capture the
//! prior, make it durable, mutate, ledger — and refuses on the record when
//! the inverse cannot be made durable (Law 3; fail-closed like K2's grant
//! admission).

use std::path::Path;
use std::time::UNIX_EPOCH;

use jinnd_api::{ErrorCode, KernelError, LedgerEventKind};
use tokio::io::AsyncWriteExt;

use super::retention::{Prior, Record};
use super::wire::{FileMeta, encode_metas, path_payload, split_write};
use super::{HostFs, contained};
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
        "read" => read(provider, &path_payload(payload, "read")?).await,
        "list" => list(provider, &path_payload(payload, "list")?).await,
        "meta" => meta(provider, &path_payload(payload, "meta")?).await,
        "write" => {
            let (path, data) = split_write(&payload)?;
            write(provider, caller, &path, data).await?;
            Ok(Vec::new())
        }
        "append" => {
            let (path, data) = split_write(&payload)?;
            append(provider, caller, &path, data).await?;
            Ok(Vec::new())
        }
        "remove" => {
            remove(provider, caller, &path_payload(payload, "remove")?).await?;
            Ok(Vec::new())
        }
        other => Err(refusal(
            ErrorCode::PluginFailed,
            format!("unknown fs operation {other:?}"),
        )),
    }
}

async fn read(provider: &HostFs, path: &str) -> Result<Vec<u8>, KernelError> {
    let file = contained(&provider.root, path)?;
    tokio::fs::read(&file)
        .await
        .map_err(|refused| io_refusal("read", path, &refused))
}

async fn list(provider: &HostFs, path: &str) -> Result<Vec<u8>, KernelError> {
    let dir = contained(&provider.root, path)?;
    let mut entries = tokio::fs::read_dir(&dir)
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

async fn meta(provider: &HostFs, path: &str) -> Result<Vec<u8>, KernelError> {
    let file = contained(&provider.root, path)?;
    let metadata = tokio::fs::metadata(&file)
        .await
        .map_err(|refused| io_refusal("meta", path, &refused))?;
    Ok(encode_metas(&[meta_of(
        path.trim_start_matches('/').to_owned(),
        &metadata,
    )]))
}

/// The prior content, or prior absence; any other io refusal is the
/// operation's.
async fn prior_content(operation: &str, path: &str, file: &Path) -> Result<Prior, KernelError> {
    match tokio::fs::read(file).await {
        Ok(bytes) => Ok(Prior::Content(bytes)),
        Err(refused) if refused.kind() == std::io::ErrorKind::NotFound => Ok(Prior::Absent),
        Err(refused) => Err(io_refusal(operation, path, &refused)),
    }
}

/// Retains the inverse durably, or refuses the effect on the record.
async fn retain(
    provider: &HostFs,
    caller: PeerId,
    operation: &str,
    label: &str,
    prior: Prior,
) -> Result<u64, KernelError> {
    match provider.retain(label, prior).await {
        Ok(id) => Ok(id),
        Err(refused) => {
            let error = refusal(
                ErrorCode::EffectFailed,
                format!(
                    "fs {operation} {label:?} refused: inverse not durable ({})",
                    refused.message
                ),
            );
            provider.sink.append(
                LedgerEventKind::ErrorRecorded {
                    error: error.clone(),
                },
                provider.attribution(caller),
            );
            Err(error)
        }
    }
}

/// Ledgers the registered effect (Law 2) with the caller's attribution.
fn registered(provider: &HostFs, caller: PeerId, operation: &str, label: &str, id: u64) {
    provider.sink.append(
        LedgerEventKind::EffectRegistered {
            label: format!("fs {operation} {label} [effect {id}]"),
        },
        provider.attribution(caller),
    );
    tracing::info!(effect = id, operation, path = %label, "fs effect registered");
}

async fn write(
    provider: &HostFs,
    caller: PeerId,
    path: &str,
    data: Vec<u8>,
) -> Result<(), KernelError> {
    let file = contained(&provider.root, path)?;
    let label = path.trim_start_matches('/');
    let prior = prior_content("write", path, &file).await?;
    let id = retain(provider, caller, "write", label, prior).await?;
    let mutate = async {
        if let Some(parent) = file.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&file, &data).await
    };
    if let Err(refused) = mutate.await {
        provider.release(id).await;
        return Err(io_refusal("write", path, &refused));
    }
    registered(provider, caller, "write", label, id);
    Ok(())
}

async fn append(
    provider: &HostFs,
    caller: PeerId,
    path: &str,
    data: Vec<u8>,
) -> Result<(), KernelError> {
    let file = contained(&provider.root, path)?;
    let label = path.trim_start_matches('/');
    let prior = match tokio::fs::metadata(&file).await {
        Ok(metadata) => Prior::Length(metadata.len()),
        Err(refused) if refused.kind() == std::io::ErrorKind::NotFound => Prior::Absent,
        Err(refused) => return Err(io_refusal("append", path, &refused)),
    };
    let id = retain(provider, caller, "append", label, prior).await?;
    let mutate = async {
        if let Some(parent) = file.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut handle = tokio::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&file)
            .await?;
        handle.write_all(&data).await?;
        handle.flush().await
    };
    if let Err(refused) = mutate.await {
        provider.release(id).await;
        return Err(io_refusal("append", path, &refused));
    }
    registered(provider, caller, "append", label, id);
    Ok(())
}

async fn remove(provider: &HostFs, caller: PeerId, path: &str) -> Result<(), KernelError> {
    let file = contained(&provider.root, path)?;
    let label = path.trim_start_matches('/');
    // A missing path is the typed not-found, and no effect: there is
    // nothing to withdraw.
    let prior = tokio::fs::read(&file)
        .await
        .map_err(|refused| io_refusal("remove", path, &refused))?;
    let id = retain(provider, caller, "remove", label, Prior::Content(prior)).await?;
    if let Err(refused) = tokio::fs::remove_file(&file).await {
        provider.release(id).await;
        return Err(io_refusal("remove", path, &refused));
    }
    registered(provider, caller, "remove", label, id);
    Ok(())
}

fn failed(refused: std::io::Error) -> KernelError {
    refusal(ErrorCode::EffectFailed, refused.to_string())
}

/// Replays one spilled inverse against the root (Law 3).
pub(super) async fn apply_inverse(root: &Path, record: &Record) -> Result<(), KernelError> {
    let file = contained(root, &record.label)?;
    match &record.prior {
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
            handle.set_len(*length).await.map_err(failed)
        }
    }
}

/// The executable witness: the file now reads as the spilled prior
/// (content, absence, or length — the bundle's declared equality).
pub(super) fn witness(root: &Path, record: &Record) -> bool {
    let Ok(file) = contained(root, &record.label) else {
        return false;
    };
    match &record.prior {
        Prior::Absent => !file.exists(),
        Prior::Content(bytes) => std::fs::read(&file).is_ok_and(|current| current == *bytes),
        Prior::Length(length) => std::fs::metadata(&file).is_ok_and(|meta| meta.len() == *length),
    }
}
