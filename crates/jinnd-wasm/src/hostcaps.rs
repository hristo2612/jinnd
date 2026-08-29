//! The base host-provider contracts as the kernel supplies them to guests
//! (R7: fs, process, net, keystore — exposed solely as contracts). Every
//! call here is one handle-less broker crossing: grant check → ledger
//! append → dispatch to the contract's live provider (Law 1 mechanical
//! closure, Law 2). The wire encodings are declared in `wit/plugin.wit`,
//! next to each operation — the contract files are the product (R12).

use std::sync::Arc;

use crate::alarms::{AlarmSpec, ArmRequest, CLOCK_CONTRACT};
use crate::bindings;
use crate::handle::{AlarmRecord, HostRecord, Registration};
use crate::hostfs::wire::{FileMeta, decode_metas};
use crate::hostfs::{FS_CONTRACT, effect_label};
use crate::hostkeystore::{KEYSTORE_CONTRACT, keystore_label};
use crate::instance::{HostState, Seat};
use crate::topics::EventTarget;

mod procnet;

pub use procnet::{NET_CONTRACT, PROCESS_CONTRACT, registration_label};

/// One u32-LE length-prefixed segment followed by the free tail — the wire
/// shape `wit/plugin.wit` declares for multi-field operations.
fn prefixed(segments: &[&[u8]], tail: &[u8]) -> Vec<u8> {
    let mut wire = Vec::new();
    for segment in segments {
        wire.extend(
            u32::try_from(segment.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        wire.extend(*segment);
    }
    wire.extend(tail);
    wire
}

async fn dispatch(
    seat: &Seat,
    contract: &str,
    operation: &str,
    payload: Vec<u8>,
) -> Result<Vec<u8>, bindings::types::KernelError> {
    seat.broker
        .dispatch(seat.peer, contract, operation, payload)
        .await
        .map_err(bindings::wire_error)
}

/// The typed `file-meta` off the provider's wire (M2-K3).
fn file_meta(meta: FileMeta) -> bindings::fs::FileMeta {
    bindings::fs::FileMeta {
        path: meta.path,
        size: meta.size,
        modified_ms: meta.modified_ms,
        is_dir: meta.is_dir,
    }
}

/// One `jinn:fs` read: the crossing under the bundle's own error (R12).
async fn fs_read(
    seat: &Seat,
    operation: &str,
    path: String,
) -> Result<Vec<u8>, bindings::fs::FsError> {
    seat.broker
        .dispatch(seat.peer, FS_CONTRACT, operation, path.into_bytes())
        .await
        .map_err(bindings::fs_error)
}

/// One `jinn:fs` revertible effect (M2-K3): the keyed crossing, then the
/// answered effect id JOINS THIS INSTANCE'S JOURNAL under the provider's
/// own label — teardown withdraws it LIFO with every other registration,
/// through the broker's current provider (R5, M1-P9b; round-2 blocker 1).
async fn fs_effect(
    state: &mut HostState,
    operation: &str,
    path: &str,
    key: &str,
    data: &[u8],
) -> Result<(), bindings::fs::FsError> {
    state
        .admit(&format!("fs {operation}"))
        .map_err(bindings::fs_error)?;
    let payload = prefixed(&[path.as_bytes(), key.as_bytes()], data);
    let answer = state
        .seat
        .broker
        .dispatch(state.seat.peer, FS_CONTRACT, operation, payload)
        .await
        .map_err(bindings::fs_error)?;
    let mut bytes = [0u8; 8];
    let taken = answer.len().min(8);
    bytes[..taken].copy_from_slice(&answer[..taken]);
    let effect = u64::from_le_bytes(bytes);
    state
        .outcome
        .registrations
        .push(Registration::Host(HostRecord {
            contract: FS_CONTRACT.to_owned(),
            label: effect_label(operation, path, effect),
            effect,
        }));
    Ok(())
}

impl bindings::fs::Host for HostState {
    async fn read(&mut self, path: String) -> Result<Vec<u8>, bindings::fs::FsError> {
        fs_read(&self.seat, "read", path).await
    }

    async fn list(
        &mut self,
        path: String,
    ) -> Result<Vec<bindings::fs::FileMeta>, bindings::fs::FsError> {
        let answer = fs_read(&self.seat, "list", path).await?;
        let metas = decode_metas(&answer).map_err(bindings::fs_error)?;
        Ok(metas.into_iter().map(file_meta).collect())
    }

    async fn meta(
        &mut self,
        path: String,
    ) -> Result<bindings::fs::FileMeta, bindings::fs::FsError> {
        let answer = fs_read(&self.seat, "meta", path).await?;
        let metas = decode_metas(&answer).map_err(bindings::fs_error)?;
        metas
            .into_iter()
            .next()
            .map(file_meta)
            .ok_or_else(|| bindings::fs::FsError::Io("empty fs meta answer".to_owned()))
    }

    async fn write(
        &mut self,
        path: String,
        bytes: Vec<u8>,
        idempotency_key: String,
    ) -> Result<(), bindings::fs::FsError> {
        fs_effect(self, "write", &path, &idempotency_key, &bytes).await
    }

    async fn append(
        &mut self,
        path: String,
        bytes: Vec<u8>,
        idempotency_key: String,
    ) -> Result<(), bindings::fs::FsError> {
        fs_effect(self, "append", &path, &idempotency_key, &bytes).await
    }

    async fn remove(
        &mut self,
        path: String,
        idempotency_key: String,
    ) -> Result<(), bindings::fs::FsError> {
        fs_effect(self, "remove", &path, &idempotency_key, &[]).await
    }
}

/// One alarm request (M2-K2): grant-checked and floor-validated where
/// failing is allowed, recorded in the activation journal as an effect
/// whose undo cancels (R5). A staged request is recorded, not armed —
/// the swap commit arms it against the new instance's own face (R8).
fn alarm(
    state: &mut HostState,
    spec: AlarmSpec,
    token: u64,
) -> Result<u64, bindings::types::KernelError> {
    state.admit("alarm").map_err(bindings::wire_error)?;
    let operation = match spec {
        AlarmSpec::At(_) => "alarm-at",
        AlarmSpec::Every(_) => "alarm-every",
    };
    state
        .seat
        .broker
        .check_grant(state.seat.peer, CLOCK_CONTRACT)
        .and_then(|()| {
            state
                .seat
                .broker
                .check_op(state.seat.peer, CLOCK_CONTRACT, operation)
        })
        .map_err(bindings::wire_error)?;
    // The floor is THIS entry's own grant scope (M2-K2, R9): grants cap
    // how fine a timer an entry may hold, never assembly-wide.
    crate::alarms::validate(&spec, state.seat.clock_floor_ms).map_err(bindings::wire_error)?;
    let label = spec.label();
    if state.seat.staging {
        state
            .outcome
            .registrations
            .push(Registration::Alarm(AlarmRecord {
                label,
                spec,
                token,
                id: None,
            }));
        return Ok(0);
    }
    // The delivery target is THIS instance's own face, like a listener's:
    // a token pairs with the instance that minted it.
    let id = state.seat.alarms.arm(ArmRequest {
        spec,
        token,
        fiber: state.seat.fiber,
        target: Arc::clone(&state.face) as Arc<dyn EventTarget>,
    });
    state
        .outcome
        .registrations
        .push(Registration::Alarm(AlarmRecord {
            label,
            spec,
            token,
            id: Some(id),
        }));
    Ok(id)
}

impl bindings::clock::Host for HostState {
    async fn now(&mut self) -> Result<u64, bindings::types::KernelError> {
        let answer = dispatch(&self.seat, CLOCK_CONTRACT, "now", Vec::new()).await?;
        let mut bytes = [0u8; 8];
        let taken = answer.len().min(8);
        bytes[..taken].copy_from_slice(&answer[..taken]);
        Ok(u64::from_le_bytes(bytes))
    }

    async fn alarm_at(
        &mut self,
        instant_ms: u64,
        token: u64,
    ) -> Result<u64, bindings::types::KernelError> {
        alarm(self, AlarmSpec::At(instant_ms), token)
    }

    async fn alarm_every(
        &mut self,
        period_ms: u64,
        token: u64,
    ) -> Result<u64, bindings::types::KernelError> {
        alarm(self, AlarmSpec::Every(period_ms), token)
    }
}

/// One `jinn:keystore` revertible effect (M2-K8): the crossing, then the
/// answered effect id JOINS THIS INSTANCE'S JOURNAL under the provider's
/// label — withdrawn LIFO with the rest, through the broker's current
/// provider (R5). The label names the key, never the value.
async fn keystore_effect(
    state: &mut HostState,
    operation: &str,
    key: &str,
    payload: Vec<u8>,
) -> Result<(), bindings::keystore::KeystoreError> {
    state.admit(&format!("keystore {operation}"))?;
    let answer = state
        .seat
        .broker
        .dispatch(state.seat.peer, KEYSTORE_CONTRACT, operation, payload)
        .await?;
    let effect = crate::hostwire::decode_handle(&answer)?;
    state
        .outcome
        .registrations
        .push(Registration::Host(HostRecord {
            contract: KEYSTORE_CONTRACT.to_owned(),
            label: keystore_label(operation, key, effect),
            effect,
        }));
    Ok(())
}

impl bindings::keystore::Host for HostState {
    async fn get(&mut self, key: String) -> Result<Vec<u8>, bindings::keystore::KeystoreError> {
        Ok(self
            .seat
            .broker
            .dispatch(self.seat.peer, KEYSTORE_CONTRACT, "get", key.into_bytes())
            .await?)
    }

    async fn put(
        &mut self,
        key: String,
        value: Vec<u8>,
    ) -> Result<(), bindings::keystore::KeystoreError> {
        let payload = prefixed(&[key.as_bytes()], &value);
        keystore_effect(self, "put", &key, payload).await
    }

    async fn delete(&mut self, key: String) -> Result<(), bindings::keystore::KeystoreError> {
        keystore_effect(self, "delete", &key, key.clone().into_bytes()).await
    }

    async fn list(&mut self) -> Result<Vec<String>, bindings::keystore::KeystoreError> {
        let answer = self
            .seat
            .broker
            .dispatch(self.seat.peer, KEYSTORE_CONTRACT, "list", Vec::new())
            .await?;
        let mut reader = crate::hostwire::Reader::new(&answer, "keystore list answer");
        let mut names = Vec::new();
        while !reader.is_empty() {
            names.push(reader.text()?);
        }
        Ok(names)
    }
}
