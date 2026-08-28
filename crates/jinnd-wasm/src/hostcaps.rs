//! The base host-provider contracts as the kernel supplies them to guests
//! (R7: fs, process, net, keystore — exposed solely as contracts). Every
//! call here is one handle-less broker crossing: grant check → ledger
//! append → dispatch to the contract's live provider (Law 1 mechanical
//! closure, Law 2). The wire encodings are declared in `wit/plugin.wit`,
//! next to each operation — the contract files are the product (R12).

use std::sync::Arc;

use crate::alarms::{AlarmSpec, ArmRequest, CLOCK_CONTRACT};
use crate::bindings;
use crate::handle::{AlarmRecord, Registration};
use crate::instance::{HostState, Seat};
use crate::topics::EventTarget;

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

impl bindings::fs::Host for HostState {
    async fn read(&mut self, path: String) -> Result<Vec<u8>, bindings::types::KernelError> {
        dispatch(&self.seat, "jinn:fs", "read", path.into_bytes()).await
    }

    async fn write(
        &mut self,
        path: String,
        data: Vec<u8>,
    ) -> Result<(), bindings::types::KernelError> {
        dispatch(
            &self.seat,
            "jinn:fs",
            "write",
            prefixed(&[path.as_bytes()], &data),
        )
        .await
        .map(|_| ())
    }
}

impl bindings::process::Host for HostState {
    async fn run(
        &mut self,
        command: String,
        args: Vec<String>,
    ) -> Result<Vec<u8>, bindings::types::KernelError> {
        let mut segments: Vec<&[u8]> = vec![command.as_bytes()];
        segments.extend(args.iter().map(String::as_bytes));
        dispatch(&self.seat, "jinn:process", "run", prefixed(&segments, &[])).await
    }
}

impl bindings::net::Host for HostState {
    async fn request(
        &mut self,
        method: String,
        url: String,
        body: Vec<u8>,
    ) -> Result<Vec<u8>, bindings::types::KernelError> {
        dispatch(
            &self.seat,
            "jinn:net",
            "request",
            prefixed(&[method.as_bytes(), url.as_bytes()], &body),
        )
        .await
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
    state
        .seat
        .broker
        .check_grant(state.seat.peer, CLOCK_CONTRACT)
        .map_err(bindings::wire_error)?;
    state
        .seat
        .alarms
        .validate(&spec)
        .map_err(bindings::wire_error)?;
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

impl bindings::keystore::Host for HostState {
    async fn get(&mut self, key: String) -> Result<Vec<u8>, bindings::types::KernelError> {
        dispatch(&self.seat, "jinn:keystore", "get", key.into_bytes()).await
    }

    async fn put(
        &mut self,
        key: String,
        value: Vec<u8>,
    ) -> Result<(), bindings::types::KernelError> {
        dispatch(
            &self.seat,
            "jinn:keystore",
            "put",
            prefixed(&[key.as_bytes()], &value),
        )
        .await
        .map(|_| ())
    }
}
