//! The base host-provider contracts as the kernel supplies them to guests
//! (R7: fs, process, net, keystore — exposed solely as contracts). Every
//! call here is one handle-less broker crossing: grant check → ledger
//! append → dispatch to the contract's live provider (Law 1 mechanical
//! closure, Law 2). The wire encodings are declared in `wit/plugin.wit`,
//! next to each operation — the contract files are the product (R12).

use crate::bindings;
use crate::instance::{HostState, Seat};

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
