//! The `jinn:auth` provider (M2-K21; contract bundle `contracts/jinn-auth`):
//! the ONE decision point for "may this caller issue a dispatch" on the
//! kernel's inbound surface. The kernel holds no inbound listener of its
//! own — a transport plugin does, through `jinn:net`, and the kernel sees
//! only bytes — so the kernel supplies the AUTHORITY: one credential, read
//! from the launcher-owned token file beside the data root ON EVERY CALL
//! (rotation and revocation without a restart), compared in constant
//! time over SHA-256 digests, refusing unless proven, and every decision
//! landing as an `AuthDecided` ledger row that carries the credential's
//! NAME and the presented value's DIGEST — never a credential byte (Law
//! 2, constitution 02 §Redaction; the M2-K8 keystore precedent).
//!
//! THREAT MODEL, WITH ITS LIMIT. In model: a process on this machine that
//! is not the launcher reaching the transport's socket — by accident, by
//! misconfiguration, or as a mistaken second instance — and a future
//! transport added without a check. NOT IN MODEL: a malicious process
//! running as the daemon's own uid. It can read whatever the daemon can
//! read, the credential file included, and nothing here holds against it.
//! A guarantee that cannot hold against same-uid is not a guarantee, so
//! none is claimed.
//!
//! NO BYPASS. This module reads no environment variable, no profile field
//! (the bundle declares no scope), no build flag, and has no test seam; a
//! test that needs a credential writes the file. `tests.rs` scans this
//! source for each of those rather than trusting this paragraph.

use std::path::PathBuf;
use std::sync::Arc;

use jinnd_api::{KernelError, KernelFuture};
use jinnd_ledger::Ledger;
use jinnd_wasm::{AUTH_CONTRACT, Broker, Peer, PeerId};

use super::wire::{Callers, unknown};

#[cfg(test)]
mod contract_tests;
#[cfg(test)]
mod tests;

/// Answer tag: granted; the principal's name follows as UTF-8.
const TAG_GRANTED: u8 = 0;
/// Answer tag: `unauthenticated`; the reason follows as UTF-8.
const TAG_UNAUTHENTICATED: u8 = 1;
/// The one credential's name: there is one operator.
const OPERATOR: &str = "operator";
/// A value shorter than this after trimming is not a credential
/// (metadata §credential `minimum-len`).
const MIN_LEN: usize = 16;
/// A credential file larger than this is not a credential — the read is
/// bounded (R9; metadata §credential `maximum-file`).
const MAX_FILE: u64 = 4096;

/// The provider: the ledger its decisions land on, the callers' attribution,
/// and WHERE the credential of record lives (never what it is — the file is
/// read per call and never cached, so a rotation needs no restart).
pub(crate) struct HostAuth {
    ledger: Ledger,
    callers: Callers,
    credential: PathBuf,
}

impl HostAuth {
    /// Registers the provider as a broker peer holding and providing the
    /// contract (providing is authority).
    ///
    /// # Errors
    ///
    /// The broker's refusal of the provision.
    pub(crate) fn register(
        broker: &Arc<Broker>,
        ledger: Ledger,
        credential: PathBuf,
    ) -> Result<(), KernelError> {
        let peer = broker.register_peer(None);
        broker.grant(peer, AUTH_CONTRACT);
        let provider = Arc::new(Self {
            ledger,
            callers: Callers::new(broker, AUTH_CONTRACT),
            credential,
        });
        broker.provide(peer, AUTH_CONTRACT, Arc::new(AuthPeer(provider)))
    }

    /// One decision. RED-FIRST SKELETON: refuses everything and records
    /// nothing — the suite in `tests.rs` is red against this body.
    async fn verify(&self, _caller: PeerId, _payload: Vec<u8>) -> Result<Vec<u8>, KernelError> {
        let _ = (&self.ledger, &self.callers, &self.credential);
        Ok(answer(TAG_UNAUTHENTICATED, "not implemented"))
    }
}

/// The bundle's wire: one tag byte, then UTF-8.
fn answer(tag: u8, text: &str) -> Vec<u8> {
    let mut wire = vec![tag];
    wire.extend(text.as_bytes());
    wire
}

/// The provider's broker face.
struct AuthPeer(Arc<HostAuth>);

impl Peer for AuthPeer {
    fn call(
        &self,
        caller: PeerId,
        _contract: &str,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let provider = Arc::clone(&self.0);
        let operation = operation.to_owned();
        Box::pin(async move {
            match operation.as_str() {
                "verify" => provider.verify(caller, payload).await,
                other => Err(unknown(AUTH_CONTRACT, other)),
            }
        })
    }
}
