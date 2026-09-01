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
//! test that needs a credential writes the file. `source_tests.rs` scans
//! this source for each of those rather than trusting this paragraph.

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;

use jinnd_api::{KernelError, KernelFuture, LedgerEventKind};
use jinnd_ledger::Ledger;
use jinnd_wasm::{AUTH_CONTRACT, Broker, Peer, PeerId, hex_digest};

use super::wire::{Callers, unknown};

#[cfg(test)]
mod contract_tests;
#[cfg(test)]
mod source_tests;
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

    /// The credential of record, read NOW — never cached, so a rotation or
    /// a revocation takes effect on the next call with no restart. Each
    /// way the file fails to be a credential is its own named
    /// precondition (metadata §credential), and every one of them refuses.
    async fn credential(&self) -> Result<Vec<u8>, &'static str> {
        let meta = tokio::fs::metadata(&self.credential)
            .await
            .map_err(|_| "credential file absent or unreadable")?;
        // Another uid that can read the file holds the credential: a
        // group- or world-accessible file is not one.
        if meta.permissions().mode() & 0o077 != 0 {
            return Err("credential file is group- or world-accessible");
        }
        if meta.len() > MAX_FILE {
            return Err("credential file exceeds the size bound");
        }
        let bytes = tokio::fs::read(&self.credential)
            .await
            .map_err(|_| "credential file absent or unreadable")?;
        let trimmed = bytes.trim_ascii();
        if trimmed.len() < MIN_LEN {
            return Err("credential is too short");
        }
        Ok(trimmed.to_vec())
    }

    /// One decision: deny unless the presented value proves the
    /// credential of record; ONE `AuthDecided` row either way, under the
    /// caller's attribution, carrying the name (on a grant) and the
    /// presented value's DIGEST — never its bytes. Nothing is mutated and
    /// nothing is registered on this path, so a refusal has reached no
    /// effect by construction; the answer is the bundle's wire.
    async fn verify(&self, caller: PeerId, payload: Vec<u8>) -> Result<Vec<u8>, KernelError> {
        let (fiber, entry) = self.callers.attribution(caller);
        let presented = hex_digest(&payload);
        let decision = match self.credential().await {
            Ok(secret)
                if constant_time_eq(hex_digest(&secret).as_bytes(), presented.as_bytes()) =>
            {
                Ok(())
            }
            Ok(_) => Err("presented credential does not match"),
            Err(why) => {
                // The operator's log line: a file that is not a credential
                // is a misconfiguration, and silence would hide it.
                tracing::warn!(path = %self.credential.display(), why, "jinn:auth has no usable credential");
                Err(why)
            }
        };
        let granted = decision.is_ok();
        self.ledger.record(
            LedgerEventKind::AuthDecided {
                name: granted.then(|| OPERATOR.to_owned()),
                presented,
                granted,
            },
            entry,
            fiber,
        );
        Ok(match decision {
            Ok(()) => answer(TAG_GRANTED, OPERATOR),
            Err(why) => answer(TAG_UNAUTHENTICATED, why),
        })
    }
}

/// Equal length and equal bytes, examined in full whatever the input: no
/// early exit for a timing side channel to read.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut differ = u8::from(left.len() != right.len());
    for (a, b) in left.iter().zip(right.iter()) {
        differ |= a ^ b;
    }
    differ == 0
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
