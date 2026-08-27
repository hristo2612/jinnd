//! Pin-by-hash artifact admission (Law 5's provenance floor for M1;
//! constitution 05: v0.1 pins exact hashes — full signature envelopes are
//! post-M1). A mismatched hash refuses to load, and the refusal is recorded.

use std::sync::Arc;

use jinnd_api::{ErrorCode, KernelError, LedgerEventKind};

use crate::broker::LedgerSink;
use crate::sha256;

/// A component artifact admitted under its content hash. Constructible only
/// through [`admit`], so no unpinned bytes reach the host.
#[derive(Clone)]
pub struct PinnedArtifact {
    hash: String,
    bytes: Arc<[u8]>,
}

impl PinnedArtifact {
    /// The lower-hex SHA-256 the bytes were admitted under.
    pub fn hash(&self) -> &str {
        &self.hash
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl std::fmt::Debug for PinnedArtifact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedArtifact")
            .field("hash", &self.hash)
            .field("len", &self.bytes.len())
            .finish()
    }
}

/// Admits `bytes` under `expected_hash` — both outcomes are ledger events.
///
/// # Errors
///
/// [`ErrorCode::InvalidProfile`] on a hash mismatch; nothing is admitted.
pub fn admit(
    bytes: Vec<u8>,
    expected_hash: &str,
    ledger: &dyn LedgerSink,
) -> Result<PinnedArtifact, KernelError> {
    let actual = sha256::hex_digest(&bytes);
    if actual != expected_hash {
        ledger.append(
            LedgerEventKind::ArtifactRefused {
                detail: format!("pinned {expected_hash}, artifact is {actual}"),
            },
            None,
        );
        return Err(KernelError {
            code: ErrorCode::InvalidProfile,
            message: "artifact hash mismatch: refused (Law 5 pin-by-hash)".to_owned(),
            fiber: None,
        });
    }
    ledger.append(
        LedgerEventKind::ArtifactLoaded {
            hash: actual.clone(),
        },
        None,
    );
    Ok(PinnedArtifact {
        hash: actual,
        bytes: bytes.into(),
    })
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use std::sync::Arc;

    use jinnd_api::{ErrorCode, LedgerEventKind};

    use super::admit;
    use crate::broker_tests::CapturedLedger;
    use crate::sha256;

    #[test]
    fn matching_hash_admits_and_records() {
        let ledger = Arc::new(CapturedLedger::default());
        let bytes = b"component".to_vec();
        let hash = sha256::hex_digest(&bytes);
        let pinned = admit(bytes, &hash, ledger.as_ref())
            .unwrap_or_else(|error| panic!("refused: {error:?}"));
        assert_eq!(pinned.hash(), hash);
        let events = ledger.events.lock().unwrap_or_else(|p| p.into_inner());
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0].0,
            LedgerEventKind::ArtifactLoaded { hash: recorded } if *recorded == hash
        ));
    }

    #[test]
    fn mismatched_hash_refuses_and_the_refusal_is_recorded() {
        let ledger = Arc::new(CapturedLedger::default());
        let refused = admit(b"component".to_vec(), "not-the-hash", ledger.as_ref());
        assert_eq!(
            refused.err().map(|error| error.code),
            Some(ErrorCode::InvalidProfile)
        );
        let events = ledger.events.lock().unwrap_or_else(|p| p.into_inner());
        assert_eq!(events.len(), 1);
        assert!(matches!(
            &events[0].0,
            LedgerEventKind::ArtifactRefused { detail } if detail.contains("not-the-hash")
        ));
    }
}
