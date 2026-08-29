//! The operator contracts' shared plumbing (M2-K7; R10 file hygiene): the
//! broker kept weakly for caller attribution, the bounds-checked request
//! reader, and the answer encodings the bundles under `contracts/`
//! declare — JSON records for structured answers, LE scalars otherwise.

use std::sync::{Arc, Weak};

use jinnd_api::{EntryId, ErrorCode, FiberId, KernelError};
use jinnd_wasm::{Broker, GrantScope, PeerId};

use crate::support::error;

/// A provider's view of who is calling (R4): fiber and entry attribution
/// and the typed authority the caller holds under the contract.
pub(crate) struct Callers {
    broker: Weak<Broker>,
    contract: &'static str,
}

impl Callers {
    pub(crate) fn new(broker: &Arc<Broker>, contract: &'static str) -> Self {
        Self {
            broker: Arc::downgrade(broker),
            contract,
        }
    }

    fn broker(&self) -> Option<Arc<Broker>> {
        self.broker.upgrade()
    }

    /// `(fiber, entry)` of one calling peer.
    pub(crate) fn attribution(&self, caller: PeerId) -> (Option<FiberId>, Option<EntryId>) {
        match self.broker() {
            Some(broker) => (
                broker.attribution(caller),
                broker.entry_of(caller).map(EntryId),
            ),
            None => (None, None),
        }
    }

    /// The typed authority `caller` holds this contract under.
    pub(crate) fn policy(&self, caller: PeerId) -> Option<GrantScope> {
        self.broker()
            .and_then(|broker| broker.policy(caller, self.contract))
    }
}

/// A bounds-checked cursor over one request payload.
pub(crate) struct Reader<'a> {
    wire: &'a [u8],
    what: &'static str,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(wire: &'a [u8], what: &'static str) -> Self {
        Self { wire, what }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], KernelError> {
        let (head, rest) = self.wire.split_at_checked(count).ok_or_else(|| {
            error(
                ErrorCode::PluginFailed,
                format!("malformed {} payload", self.what),
            )
        })?;
        self.wire = rest;
        Ok(head)
    }

    pub(crate) fn u32(&mut self) -> Result<u32, KernelError> {
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(self.take(4)?);
        Ok(u32::from_le_bytes(bytes))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, KernelError> {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(self.take(8)?);
        Ok(u64::from_le_bytes(bytes))
    }

    /// One u32-LE length-prefixed UTF-8 segment.
    pub(crate) fn text(&mut self) -> Result<String, KernelError> {
        let length = self.u32()? as usize;
        let bytes = self.take(length)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| {
            error(
                ErrorCode::PluginFailed,
                format!("malformed {} payload: not UTF-8", self.what),
            )
        })
    }

    pub(crate) fn rest(self) -> &'a [u8] {
        self.wire
    }
}

/// A JSON answer, as the bundles declare structured answers.
pub(crate) fn json(value: &serde_json::Value) -> Vec<u8> {
    value.to_string().into_bytes()
}

/// An operation the provider does not answer: typed, never a hang.
pub(crate) fn unknown(contract: &str, operation: &str) -> KernelError {
    error(
        ErrorCode::PluginFailed,
        format!("unknown {contract} operation {operation:?}"),
    )
}
