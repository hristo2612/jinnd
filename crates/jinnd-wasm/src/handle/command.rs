//! The supervisor command set behind [`InstanceHandle`](super::InstanceHandle)
//! and the "gone" error every call on a dead instance answers with. Split
//! from `handle.rs` by responsibility (R10 file hygiene).

use jinnd_api::{ErrorCode, KernelError};
use tokio::sync::oneshot;

use crate::peer::PeerId;

use super::ActivationOutcome;

pub(crate) enum Command {
    Activate {
        config: Vec<u8>,
        reply: oneshot::Sender<(Result<(), KernelError>, ActivationOutcome)>,
    },
    Check {
        consumer: PeerId,
        reply: oneshot::Sender<bool>,
    },
    Undo {
        token: u64,
        reply: oneshot::Sender<Result<(), KernelError>>,
    },
    HandleCall {
        caller: PeerId,
        contract: String,
        operation: String,
        payload: Vec<u8>,
        reply: oneshot::Sender<Result<Vec<u8>, KernelError>>,
    },
    Deliver {
        token: u64,
        topic: String,
        payload: Vec<u8>,
        reply: oneshot::Sender<Result<Vec<u8>, KernelError>>,
    },
    Snapshot {
        reply: oneshot::Sender<Result<Vec<u8>, KernelError>>,
    },
    Restore {
        blob: Vec<u8>,
        reply: oneshot::Sender<Result<(), KernelError>>,
    },
    /// Seals the instance (M2-K4): answered once every guest entry already
    /// in flight has returned; afterwards no activation, call, or delivery
    /// runs the guest — only inverses and disposal.
    Seal {
        reply: oneshot::Sender<()>,
    },
    Shutdown,
}

pub(crate) fn gone() -> KernelError {
    KernelError {
        code: ErrorCode::PluginFailed,
        message: "the instance is gone".to_owned(),
        fiber: None,
    }
}
