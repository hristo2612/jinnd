//! The supervisor's off-path answers: the sealed-seat refusal, late
//! registration commit, and the refuse-everything drain of an instance that
//! never came up. Split from `instance.rs` by responsibility (R10 file
//! hygiene).

use jinnd_api::KernelError;
use tokio::sync::mpsc;
use wasmtime::Store;

use crate::handle::{ActivationOutcome, Command};

use super::HostState;

/// The refusal a sealed seat answers (M2-K4): typed as the inactive
/// context it is — the instance's journal is closed for withdrawal.
pub(crate) fn sealed_error() -> KernelError {
    KernelError {
        code: jinnd_api::ErrorCode::InactiveContext,
        message: "refused: the seat's journal is sealed for withdrawal".to_owned(),
        fiber: None,
    }
}

/// Commits registrations a guest made outside its activation (from a
/// `handle-event` or `handle-call`) into the live seat's journal (M2-K3
/// round 2; R5, I1): an effect registered late is withdrawn LIFO with the
/// rest, never orphaned in the store. With no seat installed yet they
/// wait for the next drain.
pub(super) fn commit_late(store: &mut Store<HostState>) {
    let data = store.data_mut();
    if data.outcome.registrations.is_empty() {
        return;
    }
    let late = std::mem::take(&mut data.outcome.registrations);
    if let Some(slot) = &data.seat.slot {
        if let Some(kept) = slot.extend(late) {
            data.outcome.registrations = kept;
        }
    } else {
        data.outcome.registrations = late;
    }
}

/// Answers every remaining command with `error` — an instance that failed to
/// come up never hangs its callers.
pub(super) async fn refuse_all(rx: &mut mpsc::Receiver<Command>, error: KernelError) {
    while let Some(command) = rx.recv().await {
        match command {
            Command::Shutdown => return,
            Command::Seal { reply } => drop(reply.send(())),
            Command::Activate { reply, .. } => {
                let _ = reply.send((Err(error.clone()), ActivationOutcome::default()));
            }
            Command::Check { reply, .. } => drop(reply.send(false)),
            Command::Undo { reply, .. } | Command::Restore { reply, .. } => {
                drop(reply.send(Err(error.clone())))
            }
            Command::HandleCall { reply, .. }
            | Command::Deliver { reply, .. }
            | Command::Snapshot { reply } => drop(reply.send(Err(error.clone()))),
        }
    }
}
