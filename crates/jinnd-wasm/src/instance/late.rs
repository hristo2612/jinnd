//! The supervisor's off-path answers: the sealed-seat refusal, late
//! registration commit, and the refuse-everything drain of an instance that
//! never came up. Split from `instance.rs` by responsibility (R10 file
//! hygiene).

use std::sync::Arc;

use jinnd_api::KernelError;
use tokio::sync::mpsc;
use wasmtime::Store;

use crate::alarms::ArmRequest;
use crate::handle::{ActivationOutcome, Command, Registration};
use crate::topics::EventTarget;

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
/// wait for the next drain. While the seat is STAGED they wait too —
/// recorded, not routed — and the commit's flip runs this again: a listen
/// or alarm recorded meanwhile is routed THEN, before it joins the journal
/// (M2-K26 amendment 2, harness #53; R9).
pub(super) fn commit_late(store: &mut Store<HostState>) {
    let data = store.data_mut();
    if data.outcome.registrations.is_empty() || data.staging() {
        return;
    }
    let mut late = std::mem::take(&mut data.outcome.registrations);
    route_recorded(data, &mut late);
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
            Command::Undo { reply, .. } => drop(reply.send(Err(error.clone()))),
            Command::Restore { reply, .. } => {
                drop(reply.send((Err(error.clone()), ActivationOutcome::default())))
            }
            Command::HandleCall { reply, .. }
            | Command::Deliver { reply, .. }
            | Command::Snapshot { reply } => drop(reply.send(Err(error.clone()))),
        }
    }
}

/// Routes the listens and alarms recorded while the seat was staged (their
/// ids absent), against this instance's own face — exactly what a live
/// registration does at request time (R5: one journal, every row routed).
fn route_recorded(data: &HostState, late: &mut [Registration]) {
    let seat = &data.seat;
    for registration in late {
        match registration {
            Registration::Listen(record) if record.id.is_none() => {
                record.id = Some(seat.topics.listen_within(
                    &record.topic,
                    seat.context,
                    record.token,
                    seat.fiber,
                    record.budget,
                    data.face.clone() as Arc<dyn EventTarget>,
                ));
            }
            Registration::Alarm(record) if record.id.is_none() => {
                record.id = Some(seat.alarms.arm(ArmRequest {
                    spec: record.spec,
                    token: record.token,
                    fiber: seat.fiber,
                    target: data.face.clone() as Arc<dyn EventTarget>,
                }));
            }
            _ => {}
        }
    }
}
