//! The Mode-1 commit (R8): a staged activation becomes the slot's live
//! seat inside the batch claim's critical section. Split from `slot.rs`
//! by responsibility (R10 file hygiene).

use std::sync::Arc;

use jinnd_api::{FiberId, LedgerEventKind};

use crate::alarms::{Alarms, ArmRequest};
use crate::broker::Broker;
use crate::handle::{ActivationOutcome, InstanceHandle, Registration, peer_face};
use crate::peer::{LedgerSink, Peer, PeerId};
use crate::topics::{EventTarget, LocalTopics, Rebind};

use super::{SeatState, SharedSlot};

/// Commits a staged activation as the slot's live seat — the Mode-1 commit
/// (R8), run INSIDE the batch claim's critical section: every operation
/// here is infallible sync bookkeeping (round-3 ruling — nothing may fail,
/// block, await, or call guest code). The staged listens register
/// atomically against the NEW instance's own delivery face, the new seat
/// replaces the old for contract-call routing, provisions the predecessor
/// did not hold are provided through the slot face (kept ones never
/// re-provide, so their generation and live handles stand — Mode-1
/// continuity), orphaned ones withdraw. The staged outcome is COMMITTED,
/// exactly as an initial activation registers its own (round-2 blocker-4
/// ruling; R5, I1). The displaced seat is handed back for disposal AFTER
/// the critical section — dispose-only: the handoff transferred its
/// contribution to the successor, whose own activation registered its own
/// inverses (warm until commit, R8).
#[allow(clippy::too_many_arguments)]
#[must_use = "the displaced seat must be disposed after the critical section"]
pub fn commit_staged(
    slot: &Arc<SharedSlot>,
    staged: InstanceHandle,
    outcome: ActivationOutcome,
    broker: &Broker,
    topics: &LocalTopics,
    alarms: &Arc<Alarms>,
    peer: PeerId,
    fiber: Option<FiberId>,
    context: u64,
    ledger: &dyn LedgerSink,
) -> Option<SeatState> {
    let (old_provisions, mut old_listens, old_alarms) = slot.registrations();
    // A config restart commits through this same call (M2-K26 (b)): the
    // rows to replace are then the fiber's TOMBSTONES, left by the
    // suspension, not a displaced seat's live listens.
    if let Some(fiber) = fiber {
        old_listens.extend(topics.entombed(fiber).into_iter().map(|(id, _)| id));
    }
    let live = staged.clone();
    let face = peer_face(&staged);
    let rebinds: Vec<Rebind> = outcome
        .listens()
        .map(|record| Rebind {
            topic: record.topic.clone(),
            context,
            token: record.token,
            fiber,
            budget: record.budget,
            target: Arc::clone(&face) as Arc<dyn EventTarget>,
        })
        .collect();
    let ids = topics.rebind(&old_listens, rebinds);
    // Host-provider wakes follow the seat: the successor's face is the
    // peer's delivery target from this instant (M2-K7, R8).
    broker.attach_target(peer, Arc::clone(&face) as Arc<dyn EventTarget>);
    // The staged alarms arm against the NEW instance's own face, the
    // displaced seat's alarms cancel — live alarms survive the swap through
    // the staged outcome, exactly like any effect (M2-K2, R8); their floor
    // was validated at staging, so nothing here can fail.
    let arm_requests: Vec<ArmRequest> = outcome
        .alarms()
        .map(|record| ArmRequest {
            spec: record.spec,
            token: record.token,
            fiber,
            target: Arc::clone(&face) as Arc<dyn EventTarget>,
        })
        .collect();
    let alarm_ids = alarms.rebind(&old_alarms, arm_requests);
    // The minted ids land back in the journal, in order: the committed seat
    // carries ONE registration list, exactly as an initial activation's.
    let mut registrations = outcome.registrations;
    let mut minted = ids.into_iter();
    let mut minted_alarms = alarm_ids.into_iter();
    for registration in &mut registrations {
        match registration {
            Registration::Listen(record) => record.id = minted.next(),
            Registration::Alarm(record) => record.id = minted_alarms.next(),
            _ => {}
        }
    }
    let new_provisions: Vec<String> = registrations
        .iter()
        .filter_map(|registration| match registration {
            Registration::Provision { contract } => Some(contract.clone()),
            _ => None,
        })
        .collect();
    let displaced = slot.install(SeatState {
        instance: staged,
        registrations,
    });
    for contract in &new_provisions {
        if !old_provisions.contains(contract) {
            let provided =
                broker.provide(peer, contract, Arc::new(Arc::clone(slot)) as Arc<dyn Peer>);
            if let Err(error) = provided {
                // Grant-checked at staging; a residual refusal (an occupied
                // slot) is contained and recorded, never silent (R6).
                ledger.append(LedgerEventKind::ErrorRecorded { error }, fiber);
            }
        }
    }
    for contract in &old_provisions {
        if !new_provisions.contains(contract) {
            broker.withdraw(peer, contract);
        }
    }
    // LAST: the seat is installed and routed, so the instance goes live —
    // a registration it makes from here on routes itself, and whatever it
    // recorded while staged is routed by its supervisor on this flip
    // (M2-K26 amendment 2, harness #53; R9: nothing recorded is ever
    // silently left unarmed).
    live.commit_seat();
    displaced
}
