//! What a listener that cannot keep up costs, and how loudly it is told
//! (M2-K13 round 3): the lane's own bound, its refusals on the ledger, and
//! the gap they leave in the ordinals the listener receives.

use std::sync::{Arc, Mutex};

use jinnd_api::LedgerEventKind;

use super::super::{LANE_CAPACITY, LocalTopics, TRANSITIONS_TOPIC};
use super::{Parking, drained};
use crate::peer::LedgerSink;
use crate::topics::tests::RecordingSink;

/// BACK-PRESSURE AT THE LANE, which is where a slow listener now meets
/// its bound (M2-K13 round 3). Deterministic by construction, not by
/// timing: the listener's first delivery PARKS, so every later publish
/// meets a queue that provably cannot drain, and the preconditions say
/// so before anything is claimed about loss.
///
/// Three things must be true at once, and each is a way the absence
/// class could return here. The kernel must REFUSE rather than grow;
/// the refusals must be COUNTED on the ledger (`PublishDropped`); and
/// the loss must be VISIBLE to the listener itself as a jump in the
/// ordinals it receives — not a silent tail.
#[tokio::test]
async fn a_lane_past_its_bound_refuses_ledgers_the_loss_and_keeps_delivering() {
    const LOST: u64 = 7;
    let sink = Arc::new(RecordingSink::default());
    let topics = LocalTopics::traced(Arc::clone(&sink) as Arc<dyn LedgerSink>);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let started = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    topics.listen(
        TRANSITIONS_TOPIC,
        0,
        1,
        None,
        Arc::new(Parking {
            seen: Arc::clone(&seen),
            started: Arc::clone(&started),
            gate: Arc::clone(&gate),
            parked: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }),
    );
    let publish = |ordinal: u64| topics.publish(TRANSITIONS_TOPIC, ordinal.to_string().as_bytes());
    // One transition, taken out of the queue by the lane, which parks
    // inside it. From here the queue can only fill.
    assert_eq!(publish(1), 1);
    started.notified().await;
    let bound = LANE_CAPACITY as u64;
    for ordinal in 2..=(bound + 1) {
        assert_eq!(publish(ordinal), 1, "everything inside the bound is taken");
    }
    // PRECONDITION, asserted: the queue is FULL, so the overflow this
    // test is named for really happens. Past the bound the kernel says
    // it could not deliver rather than growing a queue on the
    // listener's behalf.
    for ordinal in (bound + 2)..=(bound + 1 + LOST) {
        assert_eq!(
            publish(ordinal),
            0,
            "past the bound a listener is REFUSED, never buffered"
        );
    }
    let dropped: u64 = sink
        .recorded()
        .iter()
        .filter_map(|(kind, _)| match kind {
            LedgerEventKind::PublishDropped { topic, dropped } => {
                assert_eq!(topic, TRANSITIONS_TOPIC);
                Some(*dropped)
            }
            _ => None,
        })
        .sum();
    assert_eq!(
        dropped, LOST,
        "every refusal is on the ledger, typed and counted (Law 2, R9)"
    );
    // Let it run. Everything the bound admitted lands, in order.
    gate.add_permits(1);
    let landed = drained(&seen, (bound + 1) as usize).await;
    assert_eq!(
        landed,
        (1..=(bound + 1)).collect::<Vec<u64>>(),
        "nothing inside the bound was reordered or lost"
    );
    // AND THE LANE IS STILL LIVE: the next transition after the loss
    // arrives, and the listener's own ordinals name exactly what it
    // missed rather than hiding it.
    let next = bound + 2 + LOST;
    assert_eq!(
        publish(next),
        1,
        "a lane that dropped is not a lane that died"
    );
    let landed = drained(&seen, (bound + 2) as usize).await;
    let gap = landed[landed.len() - 1] - landed[landed.len() - 2] - 1;
    assert_eq!(
        gap, LOST,
        "the listener's own ordinals name exactly what it missed: {landed:?}"
    );
}
