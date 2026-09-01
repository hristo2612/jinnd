//! R11 SIBLING ISOLATION (M2-K13 round 3): no listener's progress is a
//! term in another's — not within one publish, not across successive
//! publishes, and not through a trap.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;

use super::super::{LocalTopics, TRANSITIONS_TOPIC};
use super::{Timed, Trap, Trapping, settled};
use crate::peer::LedgerSink;
use crate::topics::tests::RecordingSink;

/// R11 SIBLING ISOLATION **ACROSS SUCCESSIVE PUBLISHES** — the defect
/// the M2-K13 round-2 verifier measured at 305 ms after round 2 made
/// deliveries concurrent *within* one publish. Two transitions, back to
/// back, with the slow listener registered FIRST: the quick sibling's
/// SECOND delivery must land without one instant of time passing.
///
/// The clock is PAUSED and this waits by yielding, never by sleeping,
/// so virtual time cannot advance while the assertion is pending: a
/// publish that joins its listeners — or a second publish that waits on
/// the first — leaves the quick lane un-run and this fails by
/// exhaustion rather than by a timing guess.
#[tokio::test(start_paused = true)]
async fn a_slow_listener_does_not_delay_a_quick_siblings_next_transition() {
    const SLOW: Duration = Duration::from_millis(300);
    let topics = LocalTopics::default();
    let start = Instant::now();
    let slow = Arc::new(Mutex::new(Vec::new()));
    let quick = Arc::new(Mutex::new(Vec::new()));
    topics.listen(
        TRANSITIONS_TOPIC,
        0,
        1,
        None,
        Arc::new(Timed {
            delay: SLOW,
            start,
            finished: Arc::clone(&slow),
        }),
    );
    topics.listen(
        TRANSITIONS_TOPIC,
        0,
        2,
        None,
        Arc::new(Timed {
            delay: Duration::ZERO,
            start,
            finished: Arc::clone(&quick),
        }),
    );
    assert_eq!(topics.publish(TRANSITIONS_TOPIC, b"one"), 2);
    assert_eq!(topics.publish(TRANSITIONS_TOPIC, b"two"), 2);
    let landed = settled(&quick, 2).await;
    assert_eq!(
        landed,
        vec![Duration::ZERO, Duration::ZERO],
        "the quick listener's NEXT transition waited behind a {SLOW:?} \
         sibling — the publish path still joins (R11)"
    );
    // PRECONDITION, asserted after the claim so the wait above stays
    // time-free: the slow sibling really was slow, and it is serial to
    // ITSELF — isolation across listeners is never reordering within
    // one.
    tokio::time::sleep(SLOW * 4).await;
    let dawdled = settled(&slow, 2).await;
    assert!(
        dawdled[0] >= SLOW && dawdled[1] >= SLOW * 2,
        "the slow sibling has to be slow, and serial to itself, for this \
         test to mean anything: {dawdled:?}"
    );
}

/// The same isolation WITHIN one publish (M2-K13 round 1 measured this
/// at 301 ms): the slow listener is registered first, and the quick one
/// must still finish at virtual zero.
#[tokio::test(start_paused = true)]
async fn a_slow_listener_does_not_delay_a_quick_sibling() {
    const SLOW: Duration = Duration::from_millis(300);
    let topics = LocalTopics::default();
    let start = Instant::now();
    let slow = Arc::new(Mutex::new(Vec::new()));
    let quick = Arc::new(Mutex::new(Vec::new()));
    topics.listen(
        TRANSITIONS_TOPIC,
        0,
        1,
        None,
        Arc::new(Timed {
            delay: SLOW,
            start,
            finished: Arc::clone(&slow),
        }),
    );
    topics.listen(
        TRANSITIONS_TOPIC,
        0,
        2,
        None,
        Arc::new(Timed {
            delay: Duration::ZERO,
            start,
            finished: Arc::clone(&quick),
        }),
    );
    assert_eq!(topics.publish(TRANSITIONS_TOPIC, b"{}"), 2);
    assert_eq!(
        settled(&quick, 1).await,
        vec![Duration::ZERO],
        "a quick listener waited behind its slow sibling (R11)"
    );
    // PRECONDITION, asserted: the slow sibling really was slow.
    tokio::time::sleep(SLOW * 2).await;
    let dawdled = settled(&slow, 1).await;
    assert!(
        dawdled[0] >= SLOW,
        "the slow sibling has to be slow for this test to mean anything: {dawdled:?}"
    );
}

/// A listener that TRAPS is contained twice over (R9, R11): the trap
/// reaches neither a sibling — registered second, so a walk that let
/// the panic escape would take it too — NOR the trapping listener's own
/// lane, which must still carry the transitions that follow.
#[tokio::test(start_paused = true)]
async fn a_trap_reaches_neither_a_sibling_nor_the_lanes_next_transition() {
    let topics = LocalTopics::default();
    let start = Instant::now();
    let trapped = Arc::new(Mutex::new(Vec::new()));
    let quick = Arc::new(Mutex::new(Vec::new()));
    topics.listen(
        TRANSITIONS_TOPIC,
        0,
        1,
        None,
        Arc::new(Trapping {
            landed: Arc::clone(&trapped),
            start,
            seen: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            traps: 1,
            site: Trap::Inside,
        }),
    );
    topics.listen(
        TRANSITIONS_TOPIC,
        0,
        2,
        None,
        Arc::new(Timed {
            delay: Duration::ZERO,
            start,
            finished: Arc::clone(&quick),
        }),
    );
    // The first transition TRAPS the first listener; the second must
    // still be delivered to it.
    assert_eq!(topics.publish(TRANSITIONS_TOPIC, b"one"), 2);
    assert_eq!(topics.publish(TRANSITIONS_TOPIC, b"two"), 2);
    assert_eq!(
        settled(&quick, 2).await.len(),
        2,
        "the sibling of a trapping listener still receives everything"
    );
    assert_eq!(
        settled(&trapped, 1).await.len(),
        1,
        "the trapping listener's OWN lane survived its trap and took the \
         next transition (R11)"
    );
}

/// A trap raised BEFORE the future is returned is contained too (R11) —
/// the M2-K13 round-3 verifier's probe, reproduced as the case. `deliver`
/// is the listener's own code and it runs on whatever stack calls it, so
/// containment that wraps only the RETURNED FUTURE never sees a panic
/// raised on the way to producing one: the probe accepted two publishes
/// and recorded `deliver_calls=1`, because the first synchronous trap
/// killed the lane loop and the second transition never ran.
///
/// What makes that a SELF-CONTRADICTION rather than a rough edge — and
/// so not eligible to land as a known limit — is the silence. This
/// packet's entire back-pressure claim is that a transition a listener
/// does not get is bounded, COUNTED and ledgered. A dead lane loses
/// every later transition with no `PublishDropped` row and no failure
/// count, which is the absence class this program keeps meeting.
///
/// So both halves are asserted: the lane still CALLS the listener again
/// and the next transition lands, and the trap that did happen is on the
/// ledger as a counted failure rather than as nothing at all.
#[tokio::test(start_paused = true)]
async fn a_trap_before_the_future_is_returned_does_not_kill_the_lane() {
    let sink = Arc::new(RecordingSink::default());
    let topics = LocalTopics::traced(Arc::clone(&sink) as Arc<dyn LedgerSink>);
    let start = Instant::now();
    let landed = Arc::new(Mutex::new(Vec::new()));
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    topics.listen(
        TRANSITIONS_TOPIC,
        0,
        1,
        None,
        Arc::new(Trapping {
            landed: Arc::clone(&landed),
            start,
            seen: Arc::clone(&calls),
            traps: 1,
            site: Trap::Before,
        }),
    );
    // Exactly the probe: two publishes, both accepted, the first
    // trapping synchronously.
    assert_eq!(topics.publish(TRANSITIONS_TOPIC, b"one"), 1);
    assert_eq!(topics.publish(TRANSITIONS_TOPIC, b"two"), 1);
    assert_eq!(
        settled(&landed, 1).await.len(),
        1,
        "the lane died on a synchronous trap and its next transition \
         never ran (R11)"
    );
    // PRECONDITION, asserted: the listener really was CALLED twice, so
    // the delivery above is the second transition and not a retry of the
    // first — `deliver_calls=1` was the probe's whole finding.
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "deliver_calls"
    );
    // AND THE TRAP IS NOT SILENT: the delivery that failed is counted on
    // the ledger, so no transition goes missing without a row (Law 2).
    let failures: u32 = sink
        .recorded()
        .iter()
        .filter_map(|(kind, _)| match kind {
            jinnd_api::LedgerEventKind::DispatchTrace {
                topic, failures, ..
            } => {
                assert_eq!(topic, TRANSITIONS_TOPIC);
                Some(*failures)
            }
            _ => None,
        })
        .sum();
    assert_eq!(
        failures, 1,
        "a contained trap is a COUNTED failure, never an absence"
    );
}
