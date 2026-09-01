//! R11 SIBLING ISOLATION (M2-K13 round 3): no listener's progress is a
//! term in another's — not within one publish, not across successive
//! publishes, and not through a trap.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;

use super::super::{LocalTopics, TRANSITIONS_TOPIC};
use super::{Timed, Trapping, settled};

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
