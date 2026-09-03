//! The publisher's own unit lane (R10 file hygiene: the `src/` per-file
//! cap is hard, so the suite lives beside the module rather than inside
//! it) — the bound, the ordinals, the delivered record's shape, and the
//! REAL overflow end to end against a live ledger.

use std::time::Duration;

use super::*;
use jinnd_api::{FiberId, KernelFuture, LedgerQuery, TransitionCause};

fn transition() -> Transition {
    Transition {
        fiber: FiberId(1),
        from: FiberState::Active,
        to: FiberState::Unloading,
        cause: TransitionCause::ConfigChanged,
    }
}

fn publisher() -> Arc<Lifecycle> {
    let ledger = Ledger::open_in_memory().unwrap_or_else(|error| panic!("{error}"));
    let sink: Arc<dyn jinnd_wasm::LedgerSink> = Arc::new(crate::support::Sink {
        ledger: ledger.clone(),
        fibers: Arc::new(Mutex::new(std::collections::HashMap::new())),
    });
    let lane = Arc::new(LaneCore::new(sink).unwrap_or_else(|error| panic!("{error:?}")));
    Lifecycle::new(ledger, lane)
}

/// Past the bound the kernel DROPS rather than grows or blocks, and the
/// loss is counted twice over: in the drop tally it will ledger, and as
/// the gap it leaves in the ordinals a listener receives.
#[test]
fn a_full_queue_drops_and_counts_instead_of_growing() {
    let publisher = publisher();
    let entry = EntryId("watcher".to_owned());
    for _ in 0..(CAPACITY + 10) {
        publisher.offer(&entry, &transition());
    }
    let (batch, dropped) = publisher.drain();
    assert_eq!(
        batch.len(),
        CAPACITY,
        "the queue never grows past its bound"
    );
    assert_eq!(dropped, 10, "every loss past the bound is counted");
    let ordinals: Vec<u64> = batch.iter().map(|item| item.ordinal).collect();
    assert_eq!(ordinals.first(), Some(&1));
    assert_eq!(
        ordinals.last(),
        Some(&(CAPACITY as u64)),
        "ordinals are the OFFER count, so the drops show as the gap \
         between the last delivered ordinal and the next one"
    );
}

/// A listener that writes down every ordinal it is handed, and whose
/// FIRST delivery parks until the test releases it.
struct Recorder {
    seen: Arc<Mutex<Vec<u64>>>,
    started: Arc<tokio::sync::Notify>,
    gate: Arc<tokio::sync::Semaphore>,
    parked: Arc<AtomicBool>,
}

impl jinnd_wasm::EventTarget for Recorder {
    fn deliver(
        &self,
        _token: u64,
        _topic: &str,
        payload: Vec<u8>,
        _budget: Option<std::num::NonZeroU64>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let wire: serde_json::Value = serde_json::from_slice(&payload)
            .unwrap_or_else(|error| panic!("a delivery is JSON: {error}"));
        let ordinal = wire["ordinal"]
            .as_u64()
            .unwrap_or_else(|| panic!("a delivery carries its ordinal: {wire}"));
        lock(&self.seen).push(ordinal);
        let first = !self.parked.swap(true, Ordering::SeqCst);
        let started = Arc::clone(&self.started);
        let gate = Arc::clone(&self.gate);
        Box::pin(async move {
            if first {
                started.notify_one();
                let permit = gate.acquire().await;
                drop(permit);
            }
            Ok(Vec::new())
        })
    }
}

/// Waits until `seen` holds `want` ordinals, or gives up saying so.
async fn until(seen: &Arc<Mutex<Vec<u64>>>, want: usize) -> Vec<u64> {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let landed = lock(seen).clone();
        if landed.len() >= want || std::time::Instant::now() >= deadline {
            return landed;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// THE OFFER LANE'S OVERFLOW, end to end and deterministic: commits
/// that outrun the publisher past the bound lose transitions, and the
/// loss is visible TWICE over — as a `PublishDropped` row on the real
/// ledger, and as the gap it leaves in the ordinals the listener
/// receives.
///
/// **Which bound this is, stated exactly (M2-K13 round 3).** There are
/// two, in series, and they fill for different reasons. THIS one is
/// [`CAPACITY`], between the kernel's commit path and the publisher
/// task: it fills when transitions are COMMITTED faster than the
/// publisher can drain them, which is what the burst below does — the
/// offers are synchronous, so the publisher cannot be scheduled between
/// them. A SLOW LISTENER no longer fills this queue at all; since round
/// 3 the publish hands off without joining, so a dawdling listener
/// fills its OWN lane instead, and that half is proven at
/// `jinnd_wasm::topics::publish::tests::
/// a_lane_past_its_bound_refuses_ledgers_the_loss_and_keeps_delivering`.
///
/// Round 2 named a slow listener as the cause here and was right at the
/// time; the round-3 fix moved the cause, and a probe confirmed it —
/// with the gate released so nothing parks, this still passed. Saying
/// so is the point: an assertion that survives for a different reason
/// than its name gives is the vacuity class, whichever direction it
/// drifts in.
///
/// The parking listener remains, and earns its place: it holds the
/// FIRST delivery, so the drain, the ordinals and the gap are observed
/// in one release rather than raced for.
#[tokio::test]
async fn a_real_overflow_is_ledgered_and_shows_as_a_gap_in_the_ordinals() {
    const LOST: usize = 7;
    const LATER: usize = 3;
    let ledger = Ledger::open_in_memory().unwrap_or_else(|error| panic!("{error}"));
    let sink: Arc<dyn jinnd_wasm::LedgerSink> = Arc::new(crate::support::Sink {
        ledger: ledger.clone(),
        fibers: Arc::new(Mutex::new(std::collections::HashMap::new())),
    });
    let lane = Arc::new(LaneCore::new(sink).unwrap_or_else(|error| panic!("{error:?}")));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let started = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    lane.topics.listen(
        TRANSITIONS_TOPIC,
        0,
        1,
        None,
        Arc::new(Recorder {
            seen: Arc::clone(&seen),
            started: Arc::clone(&started),
            gate: Arc::clone(&gate),
            parked: Arc::new(AtomicBool::new(false)),
        }),
    );
    let publisher = Lifecycle::new(ledger.clone(), lane);
    let entry = EntryId("watcher".to_owned());

    // One transition, delivered; the listener parks inside it so the
    // drain below is observed in one release rather than raced for.
    publisher.offer(&entry, &transition());
    started.notified().await;

    // Now COMMIT past the bound in one synchronous burst: the publisher
    // task cannot be scheduled between these offers, so CAPACITY queue
    // up and the last LOST have nowhere to go.
    for _ in 0..(CAPACITY + LOST) {
        publisher.offer(&entry, &transition());
    }
    // PRECONDITION, asserted before anything is claimed about loss:
    // the queue is FULL and the kernel really did drop. A test that
    // cannot establish this does not get to pass. (The burst above is
    // what fills it — see the note on the two bounds, above.)
    {
        let pending = lock(&publisher.pending);
        assert_eq!(
            pending.queue.len(),
            CAPACITY,
            "the queue is full — the overflow this test is named for really happened"
        );
        assert_eq!(
            pending.dropped, LOST as u64,
            "and exactly the offers past the bound were dropped"
        );
    }

    // Let it run. The queued CAPACITY drain, and the drops are reported.
    gate.add_permits(CAPACITY + LOST + LATER + 1);
    let drained = until(&seen, CAPACITY + 1).await;
    assert_eq!(
        drained.len(),
        CAPACITY + 1,
        "everything the bound admitted was delivered"
    );
    // Now offer again, so the loss shows where a listener can see it:
    // as a JUMP in its own ordinals, not as a silent tail.
    for _ in 0..LATER {
        publisher.offer(&entry, &transition());
    }
    let ordinals = until(&seen, CAPACITY + 1 + LATER).await;
    assert_eq!(ordinals.len(), CAPACITY + 1 + LATER, "{ordinals:?}");

    // THE GAP. Contiguous up to the bound, then a jump of exactly the
    // number the kernel dropped.
    let contiguous: Vec<u64> = (1..=(CAPACITY as u64 + 1)).collect();
    assert_eq!(
        ordinals[..=CAPACITY],
        contiguous[..],
        "nothing inside the bound was reordered or lost"
    );
    let gap = ordinals[CAPACITY + 1] - ordinals[CAPACITY] - 1;
    assert_eq!(
        gap, LOST as u64,
        "the listener's own ordinals name exactly what it missed: {ordinals:?}"
    );

    // And the SAME loss on the ledger, typed and counted (Law 2).
    let rows = ledger
        .events(LedgerQuery::default())
        .await
        .unwrap_or_else(|error| panic!("ledger read: {error}"));
    let ledgered: u64 = rows
        .iter()
        .filter_map(|record| match &record.kind {
            LedgerEventKind::PublishDropped { topic, dropped } => {
                assert_eq!(topic, TRANSITIONS_TOPIC);
                Some(*dropped)
            }
            _ => None,
        })
        .sum();
    assert_eq!(
        ledgered, LOST as u64,
        "every dropped transition is on the ledger too, not just in the gap"
    );
}

/// Ordinals never repeat and never go backwards: FIFO in, FIFO out.
#[test]
fn ordinals_are_monotonic_in_offer_order() {
    let publisher = publisher();
    let entry = EntryId("watcher".to_owned());
    for _ in 0..8 {
        publisher.offer(&entry, &transition());
    }
    let (batch, _) = publisher.drain();
    let ordinals: Vec<u64> = batch.iter().map(|item| item.ordinal).collect();
    assert_eq!(ordinals, (1..=8).collect::<Vec<u64>>());
}

/// The delivered record carries the introspect-admitted fields and NOT
/// the cause: the authority demonstration failed for that one field.
#[test]
fn the_delivered_record_omits_the_cause() {
    let item = Queued {
        entry: EntryId("watcher".to_owned()),
        incarnation: Some(3),
        transition: transition(),
        ordinal: 7,
    };
    let wire: serde_json::Value =
        serde_json::from_slice(&payload(&item, 42)).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(wire["from"], "active");
    assert_eq!(wire["to"], "unloading");
    assert_eq!(wire["incarnation"], 3);
    assert_eq!(wire["committed-by"], 42);
    assert!(wire.get("cause").is_none(), "no cause is delivered: {wire}");
}
