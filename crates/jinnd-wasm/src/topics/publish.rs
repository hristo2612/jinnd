//! The kernel's own side of the byte lane (M2-K13; harness #40/#41): the
//! reserved-topic vocabulary, and the publish that delivers on one. Split
//! from `topics.rs` by responsibility (R10 file hygiene) — a reserved topic
//! shares the registry's listener table and nothing else of its dispatch:
//! no selector, no mode, no reply, no wait edge, no refusal.

use std::sync::Arc;

use jinnd_api::{DispatchMode, LedgerEventKind};

use crate::peer::LedgerSink;

use super::{EventTarget, Listener, LocalTopics};

/// The kernel's RESERVED lifecycle topic (M2-K13; harness #40/#41): the
/// kernel publishes every fiber transition it commits here, and nothing
/// else ever writes to it. A guest `emit` on it is refused — a witnessed
/// transition must never be confusable with a forged one — and a
/// subscription is gated by the [`crate::INTROSPECT_CONTRACT`] grant,
/// because the delivered payload is bounded by exactly what that
/// contract's own pull already admits.
pub const TRANSITIONS_TOPIC: &str = "jinn:introspect/transitions";

/// How many published payloads may wait on ONE listener before the kernel
/// drops and counts for that listener alone. The bound is per lane, not
/// per topic, because the whole point of the lane is that a listener's
/// slowness is charged to the listener: a shared bound would let one
/// dawdler spend every other listener's headroom.
const LANE_CAPACITY: usize = 256;

/// Whether `topic` is a kernel-reserved publish topic: only the kernel may
/// [`LocalTopics::publish`] on one, and a guest `emit` is refused (M2-K13).
#[must_use]
pub fn reserved(topic: &str) -> bool {
    topic == TRANSITIONS_TOPIC
}

/// The contract grant a subscription to `topic` requires (M2-K13). Every
/// topic is its own grant name (constitution 01 §Grants) except the
/// kernel's reserved ones, which belong to the contract whose authority
/// bounds their payload.
#[must_use]
pub fn grant_for(topic: &str) -> &str {
    if reserved(topic) {
        crate::grants::INTROSPECT_CONTRACT
    } else {
        topic
    }
}

/// ONE LISTENER'S PRIVATE DELIVERY LANE (M2-K13 round 3): a bounded queue
/// and a task of its own, opened on that listener's first publish and
/// dropped with its registration.
///
/// This is the shape that makes sibling isolation structural rather than
/// careful (R11). Round 1 walked listeners sequentially and a slow one held
/// a quick sibling for its whole handler; round 2 made the deliveries of
/// ONE publish concurrent, and the same stall reappeared BETWEEN publishes
/// because the publish still joined before the next could start. A lane
/// removes the join entirely: there is no point on the publish path where
/// one listener's progress is a term in another's.
pub(super) struct Lane {
    inbox: tokio::sync::mpsc::Sender<Vec<u8>>,
}

impl Lane {
    /// Opens a listener's lane on the current runtime. Without one there is
    /// no lane and every offer to it is REFUSED and counted — the kernel
    /// says it could not deliver rather than buffering into a silence.
    fn open(target: &Arc<dyn EventTarget>, token: u64, topic: &str, sink: Sink) -> Option<Self> {
        let runtime = tokio::runtime::Handle::try_current().ok()?;
        let (inbox, mut queue) = tokio::sync::mpsc::channel::<Vec<u8>>(LANE_CAPACITY);
        let target = Arc::clone(target);
        let topic = topic.to_owned();
        runtime.spawn(async move {
            while let Some(payload) = queue.recv().await {
                // Each delivery runs on a task of its OWN, so a listener
                // that TRAPS loses that one transition and neither its own
                // lane nor a sibling's (R9, R11). Serial WITHIN the lane:
                // isolation across listeners is never reordering inside
                // one, so a listener sees transitions in commit order.
                let settled = tokio::spawn(target.deliver(token, &topic, payload)).await;
                if let Some(sink) = &sink {
                    sink.append(
                        LedgerEventKind::DispatchTrace {
                            topic: topic.clone(),
                            mode: DispatchMode::Emit,
                            listeners: 1,
                            failures: u32::from(!matches!(settled, Ok(Ok(_)))),
                            emitter: 0,
                        },
                        None,
                    );
                }
            }
        });
        Some(Self { inbox })
    }
}

type Sink = Option<Arc<dyn LedgerSink>>;

impl LocalTopics {
    /// Publishes one KERNEL-ORIGIN payload to every listener of a reserved
    /// topic (M2-K13). This is not an `emit` and deliberately shares none
    /// of its decisions: there is no emitter context, no selector, no
    /// reply, and no wait edge — the kernel is not a fiber, so a publish
    /// can neither be waited on nor close a cycle (M2-K10), and no
    /// reply-expecting refusal applies (M2-K9).
    ///
    /// **IT DOES NOT JOIN, AND CANNOT (R11).** It is not `async`: there is
    /// no point in this function where a listener's progress could be
    /// awaited, so no listener can delay the kernel, a sibling, or a
    /// sibling's NEXT transition. It hands each listener's [`Lane`] the
    /// payload and returns; delivery, ordering within a listener, trap
    /// containment and the per-delivery `DispatchTrace` all belong to the
    /// lane. Concurrency is ACROSS listeners; each listener is still served
    /// strictly in commit order.
    ///
    /// **A listener that cannot take it LOSES IT LOUDLY.** A lane past
    /// [`LANE_CAPACITY`] — or a listener with no runtime to open one on —
    /// refuses the payload, and the refusals land as one typed
    /// `PublishDropped` row for the publish (Law 2, R9). The kernel never
    /// grows a queue on a listener's behalf and never waits for room.
    ///
    /// Answers how many listeners ACCEPTED the payload.
    pub fn publish(&self, topic: &str, payload: &[u8]) -> usize {
        let sink = self.sink.clone();
        let (reached, refused) = {
            let mut inner = self.lock();
            let mut reached = 0usize;
            let mut refused = 0u64;
            for listener in inner.listeners.iter_mut() {
                if listener.topic != topic {
                    continue;
                }
                // No plugin code runs under this lock and nothing is
                // awaited under it (R1): opening a lane spawns, and
                // offering to one is a non-blocking bounded push.
                let Listener {
                    lane,
                    target,
                    token,
                    ..
                } = listener;
                let opened = match lane {
                    Some(lane) => Some(&*lane),
                    None => match Lane::open(target, *token, topic, sink.clone()) {
                        Some(fresh) => Some(&*lane.insert(fresh)),
                        None => None,
                    },
                };
                match opened {
                    Some(lane) if lane.inbox.try_send(payload.to_vec()).is_ok() => reached += 1,
                    _ => refused += 1,
                }
            }
            (reached, refused)
        };
        if refused > 0
            && let Some(sink) = &self.sink
        {
            sink.append(
                LedgerEventKind::PublishDropped {
                    topic: topic.to_owned(),
                    dropped: refused,
                },
                None,
            );
        }
        reached
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::time::Instant;

    use jinnd_api::KernelFuture;

    use super::{LANE_CAPACITY, LocalTopics, TRANSITIONS_TOPIC};
    use crate::peer::LedgerSink;
    use crate::topics::EventTarget;
    use crate::topics::tests::RecordingSink;
    use jinnd_api::LedgerEventKind;

    /// A listener that dawdles for `delay` on every delivery and writes
    /// down how far into the test each of its deliveries finished.
    struct Timed {
        delay: Duration,
        start: Instant,
        finished: Arc<Mutex<Vec<Duration>>>,
    }

    impl EventTarget for Timed {
        fn deliver(
            &self,
            _token: u64,
            _topic: &str,
            _payload: Vec<u8>,
        ) -> KernelFuture<'static, Vec<u8>> {
            let delay = self.delay;
            let start = self.start;
            let finished = Arc::clone(&self.finished);
            Box::pin(async move {
                tokio::time::sleep(delay).await;
                lock(&finished).push(start.elapsed());
                Ok(Vec::new())
            })
        }
    }

    /// A listener that TRAPS on its first `traps` deliveries and answers
    /// after that — so a lane that survived a trap can be seen carrying
    /// the next transition rather than merely not crashing.
    struct Trapping {
        landed: Arc<Mutex<Vec<Duration>>>,
        start: Instant,
        seen: Arc<std::sync::atomic::AtomicUsize>,
        traps: usize,
    }

    impl EventTarget for Trapping {
        fn deliver(
            &self,
            _token: u64,
            _topic: &str,
            _payload: Vec<u8>,
        ) -> KernelFuture<'static, Vec<u8>> {
            let landed = Arc::clone(&self.landed);
            let start = self.start;
            let attempt = self.seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let trap = attempt < self.traps;
            Box::pin(async move {
                assert!(!trap, "a listener trapped inside its delivery");
                lock(&landed).push(start.elapsed());
                Ok(Vec::new())
            })
        }
    }

    fn lock<T>(cell: &Arc<Mutex<T>>) -> std::sync::MutexGuard<'_, T> {
        cell.lock().unwrap_or_else(|poison| poison.into_inner())
    }

    /// Waits for `want` deliveries by YIELDING, never by sleeping: on a
    /// paused clock virtual time advances only when every task is idle, so
    /// a lane that is genuinely independent settles here with the clock
    /// still reading zero, and one that is blocked behind a sibling never
    /// settles at all.
    async fn settled(cell: &Arc<Mutex<Vec<Duration>>>, want: usize) -> Vec<Duration> {
        for _ in 0..10_000 {
            let landed = lock(cell).clone();
            if landed.len() >= want {
                return landed;
            }
            tokio::task::yield_now().await;
        }
        panic!(
            "only {} of {want} deliveries ever landed — the lane never ran",
            lock(cell).len()
        )
    }

    /// A listener whose FIRST delivery parks until released, and which
    /// writes down the ordinal of every payload it is handed.
    struct Parking {
        seen: Arc<Mutex<Vec<u64>>>,
        started: Arc<tokio::sync::Notify>,
        gate: Arc<tokio::sync::Semaphore>,
        parked: Arc<std::sync::atomic::AtomicBool>,
    }

    impl EventTarget for Parking {
        fn deliver(
            &self,
            _token: u64,
            _topic: &str,
            payload: Vec<u8>,
        ) -> KernelFuture<'static, Vec<u8>> {
            let ordinal: u64 = String::from_utf8_lossy(&payload)
                .parse()
                .unwrap_or_else(|error| panic!("a payload carries its ordinal: {error}"));
            lock(&self.seen).push(ordinal);
            let first = !self.parked.swap(true, std::sync::atomic::Ordering::SeqCst);
            let started = Arc::clone(&self.started);
            let gate = Arc::clone(&self.gate);
            Box::pin(async move {
                if first {
                    started.notify_one();
                    drop(gate.acquire().await);
                }
                Ok(Vec::new())
            })
        }
    }

    /// Waits, in real time, for `want` ordinals — or says how few landed.
    async fn drained(seen: &Arc<Mutex<Vec<u64>>>, want: usize) -> Vec<u64> {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let landed = lock(seen).clone();
            if landed.len() >= want || std::time::Instant::now() >= deadline {
                return landed;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

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
        let publish =
            |ordinal: u64| topics.publish(TRANSITIONS_TOPIC, ordinal.to_string().as_bytes());
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
}
