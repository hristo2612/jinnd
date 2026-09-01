//! The kernel's own side of the byte lane (M2-K13; harness #40/#41): the
//! reserved-topic vocabulary, and the publish that delivers on one. Split
//! from `topics.rs` by responsibility (R10 file hygiene) — a reserved topic
//! shares the registry's listener table and nothing else of its dispatch:
//! no selector, no mode, no reply, no wait edge, no refusal.

use std::sync::Arc;

use jinnd_api::{DispatchMode, LedgerEventKind};

use super::{EventTarget, LocalTopics};

/// The kernel's RESERVED lifecycle topic (M2-K13; harness #40/#41): the
/// kernel publishes every fiber transition it commits here, and nothing
/// else ever writes to it. A guest `emit` on it is refused — a witnessed
/// transition must never be confusable with a forged one — and a
/// subscription is gated by the [`crate::INTROSPECT_CONTRACT`] grant,
/// because the delivered payload is bounded by exactly what that
/// contract's own pull already admits.
pub const TRANSITIONS_TOPIC: &str = "jinn:introspect/transitions";

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

impl LocalTopics {
    /// Publishes one KERNEL-ORIGIN payload to every listener of a reserved
    /// topic (M2-K13). This is not an `emit` and deliberately shares none
    /// of its decisions: there is no emitter context, no selector, no
    /// reply, and no wait edge — the kernel is not a fiber, so a publish
    /// can neither be waited on nor close a cycle (M2-K10), and no
    /// reply-expecting refusal applies (M2-K9).
    ///
    /// **Siblings are ISOLATED (R11).** Each listener's delivery runs on
    /// its own task, so a listener that dawdles — or traps — inside its
    /// handler delays and fails only itself. The sequential walk this
    /// replaced held a quick listener behind a slow sibling for the
    /// sibling's whole handler (measured at 301 ms in M2-K13 round 1),
    /// which is the nested-dispatch stall (#4/#32) rebuilt one layer up on
    /// the bus a UI extension is about to subscribe to.
    ///
    /// **Order still belongs to the kernel.** A publish returns only once
    /// every delivery it started has settled, so successive publishes never
    /// overlap and each listener sees transitions in exactly the order the
    /// kernel committed them. Concurrency is ACROSS listeners, never across
    /// a listener's own deliveries.
    ///
    /// With a sink and at least one listener, exactly one `DispatchTrace`
    /// lands after the walk, attributed to no fiber and to emitter `0` —
    /// the kernel itself (Law 2). With NO listener nothing is delivered and
    /// nothing is logged: no model-visible thing happened.
    ///
    /// Answers how many listeners the publish reached.
    pub async fn publish(&self, topic: &str, payload: &[u8]) -> usize {
        let selected: Vec<(u64, Arc<dyn EventTarget>)> = {
            let inner = self.lock();
            inner
                .listeners
                .iter()
                .filter(|listener| listener.topic == topic)
                .map(|listener| (listener.token, Arc::clone(&listener.target)))
                .collect()
        };
        if selected.is_empty() {
            return 0;
        }
        let listeners = selected.len();
        let mut walk = tokio::task::JoinSet::new();
        for (token, target) in selected {
            let topic = topic.to_owned();
            let payload = payload.to_vec();
            walk.spawn(async move { target.deliver(token, &topic, payload).await.is_err() });
        }
        let mut failures = 0u32;
        while let Some(joined) = walk.join_next().await {
            // A listener that FAILED and one that TRAPPED are the same
            // contained failure here (R9, R11): both count, and neither
            // reaches a sibling or the kernel.
            if joined.unwrap_or(true) {
                failures += 1;
            }
        }
        if let Some(sink) = &self.sink {
            sink.append(
                LedgerEventKind::DispatchTrace {
                    topic: topic.to_owned(),
                    mode: DispatchMode::Emit,
                    listeners: u32::try_from(listeners).unwrap_or(u32::MAX),
                    failures,
                    emitter: 0,
                },
                None,
            );
        }
        listeners
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::time::Instant;

    use jinnd_api::KernelFuture;

    use super::{LocalTopics, TRANSITIONS_TOPIC};
    use crate::topics::EventTarget;

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

    /// A listener that traps rather than answering.
    struct Trapping {
        landed: Arc<Mutex<Vec<Duration>>>,
        start: Instant,
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
            let trap = lock(&landed).len() < self.traps;
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
        assert_eq!(topics.publish(TRANSITIONS_TOPIC, b"one").await, 2);
        assert_eq!(topics.publish(TRANSITIONS_TOPIC, b"two").await, 2);
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
        assert_eq!(topics.publish(TRANSITIONS_TOPIC, b"{}").await, 2);
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
        assert_eq!(topics.publish(TRANSITIONS_TOPIC, b"one").await, 2);
        assert_eq!(topics.publish(TRANSITIONS_TOPIC, b"two").await, 2);
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
