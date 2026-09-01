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

    /// A listener that dawdles for `delay` and writes down how far into the
    /// publish its own delivery finished.
    struct Timed {
        delay: Duration,
        start: Instant,
        finished: Arc<Mutex<Option<Duration>>>,
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
                *finished.lock().unwrap_or_else(|poison| poison.into_inner()) =
                    Some(start.elapsed());
                Ok(Vec::new())
            })
        }
    }

    /// A listener that traps rather than answering.
    struct Trapping;

    impl EventTarget for Trapping {
        fn deliver(
            &self,
            _token: u64,
            _topic: &str,
            _payload: Vec<u8>,
        ) -> KernelFuture<'static, Vec<u8>> {
            Box::pin(async { panic!("a listener trapped inside its delivery") })
        }
    }

    fn at(cell: &Arc<Mutex<Option<Duration>>>) -> Duration {
        cell.lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .unwrap_or_else(|| panic!("this listener never finished its delivery"))
    }

    /// R11 SIBLING ISOLATION, red-first against the sequential walk this
    /// replaced: with the slow listener registered FIRST — the shape the
    /// M2-K13 round-1 verifier measured at 301 ms — a quick sibling must
    /// still finish immediately, not behind the dawdler.
    ///
    /// On a PAUSED clock, so the claim is about the shape of the walk and
    /// not about how loaded the machine was: virtual time advances only
    /// when every task is idle, so a quick listener that is genuinely
    /// concurrent finishes at 0 no matter what else is running.
    #[tokio::test(start_paused = true)]
    async fn a_slow_listener_does_not_delay_a_quick_sibling() {
        const SLOW: Duration = Duration::from_millis(300);
        let topics = LocalTopics::default();
        let start = Instant::now();
        let slow = Arc::new(Mutex::new(None));
        let quick = Arc::new(Mutex::new(None));
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
        // PRECONDITION, asserted: the slow sibling really was slow. Without
        // it "the quick one was not delayed" would be a claim about
        // nothing — the vacuity class this packet's round 2 exists to end.
        let slow = at(&slow);
        assert!(
            slow >= SLOW,
            "the slow sibling has to be slow for this test to mean anything: {slow:?}"
        );
        let quick = at(&quick);
        assert_eq!(
            quick,
            Duration::ZERO,
            "a quick listener waited {quick:?} behind a {slow:?} sibling — \
             the publish serialised its listeners (R11)"
        );
    }

    /// A listener that TRAPS is contained, counted as a failure, and never
    /// reaches a sibling: the trapping listener is registered first, so a
    /// walk that let the panic escape would take the quick one with it.
    #[tokio::test(start_paused = true)]
    async fn a_trapping_listener_is_contained_and_its_sibling_still_lands() {
        let topics = LocalTopics::default();
        let start = Instant::now();
        let quick = Arc::new(Mutex::new(None));
        topics.listen(TRANSITIONS_TOPIC, 0, 1, None, Arc::new(Trapping));
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
        assert_eq!(
            topics.publish(TRANSITIONS_TOPIC, b"{}").await,
            2,
            "the publish reached both listeners and survived the trap"
        );
        let _ = at(&quick);
    }
}
