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
    /// reply-expecting refusal applies (M2-K9). Deliveries are FIFO in the
    /// caller's order, so a publish never reorders what the kernel
    /// committed; a failing or trapping listener is contained and skipped
    /// (R9, R11), never aborting the walk and never affecting a sibling.
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
        let mut failures = 0u32;
        for (token, target) in selected {
            if target
                .deliver(token, topic, payload.to_vec())
                .await
                .is_err()
            {
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
