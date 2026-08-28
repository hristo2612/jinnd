//! The seat's closing gate (M2-K4 FINDINGS #15, ruled by M2-K5 #16): two
//! flags in a fixed order. `closing` shuts the DOOR — the supervisor refuses
//! every guest entry it has not yet dequeued — while the entry already in
//! flight keeps registering; `sealed` shuts the JOURNAL — every registration
//! refuses on the record — and is raised only AFTER the in-flight entry has
//! drained under the guest deadline. A planned stop therefore lands every
//! effect of a sub-deadline handler (never a torn prefix), and the seal
//! stays the backstop for a handler that outlives its deadline (I1, R5,
//! R11). Modeled under loom (`seal_model.rs`); split from `slot.rs` by
//! responsibility (R10).

use std::sync::atomic::Ordering;

use crate::sync::AtomicBool;

#[derive(Default)]
pub(crate) struct SealGate {
    closing: AtomicBool,
    sealed: AtomicBool,
}

impl SealGate {
    /// The production closing sequence, in law order: shut the door, drain
    /// what is already inside, then seal the journal. `drain` resolves once
    /// the in-flight guest entry has returned (or its deadline killed it).
    pub(crate) async fn close(&self, drain: impl Future<Output = ()>) {
        self.closing.store(true, Ordering::SeqCst);
        drain.await;
        self.seal();
    }

    /// The journal backstop alone: every registration refuses from here
    /// on. `close` raises it last; on its own it models the handler that
    /// outlived its deadline.
    pub(crate) fn seal(&self) {
        self.sealed.store(true, Ordering::SeqCst);
    }

    /// True once the door shut: an entry dequeued now must refuse.
    pub(crate) fn closing(&self) -> bool {
        self.closing.load(Ordering::SeqCst)
    }

    /// True once the journal closed: a registration now must refuse.
    pub(crate) fn sealed(&self) -> bool {
        self.sealed.load(Ordering::SeqCst)
    }

    /// Reopens both for a fresh incarnation (M2-K4): the previous seat's
    /// closing sequence has already landed on the fiber's own task.
    pub(crate) fn reopen(&self) {
        self.closing.store(false, Ordering::SeqCst);
        self.sealed.store(false, Ordering::SeqCst);
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::SealGate;

    /// The order is the contract: the door shuts before the drain, the
    /// journal seals only after it — a handler that registers N effects
    /// while the gate drains lands all N.
    #[tokio::test]
    async fn the_journal_seals_only_after_the_drain() {
        let gate = Arc::new(SealGate::default());
        let landed = Arc::new(AtomicUsize::new(0));
        let drain = {
            let (gate, landed) = (Arc::clone(&gate), Arc::clone(&landed));
            async move {
                assert!(gate.closing(), "the door shut before the drain");
                for _ in 0..3 {
                    assert!(!gate.sealed(), "the journal is open while draining");
                    landed.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                }
            }
        };
        assert!(!gate.closing() && !gate.sealed());
        gate.close(drain).await;
        assert_eq!(landed.load(Ordering::SeqCst), 3);
        assert!(gate.closing() && gate.sealed());
        gate.reopen();
        assert!(!gate.closing() && !gate.sealed());
    }
}
