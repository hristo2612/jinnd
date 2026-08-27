//! Mode-1 hot-swap (R8): old instance warm until the new one is healthy,
//! auto-rollback on failure, batched over every entry sharing the artifact
//! hash (decision log 2026-08-25), every phase a ledger event.
//!
//! The interleaving-sensitive part — a swap racing an entry disposal — lives
//! in [`SwapCore`], a sync phase machine modeled under loom (`--features
//! loom`). The async driver above it never holds the core's lock across an
//! await (R1).

use jinnd_api::{EntryId, KernelError, KernelFuture, LedgerEventKind, SwapPhaseKind};

use crate::broker::LedgerSink;
use crate::sync::Mutex;
use std::collections::HashMap;

/// One slot's swap phase. `Tombstone` is a disposed entry: a swap must never
/// resurrect it (I1/I4 — disposal wins).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotPhase {
    Steady,
    Preparing,
    Tombstone,
}

/// The phase machine deciding ownership between a swap's commit and a
/// concurrent disposal. Exactly one side ends up owning the prepared
/// instance: a commit that loses to a tombstone reports `false` and the
/// disposer's answer says whether it observed a preparation to discard.
#[derive(Default)]
pub struct SwapCore {
    slots: Mutex<HashMap<u64, SlotPhase>>,
}

impl SwapCore {
    fn with<T>(&self, f: impl FnOnce(&mut HashMap<u64, SlotPhase>) -> T) -> T {
        let mut guard = self
            .slots
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        f(&mut guard)
    }

    /// Steady → Preparing. False for a tombstoned or already-preparing slot.
    pub fn begin(&self, slot: u64) -> bool {
        self.with(
            |slots| match slots.entry(slot).or_insert(SlotPhase::Steady) {
                phase @ SlotPhase::Steady => {
                    *phase = SlotPhase::Preparing;
                    true
                }
                _ => false,
            },
        )
    }

    /// Preparing → Steady with the new instance live. False when disposal
    /// won the race: the caller must discard the prepared instance and must
    /// not resurrect the slot.
    pub fn commit(&self, slot: u64) -> bool {
        self.with(|slots| match slots.get_mut(&slot) {
            Some(phase @ SlotPhase::Preparing) => {
                *phase = SlotPhase::Steady;
                true
            }
            _ => false,
        })
    }

    /// Preparing → Steady with the old instance kept (rollback).
    pub fn rollback(&self, slot: u64) {
        self.with(|slots| {
            if let Some(phase) = slots.get_mut(&slot)
                && *phase == SlotPhase::Preparing
            {
                *phase = SlotPhase::Steady;
            }
        });
    }

    /// Any phase → Tombstone. Returns the phase the disposer observed: seeing
    /// `Preparing` transfers ownership of the prepared instance to the
    /// disposer, which discards it.
    pub fn dispose(&self, slot: u64) -> SlotPhase {
        self.with(|slots| {
            let previous = slots.insert(slot, SlotPhase::Tombstone);
            previous.unwrap_or(SlotPhase::Steady)
        })
    }
}

/// The lane seam the swap driver drives (transport-agnostic: the adapter's
/// wasm lane implements it over real instances; unit tests over mocks).
///
/// Contract: `prepare` builds and health-gates the NEW instance — activate,
/// offer the old snapshot, health check — while the old instance stays warm
/// and untouched; `commit` atomically replaces old with new; `discard` drops
/// a prepared instance without ever touching the old one.
pub trait SwapSlots: Send + Sync {
    type Prepared: Send;

    fn entries_pinned_to(&self, hash: &str) -> Vec<EntryId>;

    fn prepare(&self, entry: &EntryId) -> KernelFuture<'_, Self::Prepared>;

    fn commit(&self, entry: &EntryId, prepared: Self::Prepared) -> KernelFuture<'_, ()>;

    fn discard(&self, prepared: Self::Prepared) -> KernelFuture<'_, ()>;
}

/// One batch outcome (mirrored to the facade's `SwapReport` by the harness).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SwapOutcome {
    pub swapped: Vec<EntryId>,
    pub rolled_back: bool,
}

/// Drives one Mode-1 swap over every entry pinned to `old_hash` — the batch
/// is by artifact hash, never per entry. Every phase is a ledger event.
///
/// # Errors
///
/// None of its own: a failed preparation is the ROLLBACK outcome, recorded,
/// with every already-prepared instance discarded and every old instance
/// still live.
pub async fn swap_batch<S: SwapSlots>(
    slots: &S,
    old_hash: &str,
    new_hash: &str,
    ledger: &dyn LedgerSink,
) -> Result<SwapOutcome, KernelError> {
    let entries = slots.entries_pinned_to(old_hash);
    ledger.append(
        LedgerEventKind::SwapPhase {
            artifact: new_hash.to_owned(),
            phase: SwapPhaseKind::Began,
        },
        None,
    );
    let mut prepared: Vec<(EntryId, S::Prepared)> = Vec::new();
    for entry in &entries {
        match slots.prepare(entry).await {
            Ok(instance) => {
                ledger.append(
                    LedgerEventKind::SwapPhase {
                        artifact: new_hash.to_owned(),
                        phase: SwapPhaseKind::InstanceHealthy,
                    },
                    None,
                );
                prepared.push((entry.clone(), instance));
            }
            Err(_) => {
                for (_, instance) in prepared {
                    let _ = slots.discard(instance).await;
                }
                ledger.append(
                    LedgerEventKind::SwapPhase {
                        artifact: new_hash.to_owned(),
                        phase: SwapPhaseKind::RolledBack,
                    },
                    None,
                );
                return Ok(SwapOutcome {
                    swapped: Vec::new(),
                    rolled_back: true,
                });
            }
        }
    }
    let mut swapped = Vec::new();
    for (entry, instance) in prepared {
        slots.commit(&entry, instance).await?;
        swapped.push(entry);
    }
    ledger.append(
        LedgerEventKind::SwapPhase {
            artifact: new_hash.to_owned(),
            phase: SwapPhaseKind::Committed,
        },
        None,
    );
    Ok(SwapOutcome {
        swapped,
        rolled_back: false,
    })
}

#[cfg(all(test, feature = "loom"))]
mod loom_model {
    use super::{SlotPhase, SwapCore};
    use loom::sync::Arc;
    use loom::thread;

    /// Under every interleaving of commit vs dispose, exactly one side owns
    /// the prepared instance and a tombstoned slot is never resurrected.
    #[test]
    fn commit_and_dispose_agree_on_ownership() {
        loom::model(|| {
            let core = Arc::new(SwapCore::default());
            assert!(core.begin(1));

            let committer = {
                let core = Arc::clone(&core);
                thread::spawn(move || core.commit(1))
            };
            let disposer = {
                let core = Arc::clone(&core);
                thread::spawn(move || core.dispose(1))
            };
            let committed = committer.join().unwrap();
            let observed = disposer.join().unwrap();

            // Ownership is exclusive: the commit landed iff the disposer did
            // NOT observe (and thereby claim) the preparation.
            assert_eq!(committed, observed != SlotPhase::Preparing);
            // Disposal always wins the end state.
            assert!(!core.begin(1), "a tombstoned slot never re-enters a swap");
        });
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use jinnd_api::{
        EntryId, ErrorCode, KernelError, KernelFuture, LedgerEventKind, SwapPhaseKind,
    };

    use super::{SlotPhase, SwapCore, SwapOutcome, SwapSlots, swap_batch};
    use crate::broker_tests::CapturedLedger;

    struct MockSlots {
        entries: Vec<EntryId>,
        fail_at: Option<usize>,
        prepared: AtomicUsize,
        discarded: AtomicUsize,
        committed: Mutex<Vec<EntryId>>,
    }

    impl MockSlots {
        fn new(count: usize, fail_at: Option<usize>) -> Self {
            Self {
                entries: (0..count).map(|i| EntryId(format!("entry-{i}"))).collect(),
                fail_at,
                prepared: AtomicUsize::new(0),
                discarded: AtomicUsize::new(0),
                committed: Mutex::new(Vec::new()),
            }
        }
    }

    impl SwapSlots for MockSlots {
        type Prepared = usize;

        fn entries_pinned_to(&self, _: &str) -> Vec<EntryId> {
            self.entries.clone()
        }

        fn prepare(&self, _: &EntryId) -> KernelFuture<'_, usize> {
            let index = self.prepared.fetch_add(1, Ordering::SeqCst);
            let failing = self.fail_at == Some(index);
            Box::pin(async move {
                if failing {
                    Err(KernelError {
                        code: ErrorCode::PluginFailed,
                        message: "unhealthy".into(),
                        fiber: None,
                    })
                } else {
                    Ok(index)
                }
            })
        }

        fn commit(&self, entry: &EntryId, _: usize) -> KernelFuture<'_, ()> {
            self.committed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(entry.clone());
            Box::pin(async { Ok(()) })
        }

        fn discard(&self, _: usize) -> KernelFuture<'_, ()> {
            self.discarded.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        }
    }

    fn phases(ledger: &CapturedLedger) -> Vec<SwapPhaseKind> {
        ledger
            .events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter_map(|(kind, _)| match kind {
                LedgerEventKind::SwapPhase { phase, .. } => Some(*phase),
                _ => None,
            })
            .collect()
    }

    #[tokio::test]
    async fn healthy_batch_commits_every_entry_sharing_the_artifact() {
        let ledger = CapturedLedger::default();
        let slots = MockSlots::new(2, None);
        let outcome = swap_batch(&slots, "old", "new", &ledger).await.unwrap();
        assert_eq!(outcome.swapped.len(), 2, "the batch is by artifact hash");
        assert!(!outcome.rolled_back);
        assert_eq!(
            phases(&ledger),
            vec![
                SwapPhaseKind::Began,
                SwapPhaseKind::InstanceHealthy,
                SwapPhaseKind::InstanceHealthy,
                SwapPhaseKind::Committed
            ]
        );
    }

    #[tokio::test]
    async fn failing_preparation_rolls_the_whole_batch_back() {
        let ledger = CapturedLedger::default();
        let slots = MockSlots::new(3, Some(1));
        let outcome = swap_batch(&slots, "old", "new", &ledger).await.unwrap();
        assert_eq!(
            outcome,
            SwapOutcome {
                swapped: Vec::new(),
                rolled_back: true
            }
        );
        assert!(
            slots
                .committed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty(),
            "old instances stay live; nothing commits"
        );
        assert_eq!(
            slots.discarded.load(Ordering::SeqCst),
            1,
            "the already-prepared instance is discarded"
        );
        assert_eq!(
            phases(&ledger),
            vec![
                SwapPhaseKind::Began,
                SwapPhaseKind::InstanceHealthy,
                SwapPhaseKind::RolledBack
            ]
        );
    }

    #[test]
    fn tombstoned_slot_refuses_commit_and_swap_reentry() {
        let core = SwapCore::default();
        assert!(core.begin(9));
        assert_eq!(core.dispose(9), SlotPhase::Preparing);
        assert!(!core.commit(9), "disposal won; the commit must discard");
        assert!(!core.begin(9), "no resurrection");
    }
}
