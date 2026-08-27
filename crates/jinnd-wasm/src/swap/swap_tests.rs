//! Unit tests of the swap driver and phase machine (crate lane; the
//! invariant suite stays verifier-owned).

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use jinnd_api::{EntryId, ErrorCode, KernelError, KernelFuture, LedgerEventKind, SwapPhaseKind};

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
    let outcome = swap_batch(&slots, "old", "new", &ledger)
        .await
        .unwrap_or_else(|error| panic!("swap: {error:?}"));
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
    let outcome = swap_batch(&slots, "old", "new", &ledger)
        .await
        .unwrap_or_else(|error| panic!("swap: {error:?}"));
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
