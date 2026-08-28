//! Unit tests of the swap driver and phase machine (crate lane; the
//! invariant suite stays verifier-owned). The atomic-commit pin is the
//! round-2 blocker-3 probe: a disposal racing the batch rolls the WHOLE
//! batch back with zero committed entries — never a partial commit.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use jinnd_api::{EntryId, ErrorCode, KernelError, KernelFuture, LedgerEventKind, SwapPhaseKind};

use super::{SwapCore, SwapOutcome, SwapSlots, swap_batch};
use crate::broker_tests::CapturedLedger;

struct MockSlots {
    entries: Vec<(EntryId, u64)>,
    fail_at: Option<usize>,
    /// A slot the mock tombstones (a concurrent disposal) right after this
    /// many preparations — the blocker-3 probe shape.
    dispose_after: Option<(usize, u64)>,
    core: Arc<SwapCore>,
    prepared: AtomicUsize,
    discarded: AtomicUsize,
    retired: AtomicUsize,
    committed: Mutex<Vec<EntryId>>,
}

impl MockSlots {
    fn new(core: &Arc<SwapCore>, count: usize, fail_at: Option<usize>) -> Self {
        Self {
            entries: (0..count)
                .map(|i| (EntryId(format!("entry-{i}")), i as u64 + 1))
                .collect(),
            fail_at,
            dispose_after: None,
            core: Arc::clone(core),
            prepared: AtomicUsize::new(0),
            discarded: AtomicUsize::new(0),
            retired: AtomicUsize::new(0),
            committed: Mutex::new(Vec::new()),
        }
    }
}

impl SwapSlots for MockSlots {
    type Prepared = usize;
    type Displaced = usize;

    fn entries_pinned_to(&self, _: &str) -> Vec<(EntryId, u64)> {
        self.entries.clone()
    }

    fn prepare(&self, _: &EntryId) -> KernelFuture<'_, usize> {
        let index = self.prepared.fetch_add(1, Ordering::SeqCst);
        let failing = self.fail_at == Some(index);
        if let Some((after, slot)) = self.dispose_after
            && index + 1 == after
        {
            // The entry behind `slot` is disposed while the batch is still
            // preparing others: the claim must observe the tombstone.
            self.core.dispose(slot);
        }
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

    fn commit(&self, entry: &EntryId, prepared: usize) -> Option<usize> {
        self.committed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(entry.clone());
        Some(prepared)
    }

    fn retire_displaced(&self, _: &EntryId, _: usize) -> KernelFuture<'_, ()> {
        self.retired.fetch_add(1, Ordering::SeqCst);
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

fn committed(slots: &MockSlots) -> Vec<EntryId> {
    slots
        .committed
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
}

#[tokio::test]
async fn healthy_batch_commits_every_entry_sharing_the_artifact() {
    let ledger = CapturedLedger::default();
    let core = Arc::new(SwapCore::default());
    let slots = MockSlots::new(&core, 2, None);
    let outcome = swap_batch(&slots, &core, "old", "new", &ledger)
        .await
        .unwrap_or_else(|error| panic!("swap: {error:?}"));
    assert_eq!(outcome.swapped.len(), 2, "the batch is by artifact hash");
    assert!(!outcome.rolled_back);
    assert_eq!(committed(&slots).len(), 2, "every entry's seat committed");
    assert_eq!(
        slots.retired.load(Ordering::SeqCst),
        2,
        "each displaced seat is retired after the critical section"
    );
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
    let core = Arc::new(SwapCore::default());
    let slots = MockSlots::new(&core, 3, Some(1));
    let outcome = swap_batch(&slots, &core, "old", "new", &ledger)
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
        committed(&slots).is_empty(),
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
    // The rollback released the claims: the entries can swap again.
    assert!(core.begin(1), "rollback returns the slot to steady");
}

/// The round-2 blocker-3 pin (verifier probe: "entry left the roster" with a
/// partial commit): an entry disposed between its preparation and the batch
/// claim forces the WHOLE batch to roll back — zero entries commit, every
/// prepared instance is discarded, and the old instances stay warm.
#[tokio::test]
async fn a_disposal_racing_the_batch_rolls_everything_back_with_zero_commits() {
    let ledger = CapturedLedger::default();
    let core = Arc::new(SwapCore::default());
    let mut slots = MockSlots::new(&core, 2, None);
    // entry-0 (slot 1) is disposed after both preparations succeed.
    slots.dispose_after = Some((2, 1));
    let outcome = swap_batch(&slots, &core, "old", "new", &ledger)
        .await
        .unwrap_or_else(|error| panic!("swap: {error:?}"));
    assert_eq!(
        outcome,
        SwapOutcome {
            swapped: Vec::new(),
            rolled_back: true
        },
        "the claim is atomic: disposal of one entry rolls back the batch"
    );
    assert_eq!(committed(&slots), Vec::<EntryId>::new(), "zero commits");
    assert_eq!(
        slots.discarded.load(Ordering::SeqCst),
        2,
        "every prepared instance is discarded"
    );
    assert_eq!(
        phases(&ledger),
        vec![
            SwapPhaseKind::Began,
            SwapPhaseKind::InstanceHealthy,
            SwapPhaseKind::InstanceHealthy,
            SwapPhaseKind::RolledBack
        ]
    );
    assert!(!core.begin(1), "the disposed slot never re-enters a swap");
    assert!(core.begin(2), "the survivor returned to steady");
}

#[test]
fn tombstoned_slot_refuses_claim_and_never_runs_the_commit_bookkeeping() {
    let core = SwapCore::default();
    assert!(core.begin(9));
    assert_eq!(core.dispose(9), super::SlotPhase::Preparing);
    let mut ran = false;
    assert!(
        core.commit_all_with(&[9], || ran = true).is_none(),
        "disposal won; the claim must discard"
    );
    assert!(!ran, "a refused claim runs NO commit bookkeeping");
    assert!(!core.begin(9), "no resurrection");
    assert!(core.is_tombstone(9));
}
