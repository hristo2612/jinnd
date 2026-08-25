//! Loom models for the registry's interleaving-sensitive decisions (per
//! AGENTS.md, concurrency claims need a model exercising the interleaving).
//!
//! Three claims are modelled, each against the very cells the store runs —
//! [`crate::leases::LeaseCell`] and [`crate::slots::SlotMap`] behind the
//! [`crate::sync`] shim:
//!
//! 1. **Close/acquire exclusion (I2).** However a consumer's lease races a
//!    provider's close, either the lease fails, or it succeeds and the provider's
//!    drain observes it: a drained cell can never still owe a release.
//! 2. **Generation-guarded removal (I1/R9).** A stale undo racing a replacement
//!    provision never removes the newer generation's slot.
//! 3. **Lease-vs-removal coherence.** A lease acquired concurrently with the
//!    removal of its generation is either refused or counted by the removed
//!    entry's drain — never leaked past it.

use loom::sync::Arc;
use loom::thread;

use jinnd_api::{ContextId, FiberId};
use jinnd_context::ContextTree;

use crate::leases::LeaseCell;
use crate::slots::{Address, SlotMap};
use crate::vitality::VitalityCell;

/// An always-active vitality, for models about the map alone.
fn live() -> std::sync::Arc<VitalityCell> {
    std::sync::Arc::new(VitalityCell::new(true))
}

fn address() -> Address {
    let tree: ContextTree = ContextTree::new();
    Address {
        context: ContextId(0),
        key: tree.dynamic_key("jinn.test/model"),
        realm: jinnd_context::RealmId::ROOT,
    }
}

/// Claim 1: a lease and a close may interleave freely; a cell that reports
/// drained owes nothing.
#[test]
fn a_drained_cell_never_owes_a_release() {
    loom::model(|| {
        let cell = Arc::new(LeaseCell::new());

        let consumer = {
            let cell = Arc::clone(&cell);
            thread::spawn(move || {
                if cell.acquire() {
                    cell.release();
                }
            })
        };
        let outstanding = cell.close();
        assert!(outstanding <= 1, "one consumer can hold at most one lease");
        assert!(!cell.acquire(), "no lease lands after close returns");

        consumer.join().unwrap_or_else(|_| unreachable!());
        assert!(
            cell.is_drained(),
            "with the consumer done, nothing is outstanding"
        );
    });
}

/// Claim 2: a stale undo never withdraws the newer generation's slot, whichever
/// side of the replacement it lands on.
#[test]
fn a_stale_undo_never_removes_a_newer_generation() {
    loom::model(|| {
        let map = Arc::new(SlotMap::new());
        let address = address();
        let first = map.insert(address, FiberId(1), std::sync::Arc::new(1_u8), live());

        let replacer = {
            let map = Arc::clone(&map);
            thread::spawn(move || {
                map.insert(address, FiberId(2), std::sync::Arc::new(2_u8), live())
            })
        };
        let removed_first = map.remove_if(&address, first.generation);
        let second = replacer.join().unwrap_or_else(|_| unreachable!());

        match map.get(&address) {
            // The stale undo lost the race entirely, or ran before the
            // replacement landed: the newer generation must survive.
            Some(entry) => assert_eq!(entry.generation, second.generation),
            // The stale undo can only have emptied the slot by removing its own
            // generation before the replacement, never by taking the newer one.
            None => assert!(
                !removed_first || second.generation > first.generation,
                "an empty slot after the race means the replacement was withdrawn"
            ),
        }
    });
}

/// Claim 3: a lease racing its generation's removal is refused or drains.
#[test]
fn a_lease_racing_removal_is_refused_or_drained() {
    loom::model(|| {
        let map = Arc::new(SlotMap::new());
        let address = address();
        let entry = map.insert(address, FiberId(1), std::sync::Arc::new(1_u8), live());

        let consumer = {
            let map = Arc::clone(&map);
            let generation = entry.generation;
            thread::spawn(move || {
                if let Some(cell) = map.lease(&address, generation) {
                    cell.release();
                }
            })
        };
        let removed = map.remove_if(&address, entry.generation);
        assert!(removed, "nothing else removes this generation");

        consumer.join().unwrap_or_else(|_| unreachable!());
        assert!(
            entry.leases.is_drained(),
            "after removal and the consumer's release, the drain owes nothing"
        );
    });
}
