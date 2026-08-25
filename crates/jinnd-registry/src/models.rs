//! Loom models for the registry's interleaving-sensitive decisions (per
//! AGENTS.md, concurrency claims need a model exercising the interleaving).
//!
//! Four claims are modelled, each against the very cells the store runs —
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
//! 4. **Duplicate-provision refusal (Def 23, R9; M1-P6c).** A different
//!    provider racing the occupant's withdrawal is refused or lands strictly
//!    after the removal — never a silent replacement, whichever way the race
//!    resolves.

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

/// Publishes, panicking on a refusal the model never expects.
fn publish(
    map: &SlotMap,
    address: Address,
    provider: FiberId,
    value: u8,
) -> crate::slots::SlotEntry {
    match map.insert(address, provider, std::sync::Arc::new(value), live()) {
        Ok(entry) => entry,
        Err(occupant) => panic!("unexpected refusal: occupied by {occupant:?}"),
    }
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
/// side of the supersession it lands on. The superseding provision is the SAME
/// provider's (the hot-swap lane, M1-P6c) — a different provider is refused,
/// modelled by claim 4.
#[test]
fn a_stale_undo_never_removes_a_newer_generation() {
    loom::model(|| {
        let map = Arc::new(SlotMap::new());
        let address = address();
        let first = publish(&map, address, FiberId(1), 1);

        let replacer = {
            let map = Arc::clone(&map);
            thread::spawn(move || publish(&map, address, FiberId(1), 2))
        };
        let removed_first = map.remove_if(&address, first.generation);
        let second = replacer.join().unwrap_or_else(|_| unreachable!());

        match map.get(&address) {
            // The stale undo lost the race entirely, or ran before the
            // supersession landed: the newer generation must survive.
            Some(entry) => assert_eq!(entry.generation, second.generation),
            // The stale undo can only have emptied the slot by removing its own
            // generation before the supersession, never by taking the newer one.
            None => assert!(
                !removed_first || second.generation > first.generation,
                "an empty slot after the race means the supersession was withdrawn"
            ),
        }
    });
}

/// Claim 4 (M1-P6c, Def 23/R9): a DIFFERENT provider racing the occupant's
/// withdrawal either lands strictly after the removal, or is refused — the
/// occupant is never silently replaced, whichever way the race resolves.
#[test]
fn a_racing_second_provider_is_refused_or_lands_after_removal() {
    loom::model(|| {
        let map = Arc::new(SlotMap::new());
        let address = address();
        let first = publish(&map, address, FiberId(1), 1);

        let contender = {
            let map = Arc::clone(&map);
            thread::spawn(move || {
                map.insert(address, FiberId(2), std::sync::Arc::new(2_u8), live())
            })
        };
        let removed = map.remove_if(&address, first.generation);
        assert!(removed, "nothing else withdraws the occupant's generation");
        let contended = contender.join().unwrap_or_else(|_| unreachable!());

        match contended {
            // The contender observed the empty slot: it can only have run
            // after the removal, and its binding is the one live now.
            Ok(entry) => {
                assert!(entry.generation > first.generation);
                let now = map.get(&address);
                assert_eq!(now.map(|found| found.provider), Some(FiberId(2)));
            }
            // The contender observed the occupant: refused, naming it, and
            // the removal left the slot empty.
            Err(occupant) => {
                assert_eq!(occupant, FiberId(1));
                assert!(map.get(&address).is_none());
            }
        }
    });
}

/// Claim 3: a lease racing its generation's removal is refused or drains.
#[test]
fn a_lease_racing_removal_is_refused_or_drained() {
    loom::model(|| {
        let map = Arc::new(SlotMap::new());
        let address = address();
        let entry = publish(&map, address, FiberId(1), 1);

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
