//! Unit tests for the slot map's decision logic (split from `slots.rs` by
//! the 300-line file cap; same module, same visibility).

use std::sync::Arc;

use jinnd_api::{ContextId, FiberId};
use jinnd_context::ContextTree;

use super::{Address, SlotMap};
use crate::vitality::VitalityCell;

/// An always-active vitality, for tests about the map alone.
fn live() -> Arc<VitalityCell> {
    Arc::new(VitalityCell::new(true))
}

fn address(tree: &ContextTree) -> Address {
    Address {
        context: ContextId(0),
        key: tree.dynamic_key("jinn.test/slot"),
        realm: jinnd_context::RealmId::ROOT,
    }
}

#[test]
fn a_missing_slot_resolves_to_nothing() {
    let map = SlotMap::new();
    assert!(map.get(&address(&ContextTree::new())).is_none());
}

/// Publishes, panicking on a refusal these map tests never expect.
fn publish(map: &SlotMap, address: Address, provider: FiberId, value: u8) -> super::SlotEntry {
    match map.insert(address, provider, Arc::new(value), live()) {
        Ok(entry) => entry,
        Err(occupant) => panic!("unexpected refusal: occupied by {occupant:?}"),
    }
}

#[test]
fn insertion_publishes_the_value_under_a_fresh_generation() {
    let map = SlotMap::new();
    let address = address(&ContextTree::new());
    let entry = publish(&map, address, FiberId(7), 41);
    let found = map.get(&address).into_iter().next();
    let found = found.as_ref();
    assert_eq!(found.map(|found| found.generation), Some(entry.generation));
    assert_eq!(found.map(|found| found.provider), Some(FiberId(7)));
    assert_eq!(
        found.and_then(|found| found.value.downcast_ref::<u8>().copied()),
        Some(41)
    );
}

#[test]
fn same_provider_supersession_mints_a_newer_generation_and_closes_old_leases() {
    let map = SlotMap::new();
    let address = address(&ContextTree::new());
    let first = publish(&map, address, FiberId(1), 1);
    assert!(first.leases.acquire());
    let second = publish(&map, address, FiberId(1), 2);
    assert!(second.generation > first.generation);
    assert!(
        !first.leases.acquire(),
        "a superseded generation must accept no new dependents"
    );
    assert!(second.leases.acquire());
}

#[test]
fn a_second_provider_is_refused_and_the_occupant_is_untouched() {
    let map = SlotMap::new();
    let address = address(&ContextTree::new());
    let first = publish(&map, address, FiberId(1), 1);
    assert!(first.leases.acquire());
    let refused = map.insert(address, FiberId(2), Arc::new(2_u8), live());
    assert_eq!(
        refused.err(),
        Some(FiberId(1)),
        "an occupied slot refuses a DIFFERENT provider (Def 23, R9)"
    );
    let occupant = map.get(&address).into_iter().next();
    assert_eq!(
        occupant.as_ref().map(|entry| entry.generation),
        Some(first.generation),
        "the occupant's binding must be untouched by the refusal"
    );
    assert!(
        first.leases.acquire(),
        "the occupant's leases stay open: nothing was superseded"
    );
}

#[test]
fn leasing_honors_the_generation_the_epoch_captured() {
    let map = SlotMap::new();
    let address = address(&ContextTree::new());
    let first = publish(&map, address, FiberId(1), 1);
    assert!(map.lease(&address, first.generation).is_some());
    let second = publish(&map, address, FiberId(1), 2);
    assert!(
        map.lease(&address, first.generation).is_none(),
        "a stale epoch must not lease the replacement generation"
    );
    assert!(map.lease(&address, second.generation).is_some());
}

#[test]
fn removal_is_generation_guarded() {
    let map = SlotMap::new();
    let address = address(&ContextTree::new());
    let first = publish(&map, address, FiberId(1), 1);
    let second = publish(&map, address, FiberId(1), 2);
    assert!(
        !map.remove_if(&address, first.generation),
        "a stale undo must not withdraw a newer provider's slot"
    );
    assert!(map.get(&address).is_some());
    assert!(map.remove_if(&address, second.generation));
    assert!(map.get(&address).is_none());
}

#[test]
fn removal_closes_the_leases_it_withdraws() {
    let map = SlotMap::new();
    let address = address(&ContextTree::new());
    let entry = publish(&map, address, FiberId(1), 1);
    assert!(entry.leases.acquire());
    assert!(map.remove_if(&address, entry.generation));
    assert!(!entry.leases.acquire());
    assert!(!entry.leases.is_drained());
    assert_eq!(entry.leases.release(), 0);
    assert!(entry.leases.is_drained());
}

#[test]
fn generations_stay_monotonic_across_distinct_slots() {
    let map = SlotMap::new();
    let tree = ContextTree::new();
    let first = Address {
        key: tree.dynamic_key("jinn.test/first"),
        ..address(&tree)
    };
    let second = Address {
        key: tree.dynamic_key("jinn.test/second"),
        ..address(&tree)
    };
    let earlier = publish(&map, first, FiberId(1), 1);
    let later = publish(&map, second, FiberId(2), 2);
    assert!(later.generation > earlier.generation);
}
