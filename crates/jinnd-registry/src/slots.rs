//! The slot map: one provider value per (context, key, realm) address.
//!
//! Pure decision logic behind the [`crate::sync`] shim, so the loom models drive
//! exactly what the store runs. The map decides three things under one short lock
//! — never held across an `await` or a call into plugin code (R1):
//!
//! * **Generations are monotonic and never reused** for any slot: replacement is
//!   always observable as a new generation, never a silent swap (R9).
//! * **Replacement closes the superseded generation's leases** in the same
//!   critical section, so no dependent can lease a generation that is no longer
//!   current.
//! * **Removal is generation-guarded**: a provider's undo withdraws exactly the
//!   generation it installed, and a stale undo racing a newer provision removes
//!   nothing (I1).

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use jinnd_api::{ContextId, FiberId, Generation};
use jinnd_context::{RealmId, ServiceKey};

use crate::leases::LeaseCell;
use crate::sync::Mutex;

/// Where one slot lives: the providing context, the typed key, and the realm the
/// provider's context resolves that key's name in (R3).
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Address {
    pub context: ContextId,
    pub key: ServiceKey,
    pub realm: RealmId,
}

/// One published provider value and its dependent tracking.
#[derive(Clone)]
pub(crate) struct SlotEntry {
    pub provider: FiberId,
    pub generation: Generation,
    pub value: Arc<dyn Any + Send + Sync>,
    pub leases: Arc<LeaseCell>,
}

impl std::fmt::Debug for SlotEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SlotEntry")
            .field("provider", &self.provider)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

/// Every live slot, and the generation counter their identities come from.
#[derive(Debug, Default)]
pub(crate) struct SlotMap {
    slots: Mutex<HashMap<Address, SlotEntry>>,
    generations: AtomicU64,
}

impl SlotMap {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Publishes `value` at `address`, returning the new entry.
    ///
    /// A previous entry at the address is superseded: its lease cell is closed in
    /// the same critical section, and the new generation is strictly greater than
    /// every generation this map has ever minted.
    pub(crate) fn insert(
        &self,
        address: Address,
        provider: FiberId,
        value: Arc<dyn Any + Send + Sync>,
    ) -> SlotEntry {
        let entry = SlotEntry {
            provider,
            generation: Generation(self.generations.fetch_add(1, Ordering::Relaxed) + 1),
            value,
            leases: Arc::new(LeaseCell::new()),
        };
        self.with(|slots| {
            if let Some(superseded) = slots.insert(address, entry.clone()) {
                superseded.leases.close();
            }
        });
        entry
    }

    /// The current entry at `address`, if any.
    pub(crate) fn get(&self, address: &Address) -> Option<SlotEntry> {
        self.with(|slots| slots.get(address).cloned())
    }

    /// Takes one lease on the current entry at `address`, but only while it still
    /// carries `generation`: a consumer never leases a generation that is not the
    /// one its epoch captured.
    pub(crate) fn lease(
        &self,
        address: &Address,
        generation: Generation,
    ) -> Option<Arc<LeaseCell>> {
        self.with(|slots| {
            let entry = slots.get(address)?;
            if entry.generation != generation || !entry.leases.acquire() {
                return None;
            }
            Some(Arc::clone(&entry.leases))
        })
    }

    /// Withdraws the entry at `address` if it still carries `generation`, closing
    /// its leases in the same critical section. Returns whether it removed.
    pub(crate) fn remove_if(&self, address: &Address, generation: Generation) -> bool {
        self.with(|slots| {
            if slots
                .get(address)
                .is_none_or(|entry| entry.generation != generation)
            {
                return false;
            }
            if let Some(removed) = slots.remove(address) {
                removed.leases.close();
            }
            true
        })
    }

    fn with<T>(&self, change: impl FnOnce(&mut HashMap<Address, SlotEntry>) -> T) -> T {
        let mut slots = self
            .slots
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        change(&mut slots)
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use std::sync::Arc;

    use jinnd_api::{ContextId, FiberId};
    use jinnd_context::ContextTree;

    use super::{Address, SlotMap};

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

    #[test]
    fn insertion_publishes_the_value_under_a_fresh_generation() {
        let map = SlotMap::new();
        let address = address(&ContextTree::new());
        let entry = map.insert(address, FiberId(7), Arc::new(41_u8));
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
    fn replacement_mints_a_strictly_newer_generation_and_closes_the_old_leases() {
        let map = SlotMap::new();
        let address = address(&ContextTree::new());
        let first = map.insert(address, FiberId(1), Arc::new(1_u8));
        assert!(first.leases.acquire());
        let second = map.insert(address, FiberId(2), Arc::new(2_u8));
        assert!(second.generation > first.generation);
        assert!(
            !first.leases.acquire(),
            "a superseded generation must accept no new dependents"
        );
        assert!(second.leases.acquire());
    }

    #[test]
    fn leasing_honors_the_generation_the_epoch_captured() {
        let map = SlotMap::new();
        let address = address(&ContextTree::new());
        let first = map.insert(address, FiberId(1), Arc::new(1_u8));
        assert!(map.lease(&address, first.generation).is_some());
        let second = map.insert(address, FiberId(2), Arc::new(2_u8));
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
        let first = map.insert(address, FiberId(1), Arc::new(1_u8));
        let second = map.insert(address, FiberId(2), Arc::new(2_u8));
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
        let entry = map.insert(address, FiberId(1), Arc::new(1_u8));
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
        let earlier = map.insert(first, FiberId(1), Arc::new(1_u8));
        let later = map.insert(second, FiberId(2), Arc::new(2_u8));
        assert!(later.generation > earlier.generation);
    }
}
