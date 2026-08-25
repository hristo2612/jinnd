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
use crate::vitality::VitalityCell;

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
    pub vitality: Arc<VitalityCell>,
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
    /// An address occupied by ANOTHER provider refuses the provision — a live
    /// binding is never silently replaced (paper Def 23, R9) — answering the
    /// occupant. The SAME provider supersedes its own binding (the hot-swap
    /// lane): the superseded lease cell is closed in the same critical
    /// section, and the new generation is strictly greater than every
    /// generation this map has ever minted.
    pub(crate) fn insert(
        &self,
        address: Address,
        provider: FiberId,
        value: Arc<dyn Any + Send + Sync>,
        vitality: Arc<VitalityCell>,
    ) -> Result<SlotEntry, FiberId> {
        let entry = SlotEntry {
            provider,
            generation: Generation(self.generations.fetch_add(1, Ordering::Relaxed) + 1),
            value,
            leases: Arc::new(LeaseCell::new()),
            vitality,
        };
        self.with(|slots| {
            if let Some(occupant) = slots.get(&address) {
                if occupant.provider != provider {
                    return Err(occupant.provider);
                }
            }
            if let Some(superseded) = slots.insert(address, entry.clone()) {
                superseded.leases.close();
            }
            Ok(entry)
        })
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
#[path = "slots_tests.rs"]
mod tests;
