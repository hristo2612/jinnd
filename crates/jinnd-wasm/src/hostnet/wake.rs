//! The readiness-wake decision core for `jinn:net` (M2-K7, harness #23):
//! one lock owns each socket's arm state, and a wake's ledger append runs
//! UNDER that lock — so once `take` (close, suspend, dispose) has returned,
//! no wake of that handle is ever appended again, and one re-arm yields
//! at most one wake (level-triggered, coalesced; never a wake per byte —
//! R9). The tokio readiness layer stays outside (`readiness.rs`); this
//! table compiles under loom and its interleavings are pinned in
//! `wake_model.rs` (the card's loom obligation).

use std::collections::HashMap;

use crate::sync::Mutex;

/// Per live socket: whether the guest has acted since the last wake — a
/// wake claims only an ARMED row, and only a guest read/accept re-arms.
#[derive(Default)]
pub(crate) struct WakeTable {
    armed: Mutex<HashMap<u64, bool>>,
}

impl WakeTable {
    fn lock(&self) -> impl std::ops::DerefMut<Target = HashMap<u64, bool>> + '_ {
        self.armed
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// A fresh socket starts armed: its first readiness wakes.
    pub(crate) fn insert(&self, handle: u64) {
        self.lock().insert(handle, true);
    }

    /// `Some(armed)` for a live row, `None` once taken.
    pub(crate) fn armed(&self, handle: u64) -> Option<bool> {
        self.lock().get(&handle).copied()
    }

    /// Claims one wake: `append` runs under the lock iff the row is live
    /// AND armed, which disarms it — the honesty guarantee (no append after
    /// `take` returned) and the coalescing guarantee (one wake per re-arm)
    /// are the same critical section.
    pub(crate) fn claim_wake(&self, handle: u64, append: impl FnOnce()) -> bool {
        let mut rows = self.lock();
        match rows.get_mut(&handle) {
            Some(armed) if *armed => {
                *armed = false;
                append();
                true
            }
            _ => false,
        }
    }

    /// The guest acted on the handle (read or accept): the next readiness
    /// wakes again. `false` for a taken row.
    pub(crate) fn rearm(&self, handle: u64) -> bool {
        match self.lock().get_mut(&handle) {
            Some(armed) => {
                *armed = true;
                true
            }
            None => false,
        }
    }

    /// Whether the row is still live.
    pub(crate) fn alive(&self, handle: u64) -> bool {
        self.lock().contains_key(&handle)
    }

    /// Removes the row — the registration's release. After this returns,
    /// `claim_wake` refuses forever.
    pub(crate) fn take(&self, handle: u64) -> bool {
        self.lock().remove(&handle).is_some()
    }
}
