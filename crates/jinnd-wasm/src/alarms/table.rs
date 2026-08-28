//! The alarm registry's decision core: one lock owns liveness, and a wake's
//! ledger append runs UNDER that lock — so once `take` (cancel, one-shot
//! completion, or teardown) has returned, no wake is ever appended again.
//! The tokio timer layer stays outside; this table compiles under loom and
//! its interleavings are pinned in `alarm_model.rs` (M2-K2 loom obligation).

use std::collections::HashMap;

use crate::sync::Mutex;

/// Row table for live alarms; `H` is the timer layer's per-alarm handle
/// (the task's abort handle in production, `()` under the loom model). A
/// row exists from `arm` until `take`; its handle arrives via `install`
/// once the timer task is spawned.
pub(crate) struct AlarmTable<H> {
    inner: Mutex<Inner<H>>,
}

struct Inner<H> {
    next: u64,
    rows: HashMap<u64, Option<H>>,
}

impl<H> Default for AlarmTable<H> {
    fn default() -> Self {
        Self {
            inner: Mutex::new(Inner {
                next: 0,
                rows: HashMap::new(),
            }),
        }
    }
}

impl<H> AlarmTable<H> {
    fn lock(&self) -> impl std::ops::DerefMut<Target = Inner<H>> + '_ {
        self.inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Mints a live row and returns its id (never reused).
    pub(crate) fn arm(&self) -> u64 {
        let mut inner = self.lock();
        inner.next += 1;
        let id = inner.next;
        inner.rows.insert(id, None);
        id
    }

    /// Attaches the timer layer's handle to a live row; `false` when the row
    /// was taken between `arm` and the spawn — the caller aborts its task.
    pub(crate) fn install(&self, id: u64, handle: H) -> bool {
        match self.lock().rows.get_mut(&id) {
            Some(slot) => {
                *slot = Some(handle);
                true
            }
            None => false,
        }
    }

    /// Claims one wake: `append` runs under the table's lock iff the row is
    /// live. This is the tap's honesty guarantee — a `take` that has
    /// returned happens-after every append the alarm will ever make.
    pub(crate) fn claim_wake(&self, id: u64, append: impl FnOnce()) -> bool {
        let inner = self.lock();
        if inner.rows.contains_key(&id) {
            append();
            true
        } else {
            false
        }
    }

    /// Whether the row is still live (delivery-failure recording gate).
    pub(crate) fn alive(&self, id: u64) -> bool {
        self.lock().rows.contains_key(&id)
    }

    /// Removes one row — cancellation, one-shot completion, or teardown all
    /// land here. Of any concurrent takers exactly one receives the row;
    /// after any of them returns, `claim_wake` refuses forever.
    pub(crate) fn take(&self, id: u64) -> Option<Option<H>> {
        self.lock().rows.remove(&id)
    }
}
