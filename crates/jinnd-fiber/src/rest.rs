//! The rest-observation cell (M1-P6c round 2).
//!
//! "At rest" is the supervisor's own verdict: the last transition landed and
//! the committed state equals the latest desired one — nothing in flight,
//! nothing owed. The profile loader begins a fiber-awaiting amendment only
//! against a resting fiber (the round-2 law): refusal is decided entirely
//! from this kernel-owned bit, never from task-locals or caller identity.
//!
//! The guarantee is causal, exactly like the withdrawal cell's: the bit is
//! lowered `SeqCst` *before* the supervisor runs any transition and raised
//! only at a settle point, so work a transition launches — the body, the
//! inverses, and tasks they spawn — happens-after the lower and never
//! observes its own fiber at rest. An observer outside those spans may see
//! either value; a refusal built on the bit is honest and retryable, never a
//! lock.

#[cfg(feature = "loom")]
use loom::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(feature = "loom"))]
use std::sync::atomic::{AtomicBool, Ordering};

/// One fiber's observable "every owed transition has landed" bit.
#[derive(Debug)]
pub(crate) struct RestCell {
    resting: AtomicBool,
}

impl RestCell {
    /// A fresh fiber still owes its first reconciliation pass: not yet at rest.
    pub(crate) fn new() -> Self {
        Self {
            resting: AtomicBool::new(false),
        }
    }

    /// Lowered before a transition runs: anything the transition reaches
    /// happens-after this store.
    pub(crate) fn lower(&self) {
        self.resting.store(false, Ordering::SeqCst);
    }

    /// Raised at a settle point: nothing owed, nothing in flight.
    pub(crate) fn raise(&self) {
        self.resting.store(true, Ordering::SeqCst);
    }

    /// True while the fiber owes no transition.
    pub(crate) fn observed(&self) -> bool {
        self.resting.load(Ordering::SeqCst)
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::RestCell;

    #[test]
    fn the_bit_tracks_lower_and_raise_and_starts_lowered() {
        let cell = RestCell::new();
        assert!(!cell.observed(), "a fresh fiber owes its first pass");
        cell.raise();
        assert!(cell.observed());
        cell.lower();
        assert!(!cell.observed());
    }
}

#[cfg(all(test, feature = "loom"))]
mod models {
    use loom::sync::Arc;
    use loom::thread;

    use super::RestCell;

    /// The causal edge the loader's rest gate is built on (M1-P6c round 2):
    /// the supervisor lowers the bit before running a transition, and raises
    /// it only after the transition's work completed (the join models the
    /// body landing). A task the transition spawns therefore never observes
    /// its own fiber at rest, however the threads interleave.
    #[test]
    fn a_task_spawned_by_a_transition_never_observes_rest() {
        loom::model(|| {
            let cell = Arc::new(RestCell::new());
            cell.raise();
            cell.lower();
            let helper = {
                let cell = Arc::clone(&cell);
                // What a body spawns mid-transition: strictly after `lower`.
                thread::spawn(move || cell.observed())
            };
            let observed = helper.join().unwrap_or_else(|_| unreachable!());
            assert!(
                !observed,
                "work launched by a transition saw its own fiber at rest"
            );
            cell.raise();
            assert!(cell.observed(), "the settle point raises the bit again");
        });
    }
}
