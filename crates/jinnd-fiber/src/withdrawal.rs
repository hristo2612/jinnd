//! The withdrawal-in-flight cell (M1-P6b).
//!
//! A withdrawal replays plugin-owned inverses, and code those inverses reach —
//! on the fiber's own task or on tasks they spawn — must be refusable by
//! anyone whose wait could otherwise close a cycle through the replay (R1).
//! The task-local teardown marker answers only for the fiber's own task; this
//! cell is the task-agnostic half: an atomic bit set for exactly the span of
//! one withdrawal replay, observable from any task through
//! [`crate::Fiber::withdrawing`].
//!
//! The guarantee callers build on is causal: the bit is stored `SeqCst`
//! *before* any inverse runs, and anything an inverse calls or spawns
//! happens-after that store — so a conflict check made from within the replay,
//! however many task boundaries it crossed, always observes the bit raised.
//! The loom model in `models` checks exactly this.

#[cfg(feature = "loom")]
use loom::sync::atomic::{AtomicBool, Ordering};
#[cfg(not(feature = "loom"))]
use std::sync::atomic::{AtomicBool, Ordering};

/// One fiber's observable "withdrawal replay in flight" bit.
#[derive(Debug)]
pub(crate) struct WithdrawalCell {
    replaying: AtomicBool,
}

impl WithdrawalCell {
    pub(crate) fn new() -> Self {
        Self {
            replaying: AtomicBool::new(false),
        }
    }

    /// Marks the span of one withdrawal replay; the span ends when the guard
    /// drops, panic unwinding included — the bit never sticks.
    pub(crate) fn begin(&self) -> WithdrawalSpan<'_> {
        self.replaying.store(true, Ordering::SeqCst);
        WithdrawalSpan { cell: self }
    }

    /// True while a withdrawal replay is in flight.
    pub(crate) fn active(&self) -> bool {
        self.replaying.load(Ordering::SeqCst)
    }
}

/// The span of one withdrawal replay.
pub(crate) struct WithdrawalSpan<'a> {
    cell: &'a WithdrawalCell,
}

impl Drop for WithdrawalSpan<'_> {
    fn drop(&mut self) {
        self.cell.replaying.store(false, Ordering::SeqCst);
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::WithdrawalCell;

    #[test]
    fn the_bit_spans_exactly_the_guard() {
        let cell = WithdrawalCell::new();
        assert!(!cell.active());
        let span = cell.begin();
        assert!(cell.active());
        drop(span);
        assert!(!cell.active());
    }
}
