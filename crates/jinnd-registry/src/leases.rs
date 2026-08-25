//! Dependent tracking for one provider generation (I2).
//!
//! Every consumer activation that captured a provider generation in its epoch
//! holds one lease on it. A dying provider closes the cell — no lease can be
//! acquired against it again — and then waits until the count drains to zero
//! before its withdrawal completes, so a dependent may still call the dying
//! service while its own teardown runs.
//!
//! The cell is pure decision logic behind the [`crate::sync`] shim, so the loom
//! models in [`crate::models`] drive exactly the code the store runs.

use crate::sync::Mutex;

/// The lease count and closed flag for one provider generation.
#[derive(Debug, Default)]
pub struct LeaseCell {
    inner: Mutex<LeaseState>,
}

#[derive(Debug, Default)]
struct LeaseState {
    active: u64,
    closed: bool,
}

impl LeaseCell {
    /// An open cell with no leases outstanding.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes one lease. Fails once the cell is closed: a dying or superseded
    /// provider generation accepts no new dependents.
    #[must_use]
    pub fn acquire(&self) -> bool {
        self.with(|state| {
            if state.closed {
                return false;
            }
            state.active += 1;
            true
        })
    }

    /// Returns one lease, reporting how many remain outstanding.
    ///
    /// Releasing more than was acquired is a kernel bug; the count saturates at
    /// zero rather than poisoning the drain (R11).
    pub fn release(&self) -> u64 {
        self.with(|state| {
            state.active = state.active.saturating_sub(1);
            state.active
        })
    }

    /// Closes the cell to new leases, reporting how many are outstanding.
    ///
    /// Idempotent: a generation superseded by replacement and later withdrawn by
    /// its own undo is closed twice and drains once.
    pub fn close(&self) -> u64 {
        self.with(|state| {
            state.closed = true;
            state.active
        })
    }

    /// True once the cell is closed and every lease has been returned: the moment
    /// a dying provider's value may drop (I2).
    #[must_use]
    pub fn is_drained(&self) -> bool {
        self.with(|state| state.closed && state.active == 0)
    }

    fn with<T>(&self, change: impl FnOnce(&mut LeaseState) -> T) -> T {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        change(&mut state)
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::LeaseCell;

    #[test]
    fn leases_count_up_and_down() {
        let cell = LeaseCell::new();
        assert!(cell.acquire());
        assert!(cell.acquire());
        assert_eq!(cell.release(), 1);
        assert_eq!(cell.release(), 0);
    }

    #[test]
    fn an_open_cell_is_never_drained() {
        let cell = LeaseCell::new();
        assert!(!cell.is_drained());
    }

    #[test]
    fn closing_stops_new_leases() {
        let cell = LeaseCell::new();
        assert!(cell.acquire());
        assert_eq!(cell.close(), 1);
        assert!(!cell.acquire());
    }

    #[test]
    fn a_closed_cell_drains_when_the_last_lease_returns() {
        let cell = LeaseCell::new();
        assert!(cell.acquire());
        assert_eq!(cell.close(), 1);
        assert!(!cell.is_drained());
        assert_eq!(cell.release(), 0);
        assert!(cell.is_drained());
    }

    #[test]
    fn closing_with_no_leases_drains_immediately() {
        let cell = LeaseCell::new();
        assert_eq!(cell.close(), 0);
        assert!(cell.is_drained());
    }

    #[test]
    fn close_is_idempotent() {
        let cell = LeaseCell::new();
        assert!(cell.acquire());
        assert_eq!(cell.close(), 1);
        assert_eq!(cell.close(), 1);
        assert_eq!(cell.release(), 0);
        assert!(cell.is_drained());
    }

    #[test]
    fn release_saturates_at_zero() {
        let cell = LeaseCell::new();
        assert_eq!(cell.release(), 0);
        assert_eq!(cell.close(), 0);
        assert!(cell.is_drained());
    }
}
