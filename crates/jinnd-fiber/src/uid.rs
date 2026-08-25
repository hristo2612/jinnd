//! Fiber uid allocation.

use std::sync::atomic::{AtomicU64, Ordering};

use jinnd_api::FiberId;

static NEXT: AtomicU64 = AtomicU64::new(1);

/// Allocates the next fiber uid.
///
/// Uids are handed out strictly increasing and are **never reused** (R3): a stale
/// reference to a disposed fiber can therefore never be mistaken for a live one, and
/// the ledger's history stays unambiguous. Exhausting `u64` is not a reachable state
/// at any plausible spawn rate, so the counter never has to wrap or recycle.
pub(crate) fn next_fiber_id() -> FiberId {
    FiberId(NEXT.fetch_add(1, Ordering::Relaxed))
}
