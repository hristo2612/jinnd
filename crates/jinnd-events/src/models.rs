//! Loom models for the listener table's interleaving-sensitive decisions (per
//! AGENTS.md, concurrency claims need a model exercising the interleaving).
//!
//! Two claims are modelled, each against the very cell dispatch runs —
//! [`crate::table::ListenerTable`] behind the [`crate::sync`] shim:
//!
//! 1. **At-most-once claim.** Two dispatches racing one once-registration
//!    resolve through `remove`: exactly one observes the claim, however the
//!    lock interleaves — the delivery count can never exceed one.
//! 2. **Snapshot isolation.** A registration racing a dispatch either misses
//!    the snapshot or joins it whole; the snapshot taken is never invalidated
//!    by the concurrent insert, and neither side deadlocks.

use loom::thread;
use std::sync::Arc;

use jinnd_api::ContextId;

use crate::table::ListenerTable;

fn key() -> std::any::TypeId {
    std::any::TypeId::of::<ListenerTable>()
}

/// Claim 1: of two racing claims for one once-registration, exactly one wins.
#[test]
fn racing_once_claims_admit_exactly_one_winner() {
    loom::model(|| {
        let table = Arc::new(ListenerTable::new());
        let id = table.insert(key(), ContextId(0), true, Arc::new(()));

        let rival = {
            let table = Arc::clone(&table);
            thread::spawn(move || table.remove(key(), id))
        };
        let won = table.remove(key(), id);
        let rival_won = rival.join().unwrap_or_else(|_| unreachable!());

        assert!(
            won ^ rival_won,
            "exactly one dispatch may deliver a once-listener"
        );
        assert!(table.snapshot(key()).is_empty());
    });
}

/// Claim 2: a concurrent registration never invalidates a taken snapshot.
#[test]
fn a_racing_registration_joins_the_snapshot_whole_or_not_at_all() {
    loom::model(|| {
        let table = Arc::new(ListenerTable::new());
        let early = table.insert(key(), ContextId(0), false, Arc::new(()));

        let registrar = {
            let table = Arc::clone(&table);
            thread::spawn(move || table.insert(key(), ContextId(1), false, Arc::new(())))
        };
        let snapshot = table.snapshot(key());
        assert!(!snapshot.is_empty(), "the settled registration is present");
        assert_eq!(snapshot[0].id, early, "registration order is preserved");
        assert!(snapshot.len() <= 2, "no phantom entries");

        let late = registrar.join().unwrap_or_else(|_| unreachable!());
        let settled = table.snapshot(key());
        assert_eq!(settled.len(), 2);
        assert_eq!(settled[1].id, late);
    });
}
