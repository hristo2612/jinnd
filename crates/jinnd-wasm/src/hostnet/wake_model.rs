//! The loom models of the readiness table's racy seams (M2-K7 card: loom
//! over the readiness/suspend interleavings). The production wake task and
//! the release path drive these same transitions: a wake claims its ledger
//! append under the table lock; close/suspend/dispose take the row under it;
//! a guest read re-arms under it.

use loom::sync::{Arc, Mutex};
use loom::thread;

use super::wake::WakeTable;

/// Under every interleaving of a firing wake vs a releasing suspend, no
/// wake append lands after the release has returned (Law 2 honesty; the
/// "registration released" guarantee is a happens-before edge).
#[test]
fn no_wake_is_appended_after_take_returns() {
    loom::model(|| {
        let table = Arc::new(WakeTable::default());
        let appended = Arc::new(Mutex::new(0_u32));
        table.insert(9);

        let firer = {
            let (table, appended) = (Arc::clone(&table), Arc::clone(&appended));
            thread::spawn(move || {
                table.claim_wake(9, || {
                    *appended.lock().unwrap_or_else(|poison| poison.into_inner()) += 1;
                })
            })
        };
        let seen_at_take = {
            let (table, appended) = (Arc::clone(&table), Arc::clone(&appended));
            thread::spawn(move || {
                table.take(9);
                *appended.lock().unwrap_or_else(|poison| poison.into_inner())
            })
        };

        let fired = firer.join().unwrap_or_else(|_| panic!("firer join"));
        let at_take = seen_at_take.join().unwrap_or_else(|_| panic!("taker join"));
        let total = *appended.lock().unwrap_or_else(|poison| poison.into_inner());
        assert_eq!(
            total,
            u32::from(fired),
            "an append happens iff the wake claimed"
        );
        assert_eq!(total, at_take, "no wake append lands after take returned");
        assert!(!table.claim_wake(9, || panic!("a taken row never claims")));
        assert!(!table.rearm(9), "a taken row never re-arms");
    });
}

/// Two wakes racing one re-arm: at most one claims — a re-arm yields one
/// wake, never a burst (R9 coalescing is a lock invariant, not timing).
#[test]
fn one_rearm_yields_at_most_one_wake() {
    loom::model(|| {
        let table = Arc::new(WakeTable::default());
        table.insert(9);
        assert!(table.claim_wake(9, || {}), "the fresh row is armed");
        let rearmer = {
            let table = Arc::clone(&table);
            thread::spawn(move || table.rearm(9))
        };
        let claimers: Vec<_> = (0..2)
            .map(|_| {
                let table = Arc::clone(&table);
                thread::spawn(move || table.claim_wake(9, || {}))
            })
            .collect();
        assert!(rearmer.join().unwrap_or_else(|_| panic!("rearm join")));
        let claimed: u32 = claimers
            .into_iter()
            .map(|claimer| u32::from(claimer.join().unwrap_or_else(|_| panic!("claim join"))))
            .sum();
        // Either one claimer won the re-arm, or both ran before it and the
        // row is armed for the next readiness — never two wakes.
        assert!(claimed <= 1, "one re-arm, at most one wake");
        assert_eq!(table.armed(9), Some(claimed == 0));
    });
}
