//! The loom models of the alarm table's racy seams (M2-K2 card: loom over
//! the timer/seat interleavings). The production timer task and the seat's
//! retire path drive these same transitions: a wake claims its ledger
//! append under the table lock; cancel/finish take the row under it.

use loom::sync::{Arc, Mutex};
use loom::thread;

use super::table::AlarmTable;

/// Under every interleaving of a firing wake vs a cancelling teardown, no
/// wake append lands after the cancel has returned — the "undo cancels the
/// alarm" guarantee is a happens-before edge, not best effort (R5, I1).
#[test]
fn no_wake_is_appended_after_cancel_returns() {
    loom::model(|| {
        let table: Arc<AlarmTable<()>> = Arc::new(AlarmTable::default());
        let appended = Arc::new(Mutex::new(0_u32));
        let id = table.arm();

        let firer = {
            let (table, appended) = (Arc::clone(&table), Arc::clone(&appended));
            thread::spawn(move || {
                table.claim_wake(id, || {
                    *appended.lock().unwrap_or_else(|poison| poison.into_inner()) += 1;
                })
            })
        };
        let seen_at_cancel = {
            let (table, appended) = (Arc::clone(&table), Arc::clone(&appended));
            thread::spawn(move || {
                table.take(id);
                // Snapshot AFTER the cancel returned: nothing may append past it.
                *appended.lock().unwrap_or_else(|poison| poison.into_inner())
            })
        };

        let fired = firer.join().unwrap_or_else(|_| panic!("firer join"));
        let at_cancel = seen_at_cancel
            .join()
            .unwrap_or_else(|_| panic!("canceller join"));
        let total = *appended.lock().unwrap_or_else(|poison| poison.into_inner());

        assert_eq!(
            total,
            u32::from(fired),
            "an append happens iff the wake claimed"
        );
        assert_eq!(
            total, at_cancel,
            "no wake append lands after cancel returned"
        );
        assert!(!table.claim_wake(id, || panic!("a taken row never claims")));
    });
}

/// One-shot completion and cancellation race for the same row: exactly one
/// side takes it — a double take (double abort, double withdrawal ledger
/// line) cannot happen.
#[test]
fn finish_and_cancel_agree_on_ownership() {
    loom::model(|| {
        let table: Arc<AlarmTable<()>> = Arc::new(AlarmTable::default());
        let id = table.arm();
        assert!(table.install(id, ()));

        let finisher = {
            let table = Arc::clone(&table);
            thread::spawn(move || table.take(id).is_some())
        };
        let canceller = {
            let table = Arc::clone(&table);
            thread::spawn(move || table.take(id).is_some())
        };
        let finished = finisher.join().unwrap_or_else(|_| panic!("finisher join"));
        let cancelled = canceller
            .join()
            .unwrap_or_else(|_| panic!("canceller join"));

        assert!(finished != cancelled, "exactly one side owns the row");
        assert!(!table.alive(id));
    });
}

/// An arm racing its own cancellation (a seat torn down mid-activation):
/// whatever the interleaving, `install` tells the spawner the truth — a
/// handle installed into a live row stays owned by the row, and an install
/// refused means the row is already gone.
#[test]
fn install_and_take_agree_on_the_handle() {
    loom::model(|| {
        let table: Arc<AlarmTable<u8>> = Arc::new(AlarmTable::default());
        let id = table.arm();

        let installer = {
            let table = Arc::clone(&table);
            thread::spawn(move || table.install(id, 7))
        };
        let taker = {
            let table = Arc::clone(&table);
            thread::spawn(move || table.take(id))
        };
        let installed = installer
            .join()
            .unwrap_or_else(|_| panic!("installer join"));
        let taken = taker.join().unwrap_or_else(|_| panic!("taker join"));

        // The one take always finds the armed row, and it carries the
        // handle iff the install landed first — never a lost handle.
        assert_eq!(taken, Some(installed.then_some(7)));
        // A refused install means the taker won and owns nothing to abort;
        // an accepted install's handle went to the taker exactly once.
        assert!(!table.alive(id));
    });
}
