//! The loom models of the swap core's racy seams (R8; CI runs them with
//! `--features loom --release`). The production driver and lane run these
//! same transitions — the model protects the shipped path (round-2
//! blocker-3 ruling).

use loom::sync::{Arc, Mutex};
use loom::thread;

use super::{SlotPhase, SwapCore};

/// Under every interleaving of claim vs dispose, exactly one side owns
/// the prepared instance and a tombstoned slot is never resurrected.
#[test]
fn claim_and_dispose_agree_on_ownership() {
    loom::model(|| {
        let core = Arc::new(SwapCore::default());
        assert!(core.begin(1));

        let claimer = {
            let core = Arc::clone(&core);
            thread::spawn(move || core.commit_all(&[1]))
        };
        let disposer = {
            let core = Arc::clone(&core);
            thread::spawn(move || core.dispose(1))
        };
        let committed = claimer.join().unwrap_or_else(|_| panic!("claimer join"));
        let observed = disposer.join().unwrap_or_else(|_| panic!("disposer join"));

        // Ownership is exclusive: the claim landed iff the disposer did
        // NOT observe (and thereby claim) the preparation.
        assert_eq!(committed, observed != SlotPhase::Preparing);
        // Disposal always wins the end state.
        assert!(!core.begin(1), "a tombstoned slot never re-enters a swap");
    });
}

/// The batch claim is atomic across slots: a disposal that beats it refuses
/// the WHOLE claim and leaves the untouched slots still preparing (so the
/// driver's rollback finds them), never a partial commit.
#[test]
fn batch_claim_is_all_or_nothing() {
    loom::model(|| {
        let core = Arc::new(SwapCore::default());
        assert!(core.begin(1));
        assert!(core.begin(2));

        let claimer = {
            let core = Arc::clone(&core);
            thread::spawn(move || core.commit_all(&[1, 2]))
        };
        let disposer = {
            let core = Arc::clone(&core);
            thread::spawn(move || core.dispose(2))
        };
        let committed = claimer.join().unwrap_or_else(|_| panic!("claimer join"));
        let observed = disposer.join().unwrap_or_else(|_| panic!("disposer join"));

        assert_eq!(committed, observed != SlotPhase::Preparing);
        if !committed {
            // Nothing was half-committed: slot 1 is still Preparing, exactly
            // where the driver's rollback expects it.
            assert!(
                core.commit_all(&[1]),
                "a refused batch claim must leave the other slots preparing"
            );
        }
        assert!(!core.begin(2), "the disposed slot never re-enters a swap");
    });
}

/// The post-claim convergence: an install racing a disposal always ends with
/// the cell empty and BOTH instances retired exactly once — the installer
/// rechecks the tombstone after landing, the disposer tombstones before it
/// takes, and whoever finds the cell occupied retires what it finds.
#[test]
fn a_claimed_install_converges_with_disposal() {
    const OLD: u8 = 1;
    const NEW: u8 = 2;
    loom::model(|| {
        let core = Arc::new(SwapCore::default());
        assert!(core.begin(1));
        let cell = Arc::new(Mutex::new(Some(OLD)));
        let retired = Arc::new(Mutex::new(Vec::new()));

        let installer = {
            let (core, cell, retired) =
                (Arc::clone(&core), Arc::clone(&cell), Arc::clone(&retired));
            thread::spawn(move || {
                if core.commit_all(&[1]) {
                    // The lane's install: land the new seat, retire the
                    // displaced one, then converge on the tombstone.
                    let displaced = cell.lock().unwrap().replace(NEW);
                    if let Some(old) = displaced {
                        retired.lock().unwrap().push(old);
                    }
                    if core.is_tombstone(1)
                        && let Some(landed) = cell.lock().unwrap().take()
                    {
                        retired.lock().unwrap().push(landed);
                    }
                } else {
                    // Disposal won before the claim: discard the staged one.
                    retired.lock().unwrap().push(NEW);
                }
            })
        };
        let disposer = {
            let (core, cell, retired) =
                (Arc::clone(&core), Arc::clone(&cell), Arc::clone(&retired));
            thread::spawn(move || {
                core.dispose(1);
                if let Some(found) = cell.lock().unwrap().take() {
                    retired.lock().unwrap().push(found);
                }
            })
        };
        installer
            .join()
            .unwrap_or_else(|_| panic!("installer join"));
        disposer.join().unwrap_or_else(|_| panic!("disposer join"));

        assert!(cell.lock().unwrap().is_none(), "nothing survives disposal");
        let mut retired = retired.lock().unwrap().clone();
        retired.sort_unstable();
        assert_eq!(
            retired,
            vec![OLD, NEW],
            "each instance is retired exactly once"
        );
    });
}
