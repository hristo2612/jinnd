//! The loom models of the swap core's racy seams (R8; CI runs them with
//! `--features loom --release`). The production driver and lane run these
//! same transitions — the model protects the shipped path (round-2
//! blocker-3 ruling): the commit bookkeeping runs INSIDE the claim's
//! critical section, exactly as [`super::swap_batch`] drives it.

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
            thread::spawn(move || core.commit_all_with(&[1], || ()).is_some())
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
/// the WHOLE claim — the bookkeeping never runs — and leaves the untouched
/// slots still preparing (so the driver's rollback finds them), never a
/// partial commit.
#[test]
fn batch_claim_is_all_or_nothing() {
    loom::model(|| {
        let core = Arc::new(SwapCore::default());
        assert!(core.begin(1));
        assert!(core.begin(2));

        let claimer = {
            let core = Arc::clone(&core);
            thread::spawn(move || core.commit_all_with(&[1, 2], || ()).is_some())
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
                core.commit_all_with(&[1], || ()).is_some(),
                "a refused batch claim must leave the other slots preparing"
            );
        }
        assert!(!core.begin(2), "the disposed slot never re-enters a swap");
    });
}

/// The production commit shape: the seat lands INSIDE the claim's critical
/// section, so a concurrent disposal either tombstones first — the claim
/// refuses, the staged instance is discarded, the disposer retires the OLD
/// seat — or arrives after the whole commit and retires the NEW seat. In
/// every interleaving the cell ends empty and both instances are retired
/// exactly once; the partial-commit window does not exist (round-3 ruling).
#[test]
fn commit_inside_the_claim_converges_with_disposal() {
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
                // The lane's commit: the seat swap is the bookkeeping run
                // inside the critical section; the displaced seat retires
                // after it (swap_batch's retire_displaced).
                let displaced = core.commit_all_with(&[1], || cell.lock().unwrap().replace(NEW));
                match displaced {
                    Some(Some(old)) => retired.lock().unwrap().push(old),
                    Some(None) => {}
                    // Disposal won before the claim: discard the staged one.
                    None => retired.lock().unwrap().push(NEW),
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
