//! The loom model of the swap core's racy seam (R8; CI runs it with
//! `--features loom --release`).

use loom::sync::Arc;
use loom::thread;

use super::{SlotPhase, SwapCore};

/// Under every interleaving of commit vs dispose, exactly one side owns
/// the prepared instance and a tombstoned slot is never resurrected.
#[test]
fn commit_and_dispose_agree_on_ownership() {
    loom::model(|| {
        let core = Arc::new(SwapCore::default());
        assert!(core.begin(1));

        let committer = {
            let core = Arc::clone(&core);
            thread::spawn(move || core.commit(1))
        };
        let disposer = {
            let core = Arc::clone(&core);
            thread::spawn(move || core.dispose(1))
        };
        let committed = committer
            .join()
            .unwrap_or_else(|_| panic!("committer join"));
        let observed = disposer.join().unwrap_or_else(|_| panic!("disposer join"));

        // Ownership is exclusive: the commit landed iff the disposer did
        // NOT observe (and thereby claim) the preparation.
        assert_eq!(committed, observed != SlotPhase::Preparing);
        // Disposal always wins the end state.
        assert!(!core.begin(1), "a tombstoned slot never re-enters a swap");
    });
}
