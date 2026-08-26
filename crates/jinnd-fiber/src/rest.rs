//! The rest gate's loom models (M1-P6c).
//!
//! "At rest" means the fiber owes no transition: the last one landed and the
//! committed state equals the latest desired one. The profile loader begins a
//! fiber-awaiting amendment only against a resting fiber (the round-2 law);
//! refusal is decided entirely from kernel-owned state, never from
//! task-locals or caller identity.
//!
//! Since round 3 the bit lives inside [`crate::steering::SteeringCell`]'s
//! critical section: every target write (restart, dispose, epoch change)
//! lowers it atomically with the write, and the supervisor's settle raises it
//! only when the movement stamp it observed is still current. Rest and target
//! can therefore never be observed out of sync — the window the round-2
//! deferred-lowering left open (restart landed, bit still raised until the
//! supervisor scheduled) is closed at the mutation site. The models below
//! claim exactly that window.

#[cfg(all(test, feature = "loom"))]
mod models {
    use jinnd_api::TransitionCause;
    use loom::sync::Arc;
    use loom::thread;

    use crate::steering::SteeringCell;

    /// The verifier's round-2 window, claimed shut: the moment `restart`
    /// returns, `resting()` answers `false` — even against a racing stale
    /// settle by the supervisor, whichever way the threads interleave.
    #[test]
    fn a_restart_is_never_observed_at_rest() {
        loom::model(|| {
            let cell = Arc::new(SteeringCell::new(None));
            let (_, observed) = cell.observed();
            assert!(cell.settle_rest(observed), "a fiber owing nothing rests");
            let supervisor = {
                let cell = Arc::clone(&cell);
                // The supervisor's settle, stamped BEFORE the restart: stale.
                thread::spawn(move || cell.settle_rest(observed))
            };
            cell.restart(TransitionCause::ExplicitRestart);
            assert!(
                !cell.resting(),
                "the moment restart returns, committed != target: never at rest"
            );
            let raised = supervisor.join().unwrap_or_else(|_| unreachable!());
            assert!(
                !raised || !cell.resting(),
                "a stale settle raised rest over a moved target"
            );
        });
    }

    /// The same window, disposal lane: a first disposal request lowers rest
    /// in its own critical section, and no stale settle can raise it back.
    #[test]
    fn a_disposal_request_is_never_observed_at_rest() {
        loom::model(|| {
            let cell = Arc::new(SteeringCell::new(None));
            let (_, observed) = cell.observed();
            assert!(cell.settle_rest(observed), "a fiber owing nothing rests");
            let supervisor = {
                let cell = Arc::clone(&cell);
                thread::spawn(move || cell.settle_rest(observed))
            };
            cell.dispose();
            assert!(
                !cell.resting(),
                "the moment dispose returns, a withdrawal is owed: never at rest"
            );
            supervisor.join().unwrap_or_else(|_| unreachable!());
            assert!(!cell.resting(), "a stale settle outlived the disposal");
        });
    }

    /// Only a settle whose stamp is current raises rest: after the writes it
    /// observed, a fresh observation settles, and the fiber rests again.
    #[test]
    fn a_current_settle_raises_rest_exactly_once_the_target_is_served() {
        loom::model(|| {
            let cell = SteeringCell::new(None);
            cell.restart(TransitionCause::ExplicitRestart);
            let (_, observed) = cell.observed();
            assert!(cell.settle_rest(observed), "the served target rests");
            assert!(cell.resting());
        });
    }
}
