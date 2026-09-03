//! Loom models for the M2-K25 fault lane: the fiber engine's ONE
//! post-activation input, `fault`, racing the other writers. Split from
//! `models.rs` by responsibility (R10 file hygiene).
//!
//! A fault is a fact about ONE incarnation — the live instance behind the
//! activation that reported it died — so it carries the incarnation stamp
//! the launch minted, and a notice from an earlier incarnation is recorded
//! and never acts. What the cell owes here: a fault is never lost against
//! a racing restart, a disposal outranks it, a stale one never doom a
//! successor, and one that lands mid-unload is reconciled by the landing
//! rather than replayed as a second withdrawal.

use loom::sync::Arc;
use loom::thread;

use jinnd_api::{FiberState, TransitionCause};

use crate::plan::{Committed, Step, plan};
use crate::steering::SteeringCell;

use super::aim;

/// A cell whose incarnation `revision` is LIVE: launched, landed, active.
fn live(cell: &SteeringCell, revision: u64) -> u64 {
    cell.launch(aim(revision));
    let incarnation = cell.incarnation();
    cell.land();
    cell.commit(|committed| {
        committed.state = FiberState::Active;
        committed.active_for = Some(aim(revision));
    });
    incarnation
}

fn committed(cell: &SteeringCell) -> Committed {
    cell.observed().0
}

/// A fault racing an operator restart: both land, neither is lost, and
/// the fault names the unload — the record says why the incarnation
/// died, and the restart's aim is what the next round loads.
#[test]
fn a_fault_racing_a_restart_is_never_lost_and_names_the_unload() {
    loom::model(|| {
        let cell = Arc::new(SteeringCell::new(None));
        let incarnation = live(&cell, 0);
        let writer = Arc::clone(&cell);
        let restarting = thread::spawn(move || writer.restart(TransitionCause::ExplicitRestart));

        let acted = cell.fault(incarnation);

        restarting.join().unwrap_or_else(|_| unreachable!());
        assert!(acted, "a fault of the live incarnation acts");
        let desired = cell.desired();
        assert!(desired.faulted, "the fault was lost");
        assert_eq!(desired.aim, aim(1), "the restart was lost");
        assert_eq!(
            plan(&committed(&cell), &desired),
            Some(Step::Unload {
                cause: TransitionCause::BodyFaulted
            }),
            "the unload is recorded under the death, not the restart"
        );
        assert!(!cell.resting(), "a fault lowers rest with its write");
    });
}

/// A disposal outranks a racing fault whichever lands first: the fiber
/// still withdraws exactly once, as the disposal, and the fault stays on
/// the record without a second terminal state after `Disposed`.
#[test]
fn a_disposal_outranks_a_racing_fault() {
    loom::model(|| {
        let cell = Arc::new(SteeringCell::new(None));
        let incarnation = live(&cell, 0);
        let writer = Arc::clone(&cell);
        let disposing = thread::spawn(move || writer.dispose());

        cell.fault(incarnation);

        disposing.join().unwrap_or_else(|_| unreachable!());
        let desired = cell.desired();
        assert!(desired.faulted && desired.disposing);
        assert_eq!(
            plan(&committed(&cell), &desired),
            Some(Step::Unload {
                cause: TransitionCause::ExplicitDispose
            }),
            "a disposal wins the unload's mode"
        );
        cell.commit(|committed| committed.state = FiberState::Disposed);
        assert_eq!(
            plan(&committed(&cell), &cell.desired()),
            None,
            "nothing follows Disposed — no Failed after it"
        );
    });
}

/// A fault carrying an EARLIER incarnation's stamp never dooms the
/// successor: whether it lands before or after the successor's launch,
/// the cell holds no pending fault once both have run.
#[test]
fn a_stale_fault_never_acts_on_the_next_incarnation() {
    loom::model(|| {
        let cell = Arc::new(SteeringCell::new(None));
        let stale = live(&cell, 0);
        cell.commit(|committed| *committed = committed.unloaded());
        let launcher = Arc::clone(&cell);
        let launching = thread::spawn(move || launcher.launch(aim(1)));

        cell.fault(stale);

        launching.join().unwrap_or_else(|_| unreachable!());
        assert!(
            !cell.desired().faulted,
            "a fault from a dead incarnation reached the live one"
        );
        cell.land();
        cell.commit(|committed| {
            committed.state = FiberState::Active;
            committed.active_for = Some(aim(1));
        });
        assert_eq!(
            plan(&committed(&cell), &cell.desired()),
            None,
            "the successor serves its aim; nothing is owed"
        );
    });
}

/// A fault landing while the incarnation is already `Unloading` for a
/// restart is reconciled by that landing: the clean unload leads to the
/// restart's load, and the launch of the successor clears the fault — one
/// withdrawal, no second one, nothing owed against the successor.
#[test]
fn a_fault_during_an_in_flight_unload_is_reconciled_by_the_landing() {
    loom::model(|| {
        let cell = Arc::new(SteeringCell::new(None));
        let incarnation = live(&cell, 0);
        cell.restart(TransitionCause::ConfigChanged);
        cell.commit(|committed| committed.state = FiberState::Unloading);
        let writer = Arc::clone(&cell);
        let faulting = thread::spawn(move || writer.fault(incarnation));

        assert_eq!(
            plan(&committed(&cell), &cell.desired()),
            None,
            "a transition in flight plans nothing"
        );
        // The landing: the supervisor commits the clean unload.
        cell.commit(|committed| *committed = committed.unloaded());

        faulting.join().unwrap_or_else(|_| unreachable!());
        assert_eq!(
            plan(&committed(&cell), &cell.desired()),
            Some(Step::Load {
                aim: aim(1),
                cause: TransitionCause::ConfigChanged
            }),
            "the restart is served, never a second unload"
        );
        cell.launch(aim(1));
        assert!(!cell.desired().faulted, "the launch clears the old fault");
    });
}
