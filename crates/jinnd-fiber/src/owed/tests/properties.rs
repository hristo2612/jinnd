//! The laws, proved as IMPLICATIONS over the whole input space rather
//! than case by case (M2-K9 round 3, both directions from round 4):
//! wherever the derivation answers [`Owed::Reload`] a replacement must
//! genuinely be schedulable, and wherever it answers [`Owed::Stalled`] the
//! planner must not be about to load.
//!
//! The generator below is the whole cartesian product of the space, so the
//! verifier's dependency-loss probe is a sample it would have produced
//! unaided — which is the point. The named cases next door say what each
//! corner means; they do not carry the proof.

use jinnd_api::{FiberState, Owed};

use super::{aim, desired, unsatisfied};
use crate::owed::owed;
use crate::plan::{Committed, Desired, Step, plan};

/// Every state the planner knows: enumerated here so a new one is a
/// failing case rather than an untested corner.
const STATES: [FiberState; 6] = [
    FiberState::Pending,
    FiberState::Loading,
    FiberState::Active,
    FiberState::Failed,
    FiberState::Unloading,
    FiberState::Disposed,
];

/// Whether the planner can reach a `Load` from `committed`, following the
/// supervisor's own landings.
///
/// Only the CLEAN landing is modeled, and a load already in flight counts
/// as reached: both make this the OPTIMISTIC reading of what a fiber can
/// still do (an unclean landing rests `Failed`, which can do strictly
/// less). A `Reload` the optimistic reading cannot justify is therefore a
/// promise the kernel definitely cannot keep — exactly the implication
/// under test.
fn reaches_load(start: &Committed, desired: &Desired) -> bool {
    let mut committed = start.clone();
    for _ in 0..4 {
        match plan(&committed, desired) {
            Some(Step::Load { .. }) => return true,
            // A disposal or a suspension is being finished; no load follows.
            Some(Step::Finish) => return false,
            Some(Step::Unload { .. }) => {
                committed.state = FiberState::Pending;
                committed.active_for = None;
            }
            None => match committed.state {
                // A load is in flight: the replacement is not merely
                // schedulable, it is already running.
                FiberState::Loading => return true,
                FiberState::Unloading => {
                    committed.state = FiberState::Pending;
                    committed.active_for = None;
                }
                FiberState::Pending
                | FiberState::Active
                | FiberState::Failed
                | FiberState::Disposed => return false,
            },
        }
    }
    false
}

/// Every (committed, desired) pair the space can present, reachable or
/// not. Testing a superset costs nothing here and cannot produce a false
/// pass: a derivation that holds over the superset holds on the reachable
/// part of it.
fn every_pair() -> Vec<(Committed, Desired)> {
    let aims = [Some(aim(0, 0)), Some(aim(0, 1)), None];
    let mut pairs = Vec::new();
    for state in STATES {
        for active_for in &aims {
            for failed_under in &aims {
                for disposal_failed in [false, true] {
                    for disposing in [false, true] {
                        for suspending in [false, true] {
                            for target in [aim(0, 0), aim(0, 1), unsatisfied()] {
                                pairs.push((
                                    Committed {
                                        state,
                                        active_for: active_for.clone(),
                                        failed_under: failed_under.clone(),
                                        disposal_failed,
                                    },
                                    Desired {
                                        disposing,
                                        suspending,
                                        ..desired(target)
                                    },
                                ));
                            }
                        }
                    }
                }
            }
        }
    }
    pairs
}

/// The law: `Reload` is the one answer that PROMISES a replacement, so it
/// is given only where a replacement is genuinely schedulable. Round 1
/// broke this for disposal and suspension, round 2 for a withdrawn
/// dependency — both through a fall-through that answered `Reload`
/// whenever nothing else matched. The implication closes the class.
#[test]
fn a_reload_is_only_promised_where_a_load_is_actually_reachable() {
    let mut promised = 0u32;
    for (committed, desired) in every_pair() {
        if owed(&committed, &desired) == Some(Owed::Reload) {
            promised += 1;
            assert!(
                reaches_load(&committed, &desired),
                "promised a restart nobody scheduled: {committed:?} / {desired:?}"
            );
        }
    }
    assert!(promised > 0, "the enumeration never exercised a promise");
}

/// The converse half: owing NOTHING means the committed state serves the
/// desired one, so the planner has nothing to run either. Without it a
/// "serves fine" answer could hide a real pending transition and put the
/// stall back that this packet exists to remove.
#[test]
fn owing_nothing_means_the_planner_has_nothing_to_run() {
    for (committed, desired) in every_pair() {
        if owed(&committed, &desired).is_none() {
            assert_eq!(
                plan(&committed, &desired),
                None,
                "owed nothing while a step was planned: {committed:?} / {desired:?}"
            );
        }
    }
}

/// The guard that keeps the inversion from degenerating: answering
/// `Stalled` to everything would satisfy "never promise a replacement
/// falsely" and destroy the packet, so a stall is forbidden wherever the
/// planner would schedule a load RIGHT NOW. The two properties together
/// pin the answer from both sides.
#[test]
fn a_stall_is_never_given_where_the_planner_would_load_right_now() {
    let mut stalled = 0u32;
    for (committed, desired) in every_pair() {
        if owed(&committed, &desired) == Some(Owed::Stalled) {
            stalled += 1;
            assert!(
                !matches!(plan(&committed, &desired), Some(Step::Load { .. })),
                "stalled a caller while a load was the planned step: {committed:?} / {desired:?}"
            );
        }
    }
    assert!(stalled > 0, "the enumeration never exercised a stall");
}
