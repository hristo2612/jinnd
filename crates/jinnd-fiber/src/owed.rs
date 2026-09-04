//! WHAT a fiber owes, derived from the same gap [`crate::plan`] reads.
//!
//! The planner answers the supervisor's question — "which transition do I
//! run next?". This answers a caller's — "can this incarnation serve me,
//! and if not, what do I do about it?". Same two inputs, one derivation
//! each, so the two can never tell different stories about one fiber.
//!
//! The answers are four DIFFERENT futures, and the whole point of naming
//! them apart is that a caller acts on them differently. Only
//! [`Owed::Reload`] promises a replacement, so it is the one answer that
//! has to be EARNED. Everything that owes a change nothing will schedule
//! answers [`Owed::Stalled`] — an honest "not served, and nothing is coming
//! until the environment moves". A cheerful wrong answer is worse than no
//! answer at all: a caller obeying "retry once the restart lands" spins
//! forever on a restart nobody scheduled, when it could have handled the
//! real state correctly.
//!
//! # The polarity (M2-K9 round 4)
//!
//! Three rounds each found one more state that falsely promised a
//! replacement — disposal and suspension, then a withdrawn dependency, then
//! a failed activation's cleanup — and each was fixed by naming the state
//! that must not promise. Enumerating the bad states failed three times,
//! because exhaustiveness over the states somebody NAMED cannot cover the
//! one nobody did.
//!
//! So the default is inverted. [`reload_scheduled`] is a closed allowlist:
//! `Reload` is answered from inside it and nowhere else, and every arm
//! carries its proof in the planner's own answer rather than in a claim
//! about the state. Everything that falls past it — a state named here, a
//! state not named here, a state that does not exist yet — lands on
//! `Stalled` BY CONSTRUCTION, not because someone remembered to add it.
//! The property tests pin both directions: a `Reload` is never given where
//! no load is reachable, and a `Stalled` is never given where the planner
//! would load right now.

use jinnd_api::{FiberState, Owed, TransitionCause};

use crate::plan::{Committed, Desired, Step, plan};

/// What the gap between `committed` and `desired` owes, or `None` when the
/// committed state already SERVES the desired one.
///
/// `None` is not "at rest": the rest bit is the fiber's own answer to
/// whether it owes anything at all, and [`crate::steering::SteeringCell`]
/// reads the two in one critical section. This function answers only what
/// the gap itself says.
pub(crate) fn owed(committed: &Committed, desired: &Desired) -> Option<Owed> {
    if serves(committed, desired) {
        return None;
    }
    if let Some(request) = requested(committed, desired) {
        return Some(request);
    }
    if reload_scheduled(committed, desired) {
        return Some(Owed::Reload);
    }
    // The conservative floor. Reaching it is not a failure of the
    // derivation, it IS the derivation: nothing above could prove a caller
    // is served or that a replacement is coming, so the caller is told the
    // truth — do not retry blindly.
    Some(Owed::Stalled)
}

/// The two ways a committed state already SERVES its target.
///
/// Named exhaustively like everything else here: a state that is not one of
/// these owes something, and the rest of the derivation says what.
fn serves(committed: &Committed, desired: &Desired) -> bool {
    match committed.state {
        // Disposal is terminal, and a clean suspend replay rests here too:
        // whichever of the two was asked for has been served in full.
        FiberState::Disposed => desired.disposing || desired.suspending,
        // A live activation made for exactly this aim, with nothing sticky
        // asked of it since.
        FiberState::Active => {
            !desired.disposing
                && !desired.suspending
                && !desired.faulted
                && committed.active_for.as_ref() == Some(&desired.aim)
        }
        FiberState::Pending | FiberState::Loading | FiberState::Failed | FiberState::Unloading => {
            false
        }
    }
}

/// The sticky requests, ranked exactly as [`crate::plan::plan`] ranks them:
/// a disposal outranks a suspension, so a fiber asked for both owes the
/// disposal and its caller is never told to wait for a resume that would
/// not help.
///
/// A disposal whose own replay failed is NOT answered here. R9 does not
/// reattempt it against an unchanged scope, so nothing will serve the
/// request; it falls through to the stall like any other dead end.
fn requested(committed: &Committed, desired: &Desired) -> Option<Owed> {
    if committed.disposal_failed {
        return None;
    }
    if desired.disposing {
        Some(Owed::Disposal)
    } else if desired.suspending {
        Some(Owed::Suspension)
    } else {
        None
    }
}

/// THE ALLOWLIST — the only door to [`Owed::Reload`], and a closed one.
///
/// Three arms, each proving a replacement is scheduled by producing the
/// planner's own answer rather than by asserting something about the state:
///
/// 1. the planner schedules the `Load` itself, now; or
/// 2. the planner schedules a restart's `Unload`, and the state that unload
///    COMMITS when it lands cleanly — [`Committed::unloaded`], the very
///    value the supervisor writes — schedules a `Load` from the same
///    planner. An unclean landing commits `Failed` instead, and commits it
///    before anyone can read it (the round-4 ordering law).
///
/// Anything else, including any state added after this was written,
/// answers `false` and therefore stalls.
fn reload_scheduled(committed: &Committed, desired: &Desired) -> bool {
    match plan(committed, desired) {
        Some(Step::Load { .. }) => true,
        Some(Step::Unload { cause }) if !terminal(&cause) => {
            matches!(
                plan(&committed.unloaded(), desired),
                Some(Step::Load { .. })
            )
        }
        // 3. a transition is IN FLIGHT (M2-K26 (d)): the planner answers
        //    `None` because it will not plan a second one, but the landing
        //    is known — a clean unload rests on `unloaded()`, a clean load
        //    serves outright, and either way the same planner loads from
        //    the unloaded projection for a satisfiable target. An unclean
        //    landing commits `Failed` BEFORE its cleanup (round-4 law), so
        //    it is never read here as in flight; a fault already reported
        //    for the incarnation (M2-K25) lands `Failed` too, and stalls.
        None if in_flight(committed.state) && !desired.faulted => {
            matches!(
                plan(&committed.unloaded(), desired),
                Some(Step::Load { .. })
            )
        }
        Some(Step::Unload { .. } | Step::Finish) | None => false,
    }
}

/// A transition already launched and not yet landed: the one shape the
/// planner declines to plan over, and the one the allowlist proves from
/// its landing instead (M2-K26 (d)).
const fn in_flight(state: FiberState) -> bool {
    matches!(state, FiberState::Loading | FiberState::Unloading)
}

/// An unload that ENDS this fiber's service rather than replacing it. A
/// fault's unload lands `Failed` (M2-K25), which R9 never reloads from
/// under the same aim — so it promises no replacement either.
const fn terminal(cause: &TransitionCause) -> bool {
    matches!(
        cause,
        TransitionCause::ExplicitDispose | TransitionCause::Suspend | TransitionCause::BodyFaulted
    )
}

#[cfg(test)]
mod tests;
