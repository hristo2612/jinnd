//! WHAT a fiber owes, derived from the same gap [`crate::plan`] reads
//! (M2-K9 round 3).
//!
//! The planner answers the supervisor's question — "which transition do I
//! run next?". This answers a caller's — "can this incarnation serve me,
//! and if not, what do I do about it?". Same two inputs, one derivation
//! each, so the two can never tell different stories about one fiber.
//!
//! The answers are four DIFFERENT futures, and the whole point of naming
//! them apart is that a caller acts on them differently. Only
//! [`Owed::Reload`] promises a replacement, so it is the one answer that
//! has to be EARNED: it is given only where the planner can genuinely
//! reach a `Load`. Everything else that owes a change nothing will
//! schedule answers [`Owed::Stalled`] — an honest "not served, and nothing
//! is coming until the environment moves". A cheerful wrong answer is
//! worse than no answer at all: a caller obeying "retry once the restart
//! lands" spins forever on a restart nobody scheduled, when it could have
//! handled the real state correctly.
//!
//! The match below is exhaustive over the committed state with NO
//! catch-all, and that is deliberate: this defect arrived twice through a
//! fall-through that answered `Reload` whenever nothing else matched
//! (round 1: disposal and suspension; round 2: a withdrawn dependency).
//! A new state must be named here or the crate does not compile.

use jinnd_api::{FiberState, Owed};

use crate::plan::{Committed, Desired};

/// What the gap between `committed` and `desired` owes, or `None` when the
/// committed state already SERVES the desired one.
///
/// `None` is not "at rest": the rest bit is the fiber's own answer to
/// whether it owes anything at all, and [`crate::steering::SteeringCell`]
/// reads the two in one critical section. This function answers only what
/// the gap itself says.
pub(crate) fn owed(committed: &Committed, desired: &Desired) -> Option<Owed> {
    match committed.state {
        // Terminal: the supervisor task has returned, so no round will run
        // again. A disposal — and the suspension that also lands here — is
        // SERVED; any other target is owed and can never be scheduled.
        FiberState::Disposed => {
            (!desired.disposing && !desired.suspending).then_some(Owed::Stalled)
        }
        // A transition is in flight. A disposal or suspension requested
        // meanwhile is what the round after the landing will serve; a
        // reload target is a promise only while its environment is there.
        FiberState::Loading | FiberState::Unloading => {
            Some(requested(desired).unwrap_or_else(|| schedulable(desired)))
        }
        // No live activation. Two dead ends live here, both R9: a disposal
        // whose own replay failed is never reattempted, and neither is a
        // load under the exact aim it already failed on.
        FiberState::Pending | FiberState::Failed => {
            if committed.disposal_failed {
                return Some(Owed::Stalled);
            }
            if let Some(request) = requested(desired) {
                return Some(request);
            }
            if committed.state == FiberState::Failed
                && committed.failed_under.as_ref() == Some(&desired.aim)
            {
                return Some(Owed::Stalled);
            }
            Some(schedulable(desired))
        }
        // A live activation. It serves the desired aim, or it is the one
        // being replaced.
        FiberState::Active => {
            if let Some(request) = requested(desired) {
                return Some(request);
            }
            if committed.active_for.as_ref() == Some(&desired.aim) {
                return None;
            }
            Some(schedulable(desired))
        }
    }
}

/// The sticky requests, ranked exactly as [`crate::plan::plan`] ranks them:
/// a disposal outranks a suspension, so a fiber asked for both owes the
/// disposal and its caller is never told to wait for a resume that would
/// not help.
const fn requested(desired: &Desired) -> Option<Owed> {
    if desired.disposing {
        Some(Owed::Disposal)
    } else if desired.suspending {
        Some(Owed::Suspension)
    } else {
        None
    }
}

/// A reload is a PROMISE, and it is only kept while the environment the
/// next activation needs is present: [`crate::plan::plan`] cannot `Load`
/// with no epoch, so a withdrawn dependency owes a stall rather than a
/// restart nobody scheduled (round-3 finding 1).
const fn schedulable(desired: &Desired) -> Owed {
    if desired.aim.epoch.is_some() {
        Owed::Reload
    } else {
        Owed::Stalled
    }
}

#[cfg(test)]
mod tests;
