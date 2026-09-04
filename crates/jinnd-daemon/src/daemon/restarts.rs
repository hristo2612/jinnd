//! The daemon's pending-restart oracle (M2-K9, harness FINDINGS #31): the
//! ONE place that answers "is this incarnation already scheduled for
//! replacement?", read by both surfaces that must agree — the topic
//! registry's reply-expecting refusal and `jinn:introspect.entry.restarting`.
//! One source, so a caller that ASKS and a caller that is REFUSED can never
//! be told different things.
//!
//! The answer is a snapshot of kernel-owned state under brief locks: the
//! tracked fiber's own typed [`Owed`] (read in the same critical section as
//! its rest bit, so the moment a target write returns this already names
//! what that write asked for) and the lane's live incarnation. No guest is
//! called and nothing blocks (R1).

use std::sync::{Arc, Weak};

use jinnd_api::{FiberId, Owed};
use jinnd_wasm::{LaneCore, RestartOracle, Unserved};

use crate::support::{SharedFibers, lock};

/// The oracle over the daemon's tracked fibers and its wasm lane. The lane
/// is held WEAKLY: the lane owns the topic registry that holds this oracle,
/// so a strong handle would close a cycle and leak the whole assembly.
pub(crate) struct Restarts {
    pub(crate) lane: Weak<LaneCore>,
    pub(crate) fibers: SharedFibers,
}

impl Restarts {
    /// Builds the oracle and installs it on the lane's topic registry: from
    /// here every reply-expecting dispatch into an incarnation already
    /// scheduled for replacement is refused before it lands. The handle
    /// comes back for `jinn:introspect`, so ASKING and BEING REFUSED read
    /// one source. Weak on the lane: the lane owns the registry that holds
    /// this, and a strong handle would close a cycle.
    pub(crate) fn install(lane: &Arc<LaneCore>, fibers: &SharedFibers) -> Arc<dyn RestartOracle> {
        let oracle = Arc::new(Self {
            lane: Arc::downgrade(lane),
            fibers: Arc::clone(fibers),
        });
        let oracle = oracle as Arc<dyn RestartOracle>;
        lane.topics.watch_restarts(Arc::clone(&oracle));
        oracle
    }
}

impl Restarts {
    /// WHAT `fiber` owes, and for which entry: the fiber's own typed
    /// answer, read atomically with its rest bit. An untracked fiber owes
    /// nothing this daemon can see — honest jurisdiction, never a guess.
    fn owes(&self, fiber: FiberId) -> Option<(jinnd_api::EntryId, Owed)> {
        let fibers = lock(&self.fibers);
        let tracked = fibers.get(&fiber)?;
        let owed = tracked.fiber.owed()?;
        Some((tracked.entry.clone(), owed))
    }
}

impl RestartOracle for Restarts {
    /// What the incarnation behind `fiber` owes, if anything. Both halves
    /// must hold: the fiber owes a transition, AND an incarnation is
    /// actually INSTALLED. The second half is what keeps a first
    /// activation — which also owes a transition, and may already have
    /// registered a listener from inside `activate` — out of the refusal:
    /// it is arriving, not leaving, and nothing is being replaced. Once
    /// teardown has taken the seat the answer lapses too, and the seat's
    /// own gate takes over with its typed sealed refusal.
    ///
    /// The `owed` field is carried through UNCHANGED from the fiber (M2-K9
    /// round 2): the kernel never upgrades a disposal, a suspension, or a
    /// stall into a promised restart, because a caller obeying that
    /// promise would wait for a replacement that is not coming. The fiber
    /// earns the promise rather than defaulting to it: [`Owed::Reload`] is
    /// answered from a closed allowlist of provably scheduled states, and
    /// anything outside it stalls by construction (round 4).
    ///
    /// Every answer is TRACED with its inputs at selection time (M2-K26,
    /// PLA-360 ruling 3): whether the fiber is tracked, its rest bit,
    /// what it owes, and the lane's incarnation for its entry — so a
    /// `None` is attributable to the input that produced it, never a
    /// mystery reconstructed from the rows around it.
    fn unserved(&self, fiber: FiberId) -> Option<Unserved> {
        let resting = lock(&self.fibers)
            .get(&fiber)
            .map(|tracked| tracked.fiber.resting());
        let owes = self.owes(fiber);
        let incarnation = owes
            .as_ref()
            .and_then(|(entry, _)| self.lane.upgrade()?.incarnation(entry));
        tracing::debug!(
            fiber = fiber.0,
            tracked = resting.is_some(),
            resting,
            owed = ?owes.as_ref().map(|(_, owed)| *owed),
            entry = owes.as_ref().map(|(entry, _)| entry.0.as_str()),
            incarnation,
            "restart oracle consulted"
        );
        let (entry, owed) = owes?;
        Some(Unserved {
            entry,
            incarnation: incarnation?,
            owed,
        })
    }
}

#[cfg(test)]
mod tests;
