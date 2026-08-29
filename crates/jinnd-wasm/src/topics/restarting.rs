//! The pending-transition seam behind a reply-expecting dispatch's refusal
//! (M2-K9, harness FINDINGS #31). Split from `topics.rs` by responsibility
//! (R10 file hygiene).
//!
//! K8 made `jinn:profile.patch-entry` non-blocking: it answers once the
//! document is committed and the patched fiber's restart is SCHEDULED.
//! That leaves a window in which a fiber owes a transition and is still
//! fully addressable — its seat installed, its listeners routed. A
//! dispatch that expects a reply must not land there: the incarnation is
//! being taken down, so the emitter would be waiting on a peer that may
//! never answer. The kernel refuses instead, and never QUEUES the dispatch
//! across the swap: buffering would hide a real state transition behind an
//! unbounded queue, so the refusal is the honest, fail-closed answer (R9).
//!
//! The refusal is TYPED all the way out (R3): it names the entry, the
//! incarnation, and — the part a caller acts on — WHAT the target owes.
//! Those are three different next moves, so the kernel never folds them
//! into one. Telling a caller to await a restart that is not coming is
//! worse than refusing without a reason: it talks a caller that could have
//! handled a terminal target out of handling it.

use jinnd_api::{DispatchMode, EntryId, FiberId, Owed};

/// One target whose live incarnation cannot serve a reply-expecting walk,
/// and why.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Unserved {
    /// The profile entry the incarnation serves.
    pub entry: EntryId,
    /// The incarnation refused for — never reused within a kernel process,
    /// so a caller can tell the incarnation it was refused by from the one
    /// a retry will reach.
    pub incarnation: u64,
    /// The transition the target already owes, which IS the caller's next
    /// move: [`Owed::Reload`] — retry once the restart lands;
    /// [`Owed::Disposal`] — terminal, never retry, re-resolve or give up;
    /// [`Owed::Suspension`] — retry after a resume, which may never come.
    pub owed: Owed,
}

/// Answers, from a SNAPSHOT of kernel-owned state, what transition a
/// listener's fiber already owes. No guest is called and nothing blocks
/// (R1); the answer feeds a refusal that is honest and retryable, never a
/// lock.
pub trait RestartOracle: Send + Sync + 'static {
    /// What the incarnation behind `fiber` owes, or `None` when the fiber
    /// rests, or holds no installed seat at all (a first activation is
    /// arriving, not leaving — it is never refused).
    fn unserved(&self, fiber: FiberId) -> Option<Unserved>;
}

/// True for the modes whose ANSWER carries listener outputs: the emitter
/// waits on the listeners it selected, so a delivery into an incarnation
/// the kernel is already taking down is a wait on a peer that may never
/// answer. `Emit` — the fire-and-forget mode, whose outputs are discarded
/// — is not decided here: its delivery semantics are unchanged.
pub(crate) const fn expects_reply(mode: DispatchMode) -> bool {
    !matches!(mode, DispatchMode::Emit)
}
