//! The pending-restart seam behind a reply-expecting dispatch's refusal
//! (M2-K9, harness FINDINGS #31). Split from `topics.rs` by responsibility
//! (R10 file hygiene).
//!
//! K8 made `jinn:profile.patch-entry` non-blocking: it answers once the
//! document is committed and the patched fiber's restart is SCHEDULED.
//! That leaves a window in which a fiber owes a restart and is still fully
//! addressable — its seat installed, its listeners routed. A dispatch that
//! expects a reply must not land there: the incarnation is being taken
//! down, so the emitter would be waiting on a peer that may never answer.
//! The kernel refuses instead, and never QUEUES the dispatch across the
//! swap: buffering would hide a real state transition behind an unbounded
//! queue, so the refusal is the honest, fail-closed answer (R9).

use jinnd_api::{DispatchMode, EntryId, ErrorCode, FiberId, KernelError};

/// One target whose live incarnation is already scheduled for replacement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Restarting {
    /// The profile entry the doomed incarnation serves.
    pub entry: EntryId,
    /// The incarnation being replaced — never reused within a kernel
    /// process, so a caller can tell the incarnation it was refused for
    /// from the one its retry will reach.
    pub incarnation: u64,
}

/// Answers, from a SNAPSHOT of kernel-owned state, whether a listener's
/// fiber owes a lifecycle transition that replaces or retires the
/// incarnation holding it. No guest is called and nothing blocks (R1); the
/// answer feeds a refusal that is honest and retryable, never a lock.
pub trait RestartOracle: Send + Sync + 'static {
    /// The doomed incarnation behind `fiber`, or `None` when the fiber
    /// rests, or holds no installed seat at all (a first activation is
    /// arriving, not leaving — it is never refused as "restarting").
    fn restarting(&self, fiber: FiberId) -> Option<Restarting>;
}

/// True for the modes whose ANSWER carries listener outputs: the emitter
/// waits on the listeners it selected, so a delivery into an incarnation
/// the kernel is already taking down is a wait on a peer that may never
/// answer. `Emit` — the fire-and-forget mode, whose outputs are discarded
/// — is not decided here: its delivery semantics are unchanged.
pub(crate) const fn expects_reply(mode: DispatchMode) -> bool {
    !matches!(mode, DispatchMode::Emit)
}

/// The typed refusal the emitting guest receives: it names the target and
/// the incarnation, so a caller acts on identity rather than on prose.
pub(crate) fn refusal(topic: &str, target: &Restarting) -> KernelError {
    KernelError {
        code: ErrorCode::Restarting,
        message: format!(
            "dispatch of {topic:?} refused: entry {:?} incarnation {} is already scheduled for \
             replacement; retry once its restart lands",
            target.entry.0, target.incarnation
        ),
        fiber: None,
    }
}
