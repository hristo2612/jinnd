//! The fiber-identity task-local (M1-P6c).
//!
//! A fiber's whole life runs on one supervisor task (R1): its activations and
//! its teardown replays alike. Scoping that task with the fiber's uid makes
//! one question decidable with no dependency analysis at all: "is this code
//! running on THIS fiber's own task?" The profile loader asks it to refuse an
//! operation that would await the calling task's own fiber — a self-deadlock,
//! not a race — honestly at the conflict point (the P6b refusal family).
//!
//! Task-confined like the teardown marker: no shared memory, no interleaving,
//! no lock, no loom model. Work a body spawns onto another task is outside the
//! marker by design — such work no longer executes inside the fiber's
//! supervisor and cannot block its settling.

use jinnd_api::FiberId;

tokio::task_local! {
    /// Present for the whole span of one fiber's supervisor task.
    static CURRENT: FiberId;
}

/// The fiber whose supervisor task is running the current code, if any.
#[must_use]
pub fn current_fiber() -> Option<FiberId> {
    CURRENT.try_with(|id| *id).ok()
}

/// Runs `work` identified as `fiber`'s own task. The spawn point scopes the
/// whole supervisor with this — the one site every fiber task funnels through.
pub(crate) async fn identified<F: std::future::Future>(fiber: FiberId, work: F) -> F::Output {
    CURRENT.scope(fiber, work).await
}
