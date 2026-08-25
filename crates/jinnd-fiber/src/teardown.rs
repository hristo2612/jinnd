//! The teardown context marker (M1-P6b).
//!
//! A fiber's withdrawal replays plugin-owned inverses on the fiber's own task
//! (R1): whatever those inverses await runs inside the teardown of the very
//! entry some loader operation may be waiting on. Amending the profile from
//! that context is a deadlock *class*, not a scheduling accident — any wait
//! the amendment takes can close a cycle through the disposing operation,
//! whichever crates the cycle happens to thread through.
//!
//! The marker makes refusing that class decidable: the supervisor scopes
//! every withdrawal replay in a task-local, and [`in_teardown`] answers "is
//! this code running inside a fiber's teardown?" with no dependency analysis
//! at all. The profile loader consults it at every amendment's entry and
//! refuses honestly. I2 is untouched: a dying plugin is entitled to call the
//! services it leases while unloading, never to reshape the profile.

tokio::task_local! {
    /// Present exactly for the span of one fiber's withdrawal replay.
    static TEARDOWN: ();
}

/// True while the current task is executing a fiber's teardown — the
/// withdrawal replay of plugin-owned inverses, on unload, disposal, and a
/// failed activation's cleanup alike.
///
/// The marker is task-confined: it involves no shared memory and no
/// interleaving, so it needs no lock and no loom model. Work a teardown
/// spawns onto another task is outside the marker by design — such work no
/// longer executes inside the teardown and cannot block it.
#[must_use]
pub fn in_teardown() -> bool {
    TEARDOWN.try_with(|()| ()).is_ok()
}

/// Runs `work` inside the teardown context. The supervisor scopes every
/// withdrawal replay with this — the one site all teardown funnels through.
pub(crate) async fn marked<F: std::future::Future>(work: F) -> F::Output {
    TEARDOWN.scope((), work).await
}
