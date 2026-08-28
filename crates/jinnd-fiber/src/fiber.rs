//! The handle a fiber's owner holds.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use jinnd_api::{EffectDescriptor, FiberId, FiberState, TransitionCause};
use tokio::sync::watch;

use crate::body::FiberBody;
use crate::readiness::ReadinessSignal;
use crate::record::FiberRecord;
use crate::shared::Shared;
use crate::supervisor::{Cell, supervise};
use crate::uid::next_fiber_id;

/// One instantiation of one plugin: its lifecycle cell (§3, "Fiber").
///
/// The handle is a view, never the fiber itself — the fiber lives on its own tokio
/// task. Every method here either reads a published value or states a new target and
/// returns; nothing a handle does blocks a transition or reaches into one.
///
/// Dropping the last handle disposes the fiber rather than leaking its supervisor.
/// Because a destructor cannot await, the teardown finishes in the background;
/// [`Fiber::dispose`] is how a caller waits for it.
#[derive(Debug)]
pub struct Fiber {
    shared: Arc<Shared>,
}

impl Fiber {
    /// Spawns a fiber for `body`, gated on `signal`.
    ///
    /// The fiber activates only once its dependencies are available, and reactivates
    /// through a full clean unload whenever their identity changes: there is no
    /// silent replacement under a live activation (§3, "Epoch gating").
    ///
    /// # Panics
    ///
    /// If called outside a tokio runtime, which is where every fiber lives (R1).
    pub fn spawn(body: Arc<dyn FiberBody>, signal: impl ReadinessSignal) -> Self {
        let shared = Arc::new(Shared::new(next_fiber_id(), signal.epoch()));
        // The whole supervisor task carries its fiber's identity, so any code
        // it runs — activations and teardown replays alike — can be refused
        // an operation that would await this very fiber (M1-P6c).
        tokio::spawn(crate::current::identified(
            shared.id,
            supervise(Cell::new(Arc::clone(&shared), body, Box::new(signal))),
        ));
        Self { shared }
    }

    /// This fiber's uid, which is never reused by another fiber (R3).
    #[must_use]
    pub fn id(&self) -> FiberId {
        self.shared.id
    }

    /// The last state the fiber committed to.
    #[must_use]
    pub fn state(&self) -> FiberState {
        *self.shared.state.borrow()
    }

    /// A subscription to every state the fiber commits to.
    #[must_use]
    pub fn states(&self) -> watch::Receiver<FiberState> {
        self.shared.state.subscribe()
    }

    /// The fiber's observable history as of its last landed transition (R6).
    #[must_use]
    pub fn record(&self) -> FiberRecord {
        self.shared.record()
    }

    /// The live effect tree as of the last landed transition (R5).
    #[must_use]
    pub fn effects(&self) -> Vec<EffectDescriptor> {
        self.shared.effects()
    }

    /// Asks for a full clean reload, stating why.
    ///
    /// The request is a target, not a command: if a transition is in flight it lands
    /// first, and a restart asked for twice before either takes effect is one
    /// restart. `cause` is what the transition is recorded under — a config edit and
    /// an operator restart are the same mechanism with different provenance.
    pub fn restart(&self, cause: TransitionCause) {
        self.shared.steering.restart(cause);
        self.shared.wake.notify_one();
    }

    /// True while the fiber is replaying its withdrawal — plugin-owned
    /// inverses executing, on unload, disposal, and a failed activation's
    /// cleanup alike (M1-P6b).
    ///
    /// Task-agnostic, unlike [`crate::in_teardown`]: any code the replay
    /// reaches — spawned tasks included — happens-after the bit was raised,
    /// so a conflict check made from within the replay always observes it.
    #[must_use]
    pub fn withdrawing(&self) -> bool {
        self.shared.withdrawal.active()
    }

    /// True while the fiber is at rest: its last transition landed and the
    /// committed state equals the latest desired one — nothing in flight,
    /// nothing owed (M1-P6c).
    ///
    /// The bit lowers ATOMICALLY with every target write — in the same
    /// critical section as [`Fiber::restart`], [`Fiber::dispose`], and a
    /// dependency-epoch change (the round-3 law) — so the moment such a call
    /// returns, this answers `false`; it never waits on supervisor
    /// scheduling. Causal for the fiber's own work too: the write that owes
    /// a transition happens-before the transition runs, so the body, the
    /// inverses, and tasks they spawn never observe their own fiber at rest.
    /// For anyone else the answer is advisory — a refusal built on it is
    /// honest and retryable, never a lock.
    #[must_use]
    pub fn resting(&self) -> bool {
        self.shared.steering.resting()
    }

    /// Resolves once the fiber has settled with nothing left to do, having re-read
    /// every input after this call.
    pub async fn quiesce(&self) {
        let asked = self.shared.probe.fetch_add(1, Ordering::SeqCst) + 1;
        self.shared.wake.notify_one();
        let mut settled = self.shared.settled.subscribe();
        let _ = settled
            .wait_for(|acknowledged| *acknowledged >= asked)
            .await;
    }

    /// Suspends the fiber (M2-K4; decision log 2026-08-28) and resolves once
    /// its suspend replay has completed and reported: kernel registrations
    /// released, world mutations retained, the cell resting `Disposed`
    /// under the `Suspend` cause — the entry's contribution persists
    /// exactly as a crash would have left it, reached cleanly. A disposal
    /// requested at any point still lands as one. Idempotent.
    pub async fn suspend(&self) {
        self.shared.steering.suspend();
        self.quiesce().await;
    }

    /// Disposes the fiber and resolves once its withdrawal has completed and
    /// reported: `Disposed` after a clean replay, `Failed` with the residue in the
    /// record when an inverse refused (R11) — never a state that hides the residue.
    ///
    /// Idempotent: a second call withdraws nothing, because the first one already
    /// withdrew everything exactly once.
    pub async fn dispose(&self) {
        self.shared.steering.dispose();
        self.quiesce().await;
    }
}

impl Drop for Fiber {
    fn drop(&mut self) {
        self.shared.steering.dispose();
        self.shared.wake.notify_one();
    }
}
