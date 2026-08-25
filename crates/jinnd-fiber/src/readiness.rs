//! The seam between a fiber and whatever decides its dependencies are available.
//!
//! A fiber never polls for availability and never reaches into a registry (§3,
//! "Services"). It consumes a signal: the identity of the environment it may
//! activate against, or `None` while any injected service is missing or failing its
//! check. The registry packet implements this trait over the real service store;
//! [`ReadinessSource`] is the watch-backed source this crate ships so the fiber
//! engine is testable and useful on its own (R10).

use std::future::{Future, pending};
use std::pin::Pin;

use jinnd_api::Epoch;
use tokio::sync::watch;

/// A boxed future with no failure mode of its own.
pub type Signal<'a> = Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

/// What a fiber is told about its dependencies.
///
/// Implementations are edge-driven: [`changed`](ReadinessSignal::changed) resolves
/// when the value may have moved, and [`epoch`](ReadinessSignal::epoch) reads the
/// current one. A signal whose source is gone never changes again, which leaves the
/// fiber holding whatever it was last told rather than spinning.
pub trait ReadinessSignal: Send + 'static {
    /// The dependency identity the fiber may activate against, if any.
    fn epoch(&self) -> Option<Epoch>;

    /// Resolves when [`epoch`](ReadinessSignal::epoch) may have changed.
    fn changed(&mut self) -> Signal<'_>;
}

/// The publishing half of a readiness signal.
///
/// Epoch identity is by value, not by counter: withdrawing a dependency and
/// restoring the very same one leaves the fiber's aim exactly where it was, which is
/// what makes a mid-flight flicker coalesce to no work at all.
#[derive(Debug)]
pub struct ReadinessSource {
    epoch: watch::Sender<Option<Epoch>>,
}

impl ReadinessSource {
    /// A source starting at `initial`.
    #[must_use]
    pub fn new(initial: Option<Epoch>) -> Self {
        Self {
            epoch: watch::Sender::new(initial),
        }
    }

    /// A source for a fiber that injects nothing, and so is always satisfied.
    #[must_use]
    pub fn independent() -> Self {
        Self::new(Some(Epoch {
            dependencies: Vec::new(),
        }))
    }

    /// Publishes that every dependency is available, at this identity.
    pub fn ready(&self, epoch: Epoch) {
        self.epoch.send_replace(Some(epoch));
    }

    /// Publishes that at least one dependency is unavailable.
    pub fn withdraw(&self) {
        self.epoch.send_replace(None);
    }

    /// A signal a fiber can consume.
    #[must_use]
    pub fn signal(&self) -> WatchReadiness {
        WatchReadiness {
            epoch: self.epoch.subscribe(),
        }
    }
}

/// The consuming half of a [`ReadinessSource`].
#[derive(Debug)]
pub struct WatchReadiness {
    epoch: watch::Receiver<Option<Epoch>>,
}

impl ReadinessSignal for WatchReadiness {
    fn epoch(&self) -> Option<Epoch> {
        self.epoch.borrow().clone()
    }

    fn changed(&mut self) -> Signal<'_> {
        Box::pin(async move {
            if self.epoch.changed().await.is_err() {
                // The source is gone. It can never speak again, so waiting forever
                // is the honest answer: a busy edge would be a lie about a change.
                pending::<()>().await;
            }
        })
    }
}
