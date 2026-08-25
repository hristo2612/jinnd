//! A swappable readiness lane: one fiber keeps one signal for its whole life,
//! while the loader may retarget which registry watch feeds it (R1: watch
//! channels and cancellation tokens, no locks anywhere near plugin code).
//!
//! Rebinding an entry's context swaps the inner watch; epoch identity is by
//! value, so a swap that resolves to the same dependencies coalesces to no
//! fiber work at all, and relevance is decided by the epoch machinery alone.

use std::sync::Arc;

use jinnd_fiber::{ReadinessSignal, ReadinessSource, WatchReadiness};
use jinnd_registry::InjectedReadiness;
use tokio_util::sync::CancellationToken;

/// The forwarding seam between one fiber and the registry watch currently
/// feeding it.
#[derive(Debug)]
pub(crate) struct ReadinessProxy {
    source: Arc<ReadinessSource>,
    stop: CancellationToken,
}

impl ReadinessProxy {
    /// A proxy initially fed by `inner`. Take the fiber's signal with
    /// [`ReadinessProxy::signal`] before spawning the fiber.
    pub(crate) fn new(inner: InjectedReadiness) -> Self {
        let mut proxy = Self {
            source: Arc::new(ReadinessSource::new(inner.epoch())),
            stop: CancellationToken::new(),
        };
        proxy.attach(inner);
        proxy
    }

    /// The signal the fiber consumes for its whole life.
    pub(crate) fn signal(&self) -> WatchReadiness {
        self.source.signal()
    }

    /// Retargets the proxy onto a new registry watch, publishing its current
    /// epoch immediately. The previous forwarder is cancelled first, so stale
    /// watches never speak again.
    pub(crate) fn attach(&mut self, inner: InjectedReadiness) {
        self.stop.cancel();
        self.stop = CancellationToken::new();
        publish(&self.source, &inner);

        let stop = self.stop.clone();
        let source = Arc::clone(&self.source);
        tokio::spawn(async move {
            let mut inner = inner;
            loop {
                let changed = inner.changed();
                tokio::select! {
                    () = stop.cancelled() => return,
                    () = changed => {}
                }
                publish(&source, &inner);
            }
        });
    }
}

impl Drop for ReadinessProxy {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

fn publish(source: &ReadinessSource, inner: &InjectedReadiness) {
    match inner.epoch() {
        Some(epoch) => source.ready(epoch),
        None => source.withdraw(),
    }
}
