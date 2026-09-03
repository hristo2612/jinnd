//! The kernel publish's own unit lane (R10 file hygiene: the `src/`
//! per-file cap is hard, so the suite lives beside the module rather than
//! inside it), split by what each half is ABOUT: [`isolation`] pins that no
//! listener's progress is ever a term in another's — within one publish,
//! across successive publishes, or through a trap — and [`bound`] pins what
//! a listener that cannot keep up costs and how loudly it is told.
//!
//! The fixtures both halves build listeners out of live here, so the two
//! can never drift about what a slow, trapping or parked listener is.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::time::Instant;

use jinnd_api::KernelFuture;

use crate::topics::EventTarget;

/// A listener that dawdles for `delay` on every delivery and writes
/// down how far into the test each of its deliveries finished.
pub(super) struct Timed {
    pub(super) delay: Duration,
    pub(super) start: Instant,
    pub(super) finished: Arc<Mutex<Vec<Duration>>>,
}

impl EventTarget for Timed {
    fn deliver(
        &self,
        _token: u64,
        _topic: &str,
        _payload: Vec<u8>,
        _: Option<std::num::NonZeroU64>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let delay = self.delay;
        let start = self.start;
        let finished = Arc::clone(&self.finished);
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            lock(&finished).push(start.elapsed());
            Ok(Vec::new())
        })
    }
}

/// WHERE a listener's trap lands, which is the whole of M2-K13 round 4:
/// a trap `Inside` the future is contained by the task the delivery runs
/// on, but one `Before` it is raised by `deliver` ITSELF, on whatever
/// stack made the call — so containment that wraps only the future never
/// sees it.
pub(super) enum Trap {
    Before,
    Inside,
}

/// A listener that TRAPS on its first `traps` deliveries and answers
/// after that — so a lane that survived a trap can be seen carrying
/// the next transition rather than merely not crashing.
pub(super) struct Trapping {
    pub(super) landed: Arc<Mutex<Vec<Duration>>>,
    pub(super) start: Instant,
    pub(super) seen: Arc<std::sync::atomic::AtomicUsize>,
    pub(super) traps: usize,
    pub(super) site: Trap,
}

impl EventTarget for Trapping {
    fn deliver(
        &self,
        _token: u64,
        _topic: &str,
        _payload: Vec<u8>,
        _: Option<std::num::NonZeroU64>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let landed = Arc::clone(&self.landed);
        let start = self.start;
        let attempt = self.seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let trap = attempt < self.traps;
        assert!(
            !(trap && matches!(self.site, Trap::Before)),
            "a listener trapped BEFORE returning its future"
        );
        Box::pin(async move {
            assert!(!trap, "a listener trapped inside its delivery");
            lock(&landed).push(start.elapsed());
            Ok(Vec::new())
        })
    }
}

pub(super) fn lock<T>(cell: &Arc<Mutex<T>>) -> std::sync::MutexGuard<'_, T> {
    cell.lock().unwrap_or_else(|poison| poison.into_inner())
}

/// Waits for `want` deliveries by YIELDING, never by sleeping: on a
/// paused clock virtual time advances only when every task is idle, so
/// a lane that is genuinely independent settles here with the clock
/// still reading zero, and one that is blocked behind a sibling never
/// settles at all.
pub(super) async fn settled(cell: &Arc<Mutex<Vec<Duration>>>, want: usize) -> Vec<Duration> {
    for _ in 0..10_000 {
        let landed = lock(cell).clone();
        if landed.len() >= want {
            return landed;
        }
        tokio::task::yield_now().await;
    }
    panic!(
        "only {} of {want} deliveries ever landed — the lane never ran",
        lock(cell).len()
    )
}

/// A listener whose FIRST delivery parks until released, and which
/// writes down the ordinal of every payload it is handed.
pub(super) struct Parking {
    pub(super) seen: Arc<Mutex<Vec<u64>>>,
    pub(super) started: Arc<tokio::sync::Notify>,
    pub(super) gate: Arc<tokio::sync::Semaphore>,
    pub(super) parked: Arc<std::sync::atomic::AtomicBool>,
}

impl EventTarget for Parking {
    fn deliver(
        &self,
        _token: u64,
        _topic: &str,
        payload: Vec<u8>,
        _: Option<std::num::NonZeroU64>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let ordinal: u64 = String::from_utf8_lossy(&payload)
            .parse()
            .unwrap_or_else(|error| panic!("a payload carries its ordinal: {error}"));
        lock(&self.seen).push(ordinal);
        let first = !self.parked.swap(true, std::sync::atomic::Ordering::SeqCst);
        let started = Arc::clone(&self.started);
        let gate = Arc::clone(&self.gate);
        Box::pin(async move {
            if first {
                started.notify_one();
                drop(gate.acquire().await);
            }
            Ok(Vec::new())
        })
    }
}

/// Waits, in real time, for `want` ordinals — or says how few landed.
pub(super) async fn drained(seen: &Arc<Mutex<Vec<u64>>>, want: usize) -> Vec<u64> {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let landed = lock(seen).clone();
        if landed.len() >= want || std::time::Instant::now() >= deadline {
            return landed;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

mod bound;
mod isolation;
