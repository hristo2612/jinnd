//! The pending-transition oracle's own probes (M2-K9 round 2), in the
//! repo's `<module>/tests.rs` convention.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::Semaphore;

use jinnd_api::{EntryId, Epoch, FiberId, KernelFuture};
use jinnd_fiber::{Fiber, FiberBody, ReadinessSource, Setup};

use super::{Owed, Restarts, SharedFibers};
use crate::support::Tracked;

/// A body that announces its arrival and then blocks until released:
/// the fiber can be held mid-transition for as long as the test needs,
/// so what it owes is OBSERVED rather than raced for.
struct Gated {
    entered: Semaphore,
    release: Arc<Semaphore>,
}

impl Gated {
    fn new() -> Self {
        Self {
            entered: Semaphore::new(0),
            release: Arc::new(Semaphore::new(0)),
        }
    }
}

impl FiberBody for Gated {
    fn activate<'a>(&'a self, _: Setup<'a>) -> KernelFuture<'a, ()> {
        self.entered.add_permits(1);
        let release = Arc::clone(&self.release);
        Box::pin(async move {
            if let Ok(permit) = release.acquire().await {
                permit.forget();
            }
            Ok(())
        })
    }
}

/// One tracked fiber whose dependencies are already satisfied, plus the
/// roster the oracle reads it from.
fn tracked(entry: &str) -> (SharedFibers, Arc<Fiber>, ReadinessSource, Arc<Gated>) {
    let source = ReadinessSource::new(Some(Epoch {
        dependencies: Vec::new(),
    }));
    let body = Arc::new(Gated::new());
    let fiber = Arc::new(Fiber::spawn(
        Arc::clone(&body) as Arc<dyn FiberBody>,
        source.signal(),
    ));
    let mut rows = HashMap::new();
    rows.insert(
        fiber.id(),
        Tracked {
            fiber: Arc::clone(&fiber),
            entry: EntryId(entry.to_owned()),
            recorded: 0,
        },
    );
    (Arc::new(Mutex::new(rows)), fiber, source, body)
}

/// Resolves once the body's activation has arrived at its gate.
async fn entered(body: &Arc<Gated>) {
    if let Ok(permit) = body.entered.acquire().await {
        permit.forget();
    }
}

/// Waits, bounded, for the oracle to report `expected`, then asserts
/// it. A budget rather than a spin: a wrong answer is a legible failed
/// assertion naming what the oracle actually said.
async fn settles(oracle: &Restarts, fiber: FiberId, expected: Owed) {
    for _ in 0..10_000u32 {
        if oracle.owes(fiber).map(|(_, owed)| owed) == Some(expected) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        oracle.owes(fiber).map(|(_, owed)| owed),
        Some(expected),
        "the oracle reports what the fiber ACTUALLY owes: a caller sent \
         to wait for a restart that is not coming waits forever"
    );
}

fn over(fibers: &SharedFibers) -> Restarts {
    Restarts {
        // No lane: `owes` is the half under test, and it reads the
        // tracked fibers alone.
        lane: Weak::new(),
        fibers: Arc::clone(fibers),
    }
}

/// M2-K9 round 2: the oracle carries the fiber's OWN answer through
/// unchanged. Mapping every non-resting fiber to a promised restart is
/// the defect: a caller refused by a fiber that is being disposed and
/// told to "retry after the restart" waits for something that is never
/// coming, when it could have handled a terminal target correctly.
#[tokio::test]
async fn the_oracle_never_promises_a_restart_for_a_disposal_or_a_suspension() {
    let (fibers, fiber, _source, body) = tracked("consumer");
    let oracle = over(&fibers);
    body.release.add_permits(1);
    fiber.quiesce().await;
    assert_eq!(
        oracle.owes(fiber.id()),
        None,
        "a resting fiber owes nothing"
    );

    body.release.add_permits(1);
    fiber.restart(jinnd_api::TransitionCause::ExplicitRestart);
    assert_eq!(
        oracle.owes(fiber.id()),
        Some((EntryId("consumer".to_owned()), Owed::Reload)),
        "a restart replaces the incarnation, and names the entry"
    );
    fiber.quiesce().await;

    // Disposal: terminal. The withdrawal is awaited to completion, so
    // the observation is taken from inside the disposal that follows.
    let (rows, doomed, _doomed_source, held) = tracked("doomed");
    let disposing = over(&rows);
    entered(&held).await;
    let withdrawal = {
        let doomed = Arc::clone(&doomed);
        tokio::spawn(async move { doomed.dispose().await })
    };
    // The activation is still held at the gate, so the fiber cannot
    // rest and the answer cannot lapse under the wait. Bounded, so a
    // WRONG answer fails legibly instead of spinning.
    settles(&disposing, doomed.id(), Owed::Disposal).await;
    assert_eq!(
        disposing.owes(doomed.id()),
        Some((EntryId("doomed".to_owned()), Owed::Disposal)),
        "terminal, and told so"
    );
    held.release.add_permits(1);
    withdrawal.await.unwrap_or_else(|error| panic!("{error}"));

    // Suspension: its own answer — a resume is not a restart.
    let (rows, paused, _paused_source, stopping) = tracked("paused");
    let suspending = over(&rows);
    entered(&stopping).await;
    let stop = {
        let paused = Arc::clone(&paused);
        tokio::spawn(async move { paused.suspend().await })
    };
    settles(&suspending, paused.id(), Owed::Suspension).await;
    assert_eq!(
        suspending.owes(paused.id()),
        Some((EntryId("paused".to_owned()), Owed::Suspension)),
        "a resume is not a restart"
    );
    stopping.release.add_permits(1);
    stop.await.unwrap_or_else(|error| panic!("{error}"));
}
