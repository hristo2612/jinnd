//! M1-P6b regression (round-1 pin, round-3 law): plugin-owned teardown runs
//! on the fiber's own task, not the disposing operation's. Any loader
//! amendment invoked from within a teardown context is REFUSED, honestly and
//! always — its own entry's and a sibling's alike. Teardown is the wrong time
//! to reshape the profile: I2 entitles a dying plugin to call the services it
//! leases, never to amend the document, and admitting any amendment from
//! teardown reopens the re-entrant deadlock class (R1).

#![cfg(not(feature = "loom"))]

mod common;

use std::any::Any;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use jinnd_api::{
    EntryId, FiberId, FiberState, KernelError, KernelFuture, Profile, TransitionCause,
};
use jinnd_context::Context;
use jinnd_effects::Disposer;
use jinnd_fiber::{Fiber, FiberBody, Setup};
use jinnd_loader::{EntryHandle, Loader, PackageLane, SpawnRequest};

use common::{Grab, entry, fixture, id, profile};

/// What each of the teardown's re-entrant loader calls observed.
pub type Observed = Arc<Mutex<Vec<String>>>;

pub fn describe(label: &str, result: &Result<(), KernelError>) -> String {
    match result {
        Ok(()) => format!("{label}: succeeded"),
        Err(error) => format!("{label}: refused: {}", error.message),
    }
}

/// A body whose one effect's undo — replayed on the fiber task at teardown —
/// re-enters the loader: once against its own mid-disposal entry, once
/// against a sibling.
struct ReenterBody {
    loader: Weak<Loader>,
    own: EntryId,
    sibling: EntryId,
    observed: Observed,
}

impl FiberBody for ReenterBody {
    fn activate<'a>(&'a self, mut setup: Setup<'a>) -> KernelFuture<'a, ()> {
        Box::pin(async move {
            let loader = self.loader.clone();
            let own = self.own.clone();
            let sibling = self.sibling.clone();
            let observed = Arc::clone(&self.observed);
            setup.effect(
                "reenter the loader on teardown",
                Disposer::future(move || async move {
                    let Some(loader) = loader.upgrade() else {
                        return Ok(());
                    };
                    let mine = loader.update_entry(&own, 7_u32).await;
                    let siblings = loader.update_entry(&sibling, 9_u32).await;
                    let mut log = observed.lock().unwrap_or_else(|poison| poison.into_inner());
                    log.push(describe("own", &mine));
                    log.push(describe("sibling", &siblings));
                    Ok(())
                }),
            )?;
            Ok(())
        })
    }
}

struct ReenterHandle {
    fiber: Arc<Fiber>,
}

impl EntryHandle for ReenterHandle {
    fn id(&self) -> FiberId {
        self.fiber.id()
    }

    fn state(&self) -> FiberState {
        self.fiber.state()
    }

    fn withdrawing(&self) -> bool {
        self.fiber.withdrawing()
    }

    fn restart(&self, cause: TransitionCause) {
        self.fiber.restart(cause);
    }

    fn restate(&self, _config: &(dyn Any + Send + Sync)) -> Result<(), KernelError> {
        Ok(())
    }

    fn rebind(&self, _at: Context<()>) {}

    fn dispose(&self) -> KernelFuture<'static, ()> {
        let fiber = Arc::clone(&self.fiber);
        Box::pin(async move {
            fiber.dispose().await;
            Ok(())
        })
    }

    fn quiesce(&self) -> KernelFuture<'static, ()> {
        let fiber = Arc::clone(&self.fiber);
        Box::pin(async move {
            fiber.quiesce().await;
            Ok(())
        })
    }
}

/// The shared fixture loader plus one `test/reenter` lane whose teardown
/// re-enters it.
fn reenter_fixture() -> (Arc<Loader>, Observed, common::Log) {
    let (loader, _registry, log) = fixture();
    let loader = Arc::new(loader);
    let observed: Observed = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&observed);
    let captured: Weak<Loader> = Arc::downgrade(&loader);
    loader
        .register_lane::<u32>(
            "test/reenter",
            PackageLane {
                injects: Vec::new(),
                provides: None,
                spawn: Box::new(move |request: SpawnRequest<'_>| {
                    let body = Arc::new(ReenterBody {
                        loader: captured.clone(),
                        own: request.entry.clone(),
                        sibling: id("b"),
                        observed: Arc::clone(&recorded),
                    });
                    let fiber = Fiber::spawn(body as Arc<dyn FiberBody>, request.signal);
                    Ok(Arc::new(ReenterHandle {
                        fiber: Arc::new(fiber),
                    }) as Arc<dyn EntryHandle>)
                }),
            },
        )
        .grab();
    (loader, observed, log)
}

#[tokio::test]
async fn teardown_reentering_the_loader_is_refused_never_deadlocked() {
    let (loader, observed, log) = reenter_fixture();
    loader
        .reconcile(profile(vec![
            entry("a", "test/reenter", 1),
            entry("b", "test/count", 1),
        ]))
        .await
        .grab();

    // The old admission gate deadlocked here: the disposal held its permit
    // while the fiber task's teardown waited on it. A timeout is the honest
    // failure mode for a regression.
    tokio::time::timeout(
        Duration::from_secs(5),
        loader.dispose_entry::<u32>(&id("a")),
    )
    .await
    .grab()
    .grab();

    let observed = observed
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    assert_eq!(observed.len(), 2, "the teardown probed both entries");
    assert!(
        observed[0].starts_with("own: refused"),
        "amending the entry mid-disposal must be refused honestly, got: {}",
        observed[0]
    );
    assert!(
        observed[1].starts_with("sibling: refused"),
        "a sibling amendment from teardown context is refused too, got: {}",
        observed[1]
    );

    // Only the disposal's own truth landed: the entry is disabled, and the
    // refused sibling amendment left the sibling exactly as it was.
    assert_eq!(common::activations(&log, "b"), 1);
    let committed: Profile<u32> = loader.persisted().grab();
    let disposed = committed
        .entries
        .iter()
        .find(|spec| spec.id == id("a"))
        .grab();
    assert!(disposed.disabled, "the disposal committed its own view");
    let sibling = committed
        .entries
        .iter()
        .find(|spec| spec.id == id("b"))
        .grab();
    assert_eq!(
        sibling.config, 1,
        "a refused amendment commits nothing anywhere"
    );
    assert!(loader.entry_faults().is_empty());
}
