//! M1-P6b regression (round-3 pin, round-4 law): a consumer's teardown moves
//! its provider amendment into a SPAWNED task and awaits it. No task-local
//! marker survives a plugin's own `tokio::spawn`, so caller identification
//! can never refuse this shape — the round-4 law refuses it structurally
//! instead: the loader never begins a fiber-awaiting amendment while a
//! withdrawal replay is in flight, whoever asks and from whatever task. The
//! spawned call happens-after the withdrawal began, so it always observes the
//! conflict and is refused honestly — never parked, never deadlocked (R1).

#![cfg(not(feature = "loom"))]

mod common;

use std::any::Any;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use jinnd_api::{
    EntryId, FiberId, FiberState, KernelError, KernelFuture, Profile, ServiceContract, ServiceType,
    TransitionCause,
};
use jinnd_context::Context;
use jinnd_effects::Disposer;
use jinnd_fiber::{Fiber, FiberBody, Setup};
use jinnd_loader::{EntryHandle, Loader, PackageLane, SpawnRequest};
use jinnd_registry::Registry;

use common::{FixtureService, Grab, activations, entry, fixture, id, profile};

/// What the teardown's spawned provider amendment observed.
type Observed = Arc<Mutex<Vec<String>>>;

/// A consumer body that leases the provider's service and whose teardown —
/// with the lease still held, LIFO — spawns a task amending the provider
/// entry and awaits it: the verifier's round-3 probe shape.
struct SpawnerBody {
    loader: Weak<Loader>,
    provider: EntryId,
    registry: Registry,
    at: Context<()>,
    observed: Observed,
}

impl FiberBody for SpawnerBody {
    fn activate<'a>(&'a self, mut setup: Setup<'a>) -> KernelFuture<'a, ()> {
        Box::pin(async move {
            let (handle, guard) = self.registry.lease::<FixtureService, ()>(&self.at)?;
            let _ = handle.service.observe();
            // Registered first, replayed last: the lease outlives the
            // spawned callback below, exactly the deadlock's shape.
            setup.effect(
                "hold the provider lease",
                Disposer::sync(move || {
                    drop(guard);
                    Ok(())
                }),
            )?;
            let loader = self.loader.clone();
            let provider = self.provider.clone();
            let observed = Arc::clone(&self.observed);
            setup.effect(
                "spawn a provider amendment on teardown",
                Disposer::future(move || async move {
                    let spawned = tokio::spawn(async move {
                        let Some(loader) = loader.upgrade() else {
                            return Ok(());
                        };
                        loader.update_entry(&provider, 5_u32).await
                    });
                    let result = spawned
                        .await
                        .unwrap_or_else(|_join| unreachable!("the spawned amendment panicked"));
                    observed
                        .lock()
                        .unwrap_or_else(|poison| poison.into_inner())
                        .push(match &result {
                            Ok(()) => "provider: succeeded".to_owned(),
                            Err(error) => format!("provider: refused: {}", error.message),
                        });
                    Ok(())
                }),
            )?;
            Ok(())
        })
    }
}

struct SpawnerHandle {
    fiber: Arc<Fiber>,
}

impl EntryHandle for SpawnerHandle {
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

/// The shared fixture plus one `test/spawner` lane: a consumer of the fixture
/// service whose teardown spawns an amendment of its provider.
fn spawner_fixture() -> (Arc<Loader>, Observed, common::Log) {
    let (loader, registry, log) = fixture();
    let loader = Arc::new(loader);
    let observed: Observed = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&observed);
    let captured: Weak<Loader> = Arc::downgrade(&loader);
    loader
        .register_lane::<u32>(
            "test/spawner",
            PackageLane {
                injects: vec![ServiceType::of::<FixtureService>()],
                provides: None,
                spawn: Box::new(move |request: SpawnRequest<'_>| {
                    let body = Arc::new(SpawnerBody {
                        loader: captured.clone(),
                        provider: id("p"),
                        registry: registry.clone(),
                        at: request.at.clone(),
                        observed: Arc::clone(&recorded),
                    });
                    let fiber = Fiber::spawn(body as Arc<dyn FiberBody>, request.signal);
                    Ok(Arc::new(SpawnerHandle {
                        fiber: Arc::new(fiber),
                    }) as Arc<dyn EntryHandle>)
                }),
            },
        )
        .grab();
    (loader, observed, log)
}

#[tokio::test]
async fn teardown_spawned_provider_amendment_is_refused_never_deadlocked() {
    let (loader, observed, log) = spawner_fixture();
    loader
        .reconcile(profile(vec![
            entry("p", "test/provider", 1),
            entry("c", "test/spawner", 1),
        ]))
        .await
        .grab();
    let consumer_fiber = loader.entry_fiber(&id("c")).grab();
    assert_eq!(
        loader.fiber_state(consumer_fiber),
        Some(FiberState::Active),
        "the consumer leased and ran"
    );

    // Round 3 deadlocked here: the spawned task carried no teardown marker,
    // its admitted amendment awaited the provider's reload, the provider's
    // withdrawal awaited the consumer's lease, and the lease awaited this
    // very teardown. A timeout is the honest failure mode for a regression.
    tokio::time::timeout(
        Duration::from_secs(5),
        loader.dispose_entry::<u32>(&id("c")),
    )
    .await
    .grab()
    .grab();

    let observed = observed
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    assert_eq!(observed.len(), 1, "the teardown probed the provider once");
    assert!(
        observed[0].starts_with("provider: refused"),
        "a spawned amendment amid the withdrawal must be refused honestly, got: {}",
        observed[0]
    );

    // The refusal committed nothing: the provider was never restarted and
    // both views hold its original config; the disposal landed its own view.
    assert_eq!(activations(&log, "p"), 1, "the provider was never reloaded");
    let committed: Profile<u32> = loader.persisted().grab();
    let provider = committed
        .entries
        .iter()
        .find(|spec| spec.id == id("p"))
        .grab();
    assert_eq!(provider.config, 1);
    assert!(!provider.disabled);
    let consumer = committed
        .entries
        .iter()
        .find(|spec| spec.id == id("c"))
        .grab();
    assert!(consumer.disabled, "the disposal committed its own view");
    assert!(loader.entry_faults().is_empty());
}
