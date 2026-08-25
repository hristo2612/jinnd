//! M1-P6c scope 1 (I2 drain ordering, paper Alg 5): a dying provider's
//! dependents drain BEFORE any of the provider's inverses run, so a consumer
//! teardown that calls the dying service still observes the provider's
//! post-provision effects intact — value-stability, not just value-liveness.

#![cfg(not(feature = "loom"))]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use common::{Grab, entry, id, plain_spawn, profile};
use jinnd_api::{KernelFuture, Realm, ServiceContract, ServiceType};
use jinnd_context::ContextTree;
use jinnd_effects::Disposer;
use jinnd_fiber::{FiberBody, Setup};
use jinnd_loader::{Loader, PackageLane, SpawnRequest};
use jinnd_registry::Registry;

/// A service whose observation reads shared state the provider's
/// post-provision effect withdraws: observing it tells apart "provider still
/// whole" (I2) from "provider partially withdrawn" (the audited defect).
#[derive(Debug)]
struct Stateful {
    state: Arc<AtomicU32>,
}

impl ServiceContract for Stateful {
    type Observation = u32;

    const NAME: &'static str = "svc.stateful";

    fn observe(&self) -> u32 {
        self.state.load(Ordering::SeqCst)
    }
}

/// Provider body: provision first, then a post-provision effect whose undo
/// zeroes the observed state — the paper's "provider effect registered after
/// provision".
struct ProviderBody {
    registry: Registry,
    root: jinnd_context::Context<()>,
    state: Arc<AtomicU32>,
}

impl FiberBody for ProviderBody {
    fn activate<'a>(&'a self, mut setup: Setup<'a>) -> KernelFuture<'a, ()> {
        let fiber = setup.fiber();
        Box::pin(async move {
            let provision = self.registry.provide::<Stateful, ()>(
                &self.root,
                &Realm::Root,
                fiber,
                Arc::new(Stateful {
                    state: Arc::clone(&self.state),
                }),
                &self.registry.vitality(true),
            )?;
            setup.draining_effect("provide svc.stateful", provision.drain, provision.undo)?;
            let state = Arc::clone(&self.state);
            setup.effect(
                "post-provision contribution",
                Disposer::sync(move || {
                    state.store(0, Ordering::SeqCst);
                    Ok(())
                }),
            )?;
            Ok(())
        })
    }
}

/// Consumer body: leases the service and, at teardown, calls the dying
/// service — exactly what I2 entitles it to — recording what it observed.
struct ConsumerBody {
    registry: Registry,
    root: jinnd_context::Context<()>,
    observed: Arc<AtomicU32>,
}

impl FiberBody for ConsumerBody {
    fn activate<'a>(&'a self, mut setup: Setup<'a>) -> KernelFuture<'a, ()> {
        Box::pin(async move {
            let (handle, guard) = self.registry.lease::<Stateful, ()>(&self.root)?;
            let observed = Arc::clone(&self.observed);
            setup.effect(
                "observe the dying service at teardown",
                Disposer::sync(move || {
                    observed.store(handle.service.observe(), Ordering::SeqCst);
                    drop(guard);
                    Ok(())
                }),
            )?;
            Ok(())
        })
    }
}

/// A loader with one stateful provider package and one consumer package.
fn stateful_fixture() -> (Loader, Arc<AtomicU32>, Arc<AtomicU32>) {
    let tree: ContextTree = ContextTree::new();
    let root = tree.root();
    let registry = Registry::new();
    let loader = Loader::new(root.clone(), registry.clone(), |_context| {});
    let state = Arc::new(AtomicU32::new(42));
    let observed = Arc::new(AtomicU32::new(u32::MAX));
    let service = ServiceType::of::<Stateful>();

    let provider_registry = registry.clone();
    let provider_root = root.clone();
    let provider_state = Arc::clone(&state);
    loader
        .register_lane::<u32>(
            "test/stateful-provider",
            PackageLane {
                injects: Vec::new(),
                provides: Some(service),
                spawn: Box::new(move |request: SpawnRequest<'_>| {
                    Ok(plain_spawn(
                        Arc::new(ProviderBody {
                            registry: provider_registry.clone(),
                            root: provider_root.clone(),
                            state: Arc::clone(&provider_state),
                        }),
                        request.signal,
                    ))
                }),
            },
        )
        .grab();

    let consumer_registry = registry.clone();
    let consumer_observed = Arc::clone(&observed);
    loader
        .register_lane::<u32>(
            "test/stateful-consumer",
            PackageLane {
                injects: vec![service],
                provides: None,
                spawn: Box::new(move |request: SpawnRequest<'_>| {
                    Ok(plain_spawn(
                        Arc::new(ConsumerBody {
                            registry: consumer_registry.clone(),
                            root: root.clone(),
                            observed: Arc::clone(&consumer_observed),
                        }),
                        request.signal,
                    ))
                }),
            },
        )
        .grab();
    (loader, state, observed)
}

#[tokio::test]
async fn consumer_teardown_observes_the_dying_providers_post_provision_effects() {
    let (loader, _state, observed) = stateful_fixture();
    loader
        .reconcile(profile(vec![
            entry("prov", "test/stateful-provider", 1),
            entry("cons", "test/stateful-consumer", 1),
        ]))
        .await
        .grab();

    // The provider's withdrawal must drain the consumer before ANY provider
    // inverse runs (I2, paper Alg 5). A timeout is the honest regression mode.
    tokio::time::timeout(
        Duration::from_secs(5),
        loader.dispose_entry::<u32>(&id("prov")),
    )
    .await
    .grab()
    .grab();

    assert_eq!(
        observed.load(Ordering::SeqCst),
        42,
        "the consumer's teardown called the dying service and must observe the \
         provider whole — post-provision effects withdrawn only after the drain (I2)"
    );
}
