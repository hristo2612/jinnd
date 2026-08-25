//! Dependency-bearing plugins through the facade: declared injection, registry
//! readiness, and leased per-activation snapshots (M1-P4 round 2).

use std::sync::{Arc, Mutex};

use jinnd_api::{
    Activation, FiberState, Inject, Kernel, KernelError, KernelFuture, PluginContract, Realm,
    ServiceContract, ServiceHandle, ServiceResolver, ServiceType,
};

#[derive(Debug)]
struct Beacon(u8);

impl ServiceContract for Beacon {
    type Observation = u8;

    const NAME: &'static str = "jinn.test/beacon";

    fn observe(&self) -> u8 {
        self.0
    }
}

/// What [`Follower`] declares it injects: one beacon, resolved per activation.
#[derive(Debug)]
struct BeaconDep {
    beacon: ServiceHandle<Beacon>,
}

impl Inject for BeaconDep {
    fn declare() -> Vec<ServiceType> {
        vec![ServiceType::of::<Beacon>()]
    }

    fn inject<R: ServiceResolver + ?Sized>(resolver: &R) -> Result<Self, KernelError> {
        Ok(Self {
            beacon: resolver.resolve::<Beacon>()?,
        })
    }
}

/// A dependency-bearing plugin: records the beacon value each activation saw.
#[derive(Clone, Debug)]
struct Follower {
    seen: Arc<Mutex<Vec<u8>>>,
}

impl PluginContract for Follower {
    type Config = ();
    type Dependencies = BeaconDep;

    const NAME: &'static str = "jinn.test/follower";

    fn activate<'a>(
        &'a self,
        activation: Activation<'a, BeaconDep>,
        (): (),
    ) -> KernelFuture<'a, ()> {
        self.seen
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(activation.dependencies.beacon.service.observe());
        Box::pin(async { Ok(()) })
    }
}

/// Lets the availability watcher process the store edge it was just handed.
async fn breathe() {
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn an_injected_plugin_waits_pending_and_activates_once_its_provider_lands() {
    let kernel = jinnd_adapter::kernel();
    let root = kernel.root_context();
    let plugin = Follower {
        seen: Arc::new(Mutex::new(Vec::new())),
    };
    let seen = Arc::clone(&plugin.seen);

    let Ok(fiber) = kernel.spawn(root, plugin, ()).await else {
        panic!("a declared-dependency plugin must spawn");
    };
    assert_eq!(
        kernel.state(fiber),
        FiberState::Pending,
        "the consumer waits for its provider instead of failing (§3)"
    );

    let Ok(_effect) = kernel.provide(root, Realm::Root, Arc::new(Beacon(9))).await else {
        panic!("the provision must install");
    };
    breathe().await;
    let Ok(()) = kernel.wait_for_quiescence().await else {
        panic!("quiescence must be reachable");
    };
    assert_eq!(kernel.state(fiber), FiberState::Active);
    assert_eq!(
        *seen.lock().unwrap_or_else(|poison| poison.into_inner()),
        vec![9],
        "the consumer activated exactly once, seeing the injected provider (R4)"
    );

    let labels: Vec<String> = kernel
        .effect_tree(fiber)
        .iter()
        .map(|descriptor| descriptor.label.clone())
        .collect();
    assert!(
        labels.iter().any(|label| label.contains("lease")),
        "the injected leases are a labelled effect on the consumer (R5): {labels:?}"
    );
}
