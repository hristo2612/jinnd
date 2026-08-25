//! Loader wiring through the facade: reconcile-by-id, package lanes, entry
//! observation, and bidirectional persistence, driven exactly the way the
//! verifier-owned invariant suite drives the kernel (R1, R3, R5, R11).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use jinnd_adapter::kernel;
use jinnd_api::{
    Activation, EntryId, FiberState, Inject, IsolationBinding, Kernel, KernelFuture,
    PluginContract, PluginRef, Profile, ProfileEntry, Realm, ServiceContract, ServiceHandle,
    ServiceResolver, ServiceType,
};

#[derive(Debug)]
struct Beacon(u32);

impl ServiceContract for Beacon {
    type Observation = u32;

    const NAME: &'static str = "jinn.test/beacon";

    fn observe(&self) -> u32 {
        self.0
    }
}

/// A consumer plugin injecting the beacon.
#[derive(Debug)]
struct NeedsBeacon {
    beacon: ServiceHandle<Beacon>,
}

impl Inject for NeedsBeacon {
    fn declare() -> Vec<ServiceType> {
        vec![ServiceType::of::<Beacon>()]
    }

    fn inject<R: ServiceResolver + ?Sized>(resolver: &R) -> Result<Self, jinnd_api::KernelError> {
        Ok(Self {
            beacon: resolver.resolve::<Beacon>()?,
        })
    }
}

#[derive(Debug)]
struct Counting {
    counter: Arc<AtomicUsize>,
}

impl PluginContract for Counting {
    type Config = u32;
    type Dependencies = ();

    const NAME: &'static str = "jinn.test/counting";

    fn activate<'a>(
        &'a self,
        _activation: Activation<'a, ()>,
        _config: u32,
    ) -> KernelFuture<'a, ()> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

#[derive(Debug)]
struct Observing {
    seen: Arc<AtomicUsize>,
}

impl PluginContract for Observing {
    type Config = u32;
    type Dependencies = NeedsBeacon;

    const NAME: &'static str = "jinn.test/observing";

    fn activate<'a>(
        &'a self,
        activation: Activation<'a, NeedsBeacon>,
        _config: u32,
    ) -> KernelFuture<'a, ()> {
        let observed = activation.dependencies.beacon.service.observe();
        self.seen.store(observed as usize, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

fn entry(name: &str, package: &str, config: u32) -> ProfileEntry<u32> {
    ProfileEntry {
        id: EntryId(name.to_owned()),
        plugin: PluginRef {
            package: package.to_owned(),
            version: "1".to_owned(),
            artifact_hash: String::new(),
        },
        config,
        disabled: false,
        parent: None,
        isolation: Vec::new(),
    }
}

fn id(name: &str) -> EntryId {
    EntryId(name.to_owned())
}

#[tokio::test(flavor = "current_thread")]
async fn reconcile_by_id_through_the_facade_touches_only_affected_entries() {
    let kernel = kernel();
    let counter = Arc::new(AtomicUsize::new(0));
    let lane_counter = Arc::clone(&counter);
    kernel
        .register_package("jinn.test/counting", move |config: u32| {
            Ok((
                Counting {
                    counter: Arc::clone(&lane_counter),
                },
                config,
            ))
        })
        .grab();

    let mut qux = entry("qux", "jinn.test/counting", 4);
    qux.disabled = true;
    let report = kernel
        .reconcile(Profile {
            entries: vec![
                entry("foo", "jinn.test/counting", 1),
                entry("bar", "jinn.test/counting", 2),
                qux,
            ],
        })
        .await
        .grab();
    assert_eq!(report.created, vec![id("foo"), id("bar")]);
    assert_eq!(counter.load(Ordering::SeqCst), 2);
    let foo_fiber = kernel.entry_fiber(&id("foo")).grab();
    assert!(kernel.entry_fiber(&id("qux")).is_none());

    // Edit: bar leaves, qux enables; foo's fiber uid must not move.
    let report = kernel
        .reconcile(Profile {
            entries: vec![
                entry("foo", "jinn.test/counting", 1),
                entry("qux", "jinn.test/counting", 4),
            ],
        })
        .await
        .grab();
    assert_eq!(report.disposed, vec![id("bar")]);
    assert_eq!(report.created, vec![id("qux")]);
    assert_eq!(report.unchanged, vec![id("foo")]);
    assert_eq!(kernel.entry_fiber(&id("foo")), Some(foo_fiber));
    assert_eq!(counter.load(Ordering::SeqCst), 3);

    // The persisted document tracks the committed profile.
    let persisted = kernel.persisted_profile::<u32>().grab();
    assert_eq!(persisted.entries.len(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn runtime_updates_write_back_through_the_facade() {
    let kernel = kernel();
    let counter = Arc::new(AtomicUsize::new(0));
    let lane_counter = Arc::clone(&counter);
    kernel
        .register_package("jinn.test/counting", move |config: u32| {
            Ok((
                Counting {
                    counter: Arc::clone(&lane_counter),
                },
                config,
            ))
        })
        .grab();
    kernel
        .reconcile(Profile {
            entries: vec![
                entry("one", "jinn.test/counting", 1),
                entry("four", "jinn.test/counting", 4),
            ],
        })
        .await
        .grab();

    kernel.update_entry(&id("one"), 3u32).await.grab();
    let persisted = kernel.persisted_profile::<u32>().grab();
    let one = persisted.entries.iter().find(|e| e.id == id("one")).grab();
    assert_eq!(one.config, 3);
    assert_eq!(counter.load(Ordering::SeqCst), 3, "one reloaded once");

    kernel.dispose_entry::<u32>(&id("one")).await.grab();
    let persisted = kernel.persisted_profile::<u32>().grab();
    let one = persisted.entries.iter().find(|e| e.id == id("one")).grab();
    assert!(one.disabled);
    assert_eq!(one.config, 3);
    assert!(kernel.entry_fiber(&id("one")).is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn provider_and_consumer_entries_connect_and_isolate_through_realms() {
    let kernel = kernel();
    kernel
        .register_provider_package("jinn.test/beacon", |config: u32| {
            Ok(Arc::new(Beacon(config)))
        })
        .grab();
    let seen = Arc::new(AtomicUsize::new(0));
    let lane_seen = Arc::clone(&seen);
    kernel
        .register_package("jinn.test/observing", move |config: u32| {
            Ok((
                Observing {
                    seen: Arc::clone(&lane_seen),
                },
                config,
            ))
        })
        .grab();

    // Root provider and consumer connect.
    kernel
        .reconcile(Profile {
            entries: vec![
                entry("beacon", "jinn.test/beacon", 7),
                entry("watcher", "jinn.test/observing", 0),
            ],
        })
        .await
        .grab();
    kernel.wait_for_quiescence().await.grab();
    let watcher = kernel.entry_fiber(&id("watcher")).grab();
    assert_eq!(kernel.state(watcher), FiberState::Active);
    assert_eq!(seen.load(Ordering::SeqCst), 7);

    // Isolating the consumer's beacon into its own realm unloads it cleanly.
    let mut isolated = entry("watcher", "jinn.test/observing", 0);
    isolated.isolation.push(IsolationBinding {
        service: Beacon::NAME.to_owned(),
        realm: Realm::Local(id("watcher")),
    });
    kernel
        .reconcile(Profile {
            entries: vec![entry("beacon", "jinn.test/beacon", 7), isolated],
        })
        .await
        .grab();
    kernel.wait_for_quiescence().await.grab();
    assert_eq!(
        kernel.entry_fiber(&id("watcher")),
        Some(watcher),
        "same fiber"
    );
    assert_eq!(kernel.state(watcher), FiberState::Pending);
}

/// Repo convention: no `unwrap`/`expect`. `grab` is the tests' one panicking
/// accessor, carrying the caller's location.
pub trait Grab<T> {
    fn grab(self) -> T;
}

impl<T, E: std::fmt::Debug> Grab<T> for Result<T, E> {
    #[track_caller]
    fn grab(self) -> T {
        match self {
            Ok(value) => value,
            Err(error) => panic!("unexpected error: {error:?}"),
        }
    }
}

impl<T> Grab<T> for Option<T> {
    #[track_caller]
    fn grab(self) -> T {
        match self {
            Some(value) => value,
            None => panic!("unexpectedly empty"),
        }
    }
}
