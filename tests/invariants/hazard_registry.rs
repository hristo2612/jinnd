mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use jinnd_api::{
    Activation, EntryId, ErrorCode, FiberState, Inject, Kernel, KernelError, KernelFuture,
    PluginContract, PluginRef, Profile, ProfileEntry, ServiceContract, ServiceHandle,
    ServiceResolver, ServiceType,
};
use support::{expect_ok, spec_case};

#[derive(Debug)]
struct VersionService(u32);

impl ServiceContract for VersionService {
    type Observation = u32;

    const NAME: &'static str = "jinn.test/hazard-version";

    fn observe(&self) -> u32 {
        self.0
    }
}

#[derive(Debug)]
struct NeedsVersion {
    service: ServiceHandle<VersionService>,
}

impl Inject for NeedsVersion {
    fn declare() -> Vec<ServiceType> {
        vec![ServiceType::of::<VersionService>()]
    }

    fn inject<R: ServiceResolver + ?Sized>(resolver: &R) -> Result<Self, KernelError> {
        Ok(Self {
            service: resolver.resolve::<VersionService>()?,
        })
    }
}

#[derive(Debug)]
struct FailingConsumer {
    attempts: Arc<AtomicUsize>,
    observations: Arc<Mutex<Vec<(u32, u64)>>>,
}

impl PluginContract for FailingConsumer {
    type Config = u32;
    type Dependencies = NeedsVersion;

    const NAME: &'static str = "jinn.test/failing-version-consumer";

    fn activate<'a>(
        &'a self,
        activation: Activation<'a, NeedsVersion>,
        _config: u32,
    ) -> KernelFuture<'a, ()> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let handle = &activation.dependencies.service;
        self.observations
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((handle.service.observe(), handle.generation.0));
        Box::pin(async {
            Err(KernelError {
                code: ErrorCode::PluginFailed,
                message: "fixture failure".to_owned(),
                fiber: None,
            })
        })
    }
}

fn entry(id: &str, package: &str, config: u32) -> ProfileEntry<u32> {
    ProfileEntry {
        id: EntryId(id.to_owned()),
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

fn observations(log: &Mutex<Vec<(u32, u64)>>) -> Vec<(u32, u64)> {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
}

spec_case! {
    /// Paper origin: Definition 23 O-Insert; occupied service slots refuse another provider.
    a_second_provider_for_an_occupied_slot_is_refused_without_replacement,
    origin: "paper: Definition 23 / O-Insert",
    test: "duplicate provide is refused without replacement",
    setup: ["two distinct provider entries target one service and root realm"],
    actions: ["reconcile both providers in document order", "wait for quiescence"],
    expected: ["first provider is active", "second provider fails cleanly", "the first value remains resolved"],
    body: |_case| {
        const PROVIDER: &str = "jinn.test/duplicate-provider";
        let kernel = jinnd_adapter::kernel();
        expect_ok(
            kernel.register_provider_package(PROVIDER, |value: u32| {
                Ok(Arc::new(VersionService(value)))
            }),
            "the provider package should register",
        );
        let report = expect_ok(
            kernel
                .reconcile(Profile {
                    entries: vec![entry("first", PROVIDER, 42), entry("second", PROVIDER, 7)],
                })
                .await,
            "the duplicate profile should reconcile with local failure",
        );
        assert!(report.errors.is_empty(), "runtime activation failure is fiber-local");
        expect_ok(
            kernel.wait_for_quiescence().await,
            "both provider attempts should settle",
        );
        let first = kernel
            .entry_fiber(&EntryId("first".to_owned()))
            .unwrap_or_else(|| panic!("first provider should have a fiber"));
        let second = kernel
            .entry_fiber(&EntryId("second".to_owned()))
            .unwrap_or_else(|| panic!("second provider should have a fiber"));
        assert_eq!(kernel.state(first), FiberState::Active);
        assert_eq!(kernel.state(second), FiberState::Failed);
        let resolved = expect_ok(
            kernel.resolve::<VersionService>(kernel.root_context()),
            "the occupied slot should still resolve",
        );
        assert_eq!(resolved.service.observe(), 42);
        assert_eq!(resolved.provider, first);
    }
}

spec_case! {
    /// Paper origin: L-Begin divergence; a changed dependency epoch deliberately re-arms failure once.
    a_failed_consumer_rearms_once_after_provider_generation_changes,
    origin: "paper: L-Begin divergence / R9 changed-environment re-arm",
    test: "failed fiber re-arms exactly once after a provider generation bump",
    setup: ["provider generation one is active", "consumer records one attempt then fails"],
    actions: ["update the provider to generation two", "wait for the dependency epoch to settle"],
    expected: ["consumer attempts exactly once more", "second attempt observes generation two", "consumer returns to failed without looping"],
    body: |_case| {
        const PROVIDER: &str = "jinn.test/rearm-provider";
        const CONSUMER: &str = "jinn.test/rearm-consumer";
        let kernel = jinnd_adapter::kernel();
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = Arc::new(Mutex::new(Vec::new()));
        expect_ok(
            kernel.register_provider_package(PROVIDER, |value: u32| {
                Ok(Arc::new(VersionService(value)))
            }),
            "the provider package should register",
        );
        let consumer_attempts = Arc::clone(&attempts);
        let consumer_observed = Arc::clone(&observed);
        expect_ok(
            kernel.register_package(CONSUMER, move |_config: u32| {
                Ok((
                    FailingConsumer {
                        attempts: Arc::clone(&consumer_attempts),
                        observations: Arc::clone(&consumer_observed),
                    },
                    0,
                ))
            }),
            "the failing consumer package should register",
        );
        expect_ok(
            kernel
                .reconcile(Profile {
                    entries: vec![entry("provider", PROVIDER, 1), entry("consumer", CONSUMER, 0)],
                })
                .await,
            "the initial generation should reconcile",
        );
        expect_ok(
            kernel.wait_for_quiescence().await,
            "the initial failure should settle",
        );
        let consumer = kernel
            .entry_fiber(&EntryId("consumer".to_owned()))
            .unwrap_or_else(|| panic!("consumer should have a fiber"));
        assert_eq!(kernel.state(consumer), FiberState::Failed);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        let first = observations(&observed);
        assert_eq!(first.len(), 1);

        expect_ok(
            kernel.update_entry(&EntryId("provider".to_owned()), 2_u32).await,
            "the provider generation should update",
        );
        expect_ok(
            kernel.wait_for_quiescence().await,
            "the changed dependency epoch should settle",
        );
        let final_observations = observations(&observed);
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(final_observations.len(), 2);
        assert_eq!(final_observations[0].0, 1);
        assert_eq!(final_observations[1].0, 2);
        assert!(final_observations[1].1 > final_observations[0].1);
        assert_eq!(kernel.state(consumer), FiberState::Failed);
    }
}
