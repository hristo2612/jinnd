mod loader_fixture;
mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use jinnd_api::{
    Activation, FiberState, Inject, Kernel, KernelFuture, PluginContract, PluginRef, Profile,
    ProfileEntry, Realm, ServiceContract, ServiceHandle, ServiceResolver, ServiceType, Undo,
};
use loader_fixture::{CONSUMER, Config, PROVIDER, SERVICE};
use support::{expect_ok, spec_case};

const SUBSYSTEM: support::Subsystem = support::Subsystem::Fiber;
const FACADE_GAP_REASON: &str =
    "the facade cannot declare dependencies or invoke a dying service from consumer teardown";

#[derive(Debug)]
struct StableService(u32);

impl ServiceContract for StableService {
    type Observation = u32;

    const NAME: &'static str = "jinn.test/stable-service";

    fn observe(&self) -> u32 {
        self.0
    }
}

#[derive(Debug)]
struct NeedsStable {
    service: ServiceHandle<StableService>,
}

impl Inject for NeedsStable {
    fn declare() -> Vec<ServiceType> {
        vec![ServiceType::of::<StableService>()]
    }

    fn inject<R: ServiceResolver + ?Sized>(resolver: &R) -> Result<Self, jinnd_api::KernelError> {
        Ok(Self {
            service: resolver.resolve::<StableService>()?,
        })
    }
}

struct ObserveUndo(Arc<StableService>, Arc<Mutex<Vec<u32>>>);

impl Undo for ObserveUndo {
    fn undo(self: Box<Self>) -> KernelFuture<'static, ()> {
        let observed = self.0.observe();
        self.1
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(observed);
        Box::pin(async { Ok(()) })
    }
}

#[derive(Debug)]
struct StableConsumer(Arc<Mutex<Vec<u32>>>);

impl PluginContract for StableConsumer {
    type Config = u32;
    type Dependencies = NeedsStable;

    const NAME: &'static str = "jinn.test/stable-consumer";

    fn activate<'a>(
        &'a self,
        activation: Activation<'a, NeedsStable>,
        _config: u32,
    ) -> KernelFuture<'a, ()> {
        let service = Arc::clone(&activation.dependencies.service.service);
        let observations = Arc::clone(&self.0);
        Box::pin(async move {
            activation.effects.register(
                "observe dying provider".to_owned(),
                Box::new(ObserveUndo(service, observations)),
            )?;
            Ok(())
        })
    }
}

fn stable_entry(id: &str, package: &str, config: u32) -> ProfileEntry<u32> {
    ProfileEntry {
        id: jinnd_api::EntryId(id.to_owned()),
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

spec_case! {
    /// Paper origin: ordering and resolution-coherence theorem; SOURCE-OF-TRUTH §4 invariant I2.
    consumer_can_call_dying_provider_during_its_teardown,
    origin: "paper: ordering and resolution-coherence theorem / I2",
    test: "consumer may call dying provider during teardown",
    setup: ["active provider and consumer with one owned dependency snapshot", "consumer undo calls provider method"],
    actions: ["dispose provider and wait for quiescence"],
    expected: ["provider slot stops accepting new resolutions before teardown", "existing consumer handle remains callable during consumer undo", "consumer finishes before provider value disappears"]
}

spec_case! {
    /// Paper origin: Theorem 63(3), provider value stability during dependent teardown.
    provider_observation_is_stable_until_consumer_teardown_finishes,
    origin: "paper: Theorem 63(3) / I2 value stability",
    test: "dying provider remains observationally equal through consumer teardown",
    setup: ["active provider exposes an observable value", "consumer owns one dependency snapshot"],
    actions: ["capture the provider observation", "withdraw the provider and observe it from consumer teardown"],
    expected: ["the teardown observation equals the pre-withdrawal observation", "provider effects begin withdrawing only after dependents finish"],
    body: |_case| {
        const STABLE_PROVIDER: &str = "jinn.test/stable-provider-package";
        const STABLE_CONSUMER: &str = "jinn.test/stable-consumer-package";

        let kernel = jinnd_adapter::kernel();
        let observations = Arc::new(Mutex::new(Vec::new()));
        expect_ok(
            kernel.register_provider_package(STABLE_PROVIDER, |config: u32| {
                Ok(Arc::new(StableService(config)))
            }),
            "provider lane should register",
        );
        let consumer_observations = Arc::clone(&observations);
        expect_ok(
            kernel.register_package(STABLE_CONSUMER, move |config: u32| {
                Ok((StableConsumer(Arc::clone(&consumer_observations)), config))
            }),
            "consumer lane should register",
        );
        let report = expect_ok(
            kernel
                .reconcile(Profile {
                    entries: vec![
                        stable_entry("provider", STABLE_PROVIDER, 42),
                        stable_entry("consumer", STABLE_CONSUMER, 0),
                    ],
                })
                .await,
            "profile should reconcile",
        );
        assert!(report.errors.is_empty());
        expect_ok(kernel.wait_for_quiescence().await, "profile should quiesce");
        let before = expect_ok(
            kernel.resolve::<StableService>(kernel.root_context()),
            "the active provider should resolve",
        )
        .service
        .observe();
        assert_eq!(before, 42);
        expect_ok(
            kernel.dispose_entry::<u32>(&jinnd_api::EntryId("provider".to_owned())).await,
            "provider should withdraw",
        );
        assert_eq!(
            observations
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .as_slice(),
            [before],
            "consumer teardown must observe the provider's pre-withdrawal value",
        );
    }
}

spec_case! {
    /// Paper origin: ordering and resolution-coherence theorem; SOURCE-OF-TRUTH §4 invariant I2.
    one_activation_never_observes_mixed_provider_generations,
    origin: "paper: ordering and resolution-coherence theorem / I2",
    test: "one resolution per transition",
    setup: ["consumer owns two calls through one provider dependency snapshot"],
    actions: ["hot-swap provider between the two scheduled calls"],
    expected: ["first activation sees one generation only", "consumer fully unloads before a new activation captures replacement generation"]
}

spec_case! {
    /// Paper origin: Definition 29 realm resolution and Theorem 63(3) withdrawal ordering.
    a_different_realm_consumer_does_not_delay_provider_withdrawal,
    origin: "paper: Definition 29 / Theorem 63(3)",
    test: "cross-realm consumers do not block withdrawal",
    setup: ["provider is active in the root realm", "consumer injects the same service name from a distinct shared realm"],
    actions: ["withdraw the root-realm provider with a bounded deadline"],
    expected: ["the unrelated consumer stays pending", "provider withdrawal completes without waiting on another realm"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let log = loader_fixture::log();
        loader_fixture::register(&kernel, &log);
        let consumer = loader_fixture::isolated(
            loader_fixture::entry("other-realm-consumer", CONSUMER, 0),
            SERVICE,
            Realm::Shared("other".to_owned()),
        );
        loader_fixture::reconcile(
            &kernel,
            vec![loader_fixture::entry("provider", PROVIDER, 7), consumer],
        )
        .await;
        let provider = loader_fixture::fiber(&kernel, "provider");
        assert_eq!(
            loader_fixture::state(&kernel, "other-realm-consumer"),
            Some(FiberState::Pending)
        );

        expect_ok(
            tokio::time::timeout(
                Duration::from_secs(2),
                kernel.dispose_entry::<Config>(&loader_fixture::id("provider")),
            )
            .await,
            "another realm must not hold the provider drain open",
        )
        .unwrap_or_else(|error| panic!("provider withdrawal should succeed: {error:?}"));

        assert_eq!(kernel.state(provider), FiberState::Disposed);
        assert_eq!(
            loader_fixture::state(&kernel, "other-realm-consumer"),
            Some(FiberState::Pending)
        );
    }
}
