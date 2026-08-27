mod loader_fixture;
mod support;

use std::time::Duration;

use jinnd_api::{FiberState, Kernel, Realm, ServiceContract};
use loader_fixture::{CONSUMER, Config, PROVIDER, SERVICE};
use support::{expect_ok, facade_gap_at, spec_case};

const SUBSYSTEM: support::Subsystem = support::Subsystem::Fiber;
const FACADE_GAP_REASON: &str =
    "the facade cannot declare dependencies or invoke a dying service from consumer teardown";

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
    body: |case| {
        let kernel = jinnd_adapter::kernel();
        let log = loader_fixture::log();
        loader_fixture::register(&kernel, &log);
        loader_fixture::reconcile(
            &kernel,
            vec![
                loader_fixture::entry("provider", PROVIDER, 42),
                loader_fixture::entry("consumer", CONSUMER, 0),
            ],
        )
        .await;
        let before = expect_ok(
            kernel.resolve::<loader_fixture::FixtureService>(kernel.root_context()),
            "the active provider should resolve",
        )
        .service
        .observe();
        assert_eq!(before, 42);

        facade_gap_at(
            &case,
            "the facade has no consumer teardown hook that can observe its dying service handle",
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
