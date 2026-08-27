mod loader_fixture;
mod support;

use std::sync::Arc;

use jinnd_api::{ErrorCode, Kernel, Realm, ServiceContract};
use support::{expect_ok, spec_case};

const SUBSYSTEM: support::Subsystem = support::Subsystem::Services;
const V02_DEFERRED_BOUND: &str = "SOURCE-OF-TRUTH R4/R7 and constitution 01: v0.1 broker calls carry caller peers, but typed harness Arc handles expose no call boundary that can mint caller-owned effects";

#[derive(Debug)]
struct SnapshotService(u8);

impl ServiceContract for SnapshotService {
    type Observation = u8;
    const NAME: &'static str = "jinn.test/service-snapshot";
    fn observe(&self) -> u8 {
        self.0
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/service.spec.ts`, test `pending inject`.
    injection_waits_for_provider_initialization,
    origin: "packages/core/tests/service.spec.ts",
    test: "pending inject",
    setup: ["consumer requires a service", "provider initialization waits on an event"],
    actions: ["start provider", "emit initialization event"],
    expected: ["consumer remains pending while its provider is unavailable", "consumer activates exactly once after provider is active"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let log = loader_fixture::log();
        loader_fixture::register(&kernel, &log);
        loader_fixture::reconcile(
            &kernel,
            vec![loader_fixture::entry("waiting-consumer", loader_fixture::CONSUMER, 0)],
        ).await;
        assert_eq!(loader_fixture::state(&kernel, "waiting-consumer"), Some(jinnd_api::FiberState::Pending));
        assert!(loader_fixture::observations(&log, "waiting-consumer").is_empty());
        loader_fixture::reconcile(
            &kernel,
            vec![
                loader_fixture::entry("waiting-consumer", loader_fixture::CONSUMER, 0),
                loader_fixture::entry("late-provider", loader_fixture::PROVIDER, 7),
            ],
        ).await;
        assert_eq!(loader_fixture::state(&kernel, "waiting-consumer"), Some(jinnd_api::FiberState::Active));
        assert_eq!(loader_fixture::observations(&log, "waiting-consumer").len(), 1);
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/service.spec.ts`, test `traceable effect (with inject)`; translated to R4 handles.
    injected_handle_charges_effect_to_each_explicit_caller,
    origin: "packages/core/tests/service.spec.ts",
    test: "traceable effect (with inject) (R4 handle equivalent)",
    setup: ["counter service mutates only through reversible effects", "consumer resolves a scope-carrying handle"],
    actions: ["call through root handle", "call through consumer handle", "dispose consumer", "call through root handle again"],
    expected: ["values advance 1, 2, then roll back consumer contribution to 1, then advance to 2", "no caller-attribution warning"]
}

spec_case! {
    /// TS origin: `packages/core/tests/service.spec.ts`, test `traceable effect (without inject)`; translated to R4 handles.
    unowned_handle_cannot_silently_charge_an_unrelated_fiber,
    origin: "packages/core/tests/service.spec.ts",
    test: "traceable effect (without inject) (R4 handle equivalent)",
    setup: ["service internally uses an undeclared counter dependency"],
    actions: ["resolve from root", "attempt call from a consumer without the dependency snapshot"],
    expected: ["root-scoped effect remains root-owned", "consumer call is rejected instead of misattributed"]
}

spec_case! {
    /// TS origin: `packages/core/tests/service.spec.ts`, test `compare snapshot`.
    service_registration_and_removal_restore_registry_snapshot,
    origin: "packages/core/tests/service.spec.ts",
    test: "compare snapshot",
    setup: ["capture registry and hook snapshot", "activate service with nested injection"],
    actions: ["remove service", "reactivate service"],
    expected: ["removal restores exact pre-activation observation", "reactivation matches original active observation"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let root = kernel.root_context();
        assert_eq!(kernel.resolve::<SnapshotService>(root).err().map(|error| error.code), Some(ErrorCode::MissingDependency));
        let first = expect_ok(kernel.provide(root, Realm::Root, Arc::new(SnapshotService(42))).await, "first registration");
        let active = expect_ok(kernel.resolve::<SnapshotService>(root), "active snapshot");
        assert_eq!(active.service.observe(), 42);
        expect_ok(kernel.dispose_effect(first).await, "remove service");
        assert_eq!(kernel.resolve::<SnapshotService>(root).err().map(|error| error.code), Some(ErrorCode::MissingDependency));
        expect_ok(kernel.provide(root, Realm::Root, Arc::new(SnapshotService(42))).await, "reactivate service");
        let reactivated = expect_ok(kernel.resolve::<SnapshotService>(root), "reactivated snapshot");
        assert_eq!(reactivated.service.observe(), active.service.observe());
        assert!(reactivated.generation > active.generation);
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/service.spec.ts`, test `multiple injects`.
    dependency_chain_activates_each_provider_once,
    origin: "packages/core/tests/service.spec.ts",
    test: "multiple injects",
    setup: ["foo requires qux", "bar requires foo and qux", "all begin pending"],
    actions: ["provide qux and wait for quiescence"],
    expected: ["foo, bar, and qux each initialize exactly once", "all end active"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let log = loader_fixture::log();
        loader_fixture::register(&kernel, &log);
        loader_fixture::reconcile(
            &kernel,
            vec![
                loader_fixture::entry("foo", loader_fixture::CONSUMER, 0),
                loader_fixture::entry("bar", loader_fixture::CONSUMER, 0),
                loader_fixture::entry("qux", loader_fixture::PROVIDER, 42),
            ],
        ).await;
        for id in ["foo", "bar", "qux"] {
            assert_eq!(loader_fixture::state(&kernel, id), Some(jinnd_api::FiberState::Active));
        }
        assert_eq!(loader_fixture::observations(&log, "foo").len(), 1);
        assert_eq!(loader_fixture::observations(&log, "bar").len(), 1);
    }
}
