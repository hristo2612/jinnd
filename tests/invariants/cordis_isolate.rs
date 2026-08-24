mod support;

use support::spec_case;

spec_case! {
    /// TS origin: `packages/core/tests/isolate.spec.ts`, test `isolated context`.
    isolated_contexts_resolve_independent_service_slots,
    origin: "packages/core/tests/isolate.spec.ts",
    test: "isolated context",
    setup: ["root and two child contexts inject one typed service in distinct local realms"],
    actions: ["provide and withdraw root, child-one, and child-two generations"],
    expected: ["each consumer activates only for its own realm", "withdrawal unloads only the matching consumer"]
}

spec_case! {
    /// TS origin: `packages/core/tests/isolate.spec.ts`, test `shared label`.
    shared_realm_label_connects_separate_derived_contexts,
    origin: "packages/core/tests/isolate.spec.ts",
    test: "shared label",
    setup: ["two child contexts map a service to the same shared realm"],
    actions: ["provide and withdraw a value through the first child"],
    expected: ["both children resolve the same generation", "both consumers activate and unload together", "root realm remains independent"]
}

spec_case! {
    /// TS origin: `packages/core/tests/isolate.spec.ts`, test `isolated event`.
    event_payload_filter_routes_to_matching_isolated_context,
    origin: "packages/core/tests/isolate.spec.ts",
    test: "isolated event",
    setup: ["root and isolated child register listeners", "service is provided inside isolated child"],
    actions: ["service emits a typed payload scoped to its caller context"],
    expected: ["child listener receives one event", "root listener receives none"]
}
