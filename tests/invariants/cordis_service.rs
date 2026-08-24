mod support;

use support::spec_case;

spec_case! {
    /// TS origin: `packages/core/tests/service.spec.ts`, test `pending inject`.
    injection_waits_for_provider_initialization,
    origin: "packages/core/tests/service.spec.ts",
    test: "pending inject",
    setup: ["consumer requires a service", "provider initialization waits on an event"],
    actions: ["start provider", "emit initialization event"],
    expected: ["consumer remains pending during initialization", "consumer activates exactly once after provider is active"]
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
    expected: ["removal restores exact pre-activation observation", "reactivation matches original active observation"]
}

spec_case! {
    /// TS origin: `packages/core/tests/service.spec.ts`, test `multiple injects`.
    dependency_chain_activates_each_provider_once,
    origin: "packages/core/tests/service.spec.ts",
    test: "multiple injects",
    setup: ["foo requires qux", "bar requires foo and qux", "all begin pending"],
    actions: ["provide qux and wait for quiescence"],
    expected: ["foo, bar, and qux each initialize exactly once", "all end active"]
}
