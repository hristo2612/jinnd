mod loader_cases;
mod loader_fixture;
mod support;

use support::spec_case;

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: basic / initiate`.
    loader_isolation_fixture_initially_connects_root_provider_and_consumer,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: basic / initiate",
    setup: ["root-realm provider bar and consumer foo requiring bar"],
    actions: ["create both entries and settle"],
    expected: ["foo activates once", "no disposal occurs"],
    body: |_case| { loader_cases::isolate_basic::run(0).await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: basic / add isolate on injector (relevant)`.
    adding_relevant_injector_isolation_unloads_consumer,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: basic / add isolate on injector (relevant)",
    setup: ["root provider and consumer are active"],
    actions: ["map consumer dependency bar to a new local realm"],
    expected: ["consumer unloads once and becomes pending", "provider stays active"],
    body: |_case| { loader_cases::isolate_basic::run(1).await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: basic / add isolate on injector (irrelevant)`.
    adding_irrelevant_injector_isolation_does_not_restart,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: basic / add isolate on injector (irrelevant)",
    setup: ["consumer is already pending because bar is isolated"],
    actions: ["also isolate unrelated qux"],
    expected: ["no activation or disposal occurs"],
    body: |_case| { loader_cases::isolate_basic::run(2).await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: basic / remove isolate on injector (relevant)`.
    removing_relevant_injector_isolation_reactivates_consumer,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: basic / remove isolate on injector (relevant)",
    setup: ["consumer is pending in isolated bar realm while root provider is active"],
    actions: ["remove bar mapping but retain unrelated qux mapping"],
    expected: ["consumer activates once against root bar generation"],
    body: |_case| { loader_cases::isolate_basic::run(3).await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: basic / remove isolate on injector (irrelevant)`.
    removing_last_irrelevant_injector_isolation_does_not_restart,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: basic / remove isolate on injector (irrelevant)",
    setup: ["consumer is active and only unrelated qux remains isolated"],
    actions: ["remove qux isolation"],
    expected: ["consumer neither activates nor disposes"],
    body: |_case| { loader_cases::isolate_basic::run(4).await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: basic / add isolate on provider (relevant)`.
    adding_relevant_provider_isolation_unloads_root_consumer,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: basic / add isolate on provider (relevant)",
    setup: ["root provider and consumer are active"],
    actions: ["move provider bar to a local realm"],
    expected: ["root consumer unloads once and becomes pending"],
    body: |_case| { loader_cases::isolate_basic::run(5).await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: basic / add isolate on provider (irrelevant)`.
    adding_irrelevant_provider_isolation_does_not_restart,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: basic / add isolate on provider (irrelevant)",
    setup: ["provider bar is already local and root consumer is pending"],
    actions: ["also isolate unrelated qux on provider"],
    expected: ["no activation or disposal occurs"],
    body: |_case| { loader_cases::isolate_basic::run(6).await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: basic / remove isolate on provider (relevant)`.
    removing_relevant_provider_isolation_reactivates_root_consumer,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: basic / remove isolate on provider (relevant)",
    setup: ["provider is local while root consumer is pending"],
    actions: ["return provider bar to root realm while unrelated qux remains local"],
    expected: ["consumer activates once"],
    body: |_case| { loader_cases::isolate_basic::run(7).await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: basic / remove isolate on provider (irrelevant)`.
    removing_last_irrelevant_provider_isolation_does_not_restart,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: basic / remove isolate on provider (irrelevant)",
    setup: ["provider and consumer are active in root realm"],
    actions: ["remove unrelated qux isolation"],
    expected: ["consumer neither activates nor disposes"],
    body: |_case| { loader_cases::isolate_basic::run(8).await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: realm / add isolate group`.
    isolated_groups_create_distinct_provider_realms,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: realm / add isolate group",
    setup: ["alpha local realm and beta shared realm each contain provider bar"],
    actions: ["create both groups and settle"],
    expected: ["two provider fibers remain independently registered", "no unrelated consumer activates"],
    body: |_case| { loader_cases::isolate_realms::partitioned_providers().await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: realm / update isolate group (no change)`.
    semantically_identical_realm_update_is_inert,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: realm / update isolate group (no change)",
    setup: ["alpha group already maps bar to its local realm"],
    actions: ["write the same isolation mapping again"],
    expected: ["provider generations are unchanged", "no consumer activation or disposal occurs"],
    body: |_case| { loader_cases::isolate_realms::identical_realm_is_inert().await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: realm / realm reference`.
    consumer_can_select_inherited_shared_or_fresh_local_realm,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: realm / realm reference",
    setup: ["alpha and beta each provide bar values in distinct realms"],
    actions: ["create inherited-alpha, explicit-beta, and fresh-local consumers under alpha"],
    expected: ["first observes alpha and is active", "second observes beta and is active", "third has no provider and is pending"],
    body: |_case| { loader_cases::isolate_realms::consumer_selects_realms().await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: realm / special case: nested realms`.
    redundant_ancestor_realm_edits_do_not_restart_nested_consumers,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: realm / special case: nested realms",
    setup: ["nested inner maps bar to shared custom realm with provider", "two consumers already resolve custom"],
    actions: ["add then remove redundant custom mapping on outer ancestor"],
    expected: ["both consumers retain the same provider generation", "no activation or disposal occurs"],
    body: |_case| { loader_cases::isolate_realms::redundant_ancestor_is_inert().await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: realm / special case: change provider`.
    changing_group_realm_switches_consumer_provider_generation,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: realm / special case: change provider",
    setup: ["alpha and beta provider realms", "group consumer currently resolves alpha"],
    actions: ["change group mapping from alpha to beta"],
    expected: ["consumer unloads once and reloads once", "new activation observes beta"],
    body: |_case| { loader_cases::isolate_realms::changing_group_switches_provider().await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: realm / special case: change injector`.
    moving_provider_realm_retargets_only_matching_external_consumer,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: realm / special case: change injector",
    setup: ["external alpha and beta consumers", "nested provider currently maps to alpha"],
    actions: ["change provider group mapping from alpha to beta"],
    expected: ["alpha consumer unloads once", "beta consumer activates once", "availability swaps without cross-realm leakage"],
    body: |_case| { loader_cases::isolate_realms::moving_provider_retargets_external_consumers().await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: transfer / initiate`.
    transfer_fixture_initially_connects_root_entries,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: transfer / initiate",
    setup: ["empty group isolates bar", "provider and consumer begin at root"],
    actions: ["settle initial profile"],
    expected: ["consumer activates once against root provider"],
    body: |_case| { loader_cases::isolate_transfer::run(0).await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: transfer / transfer injector into group`.
    moving_consumer_into_isolated_group_unloads_it,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: transfer / transfer injector into group",
    setup: ["root provider and consumer are active", "target group has empty isolated bar realm"],
    actions: ["move consumer into group"],
    expected: ["consumer unloads once and becomes pending"],
    body: |_case| { loader_cases::isolate_transfer::run(1).await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: transfer / transfer provider into group`.
    moving_provider_into_same_isolated_group_reactivates_consumer,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: transfer / transfer provider into group",
    setup: ["consumer is pending inside isolated group", "provider remains at root"],
    actions: ["move provider into group"],
    expected: ["consumer activates once against the group's provider generation"],
    body: |_case| { loader_cases::isolate_transfer::run(2).await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: transfer / transfer injector out of group`.
    moving_consumer_out_of_group_unloads_it_when_provider_stays_inside,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: transfer / transfer injector out of group",
    setup: ["provider and consumer are active inside isolated group"],
    actions: ["move consumer to root"],
    expected: ["consumer unloads once and becomes pending"],
    body: |_case| { loader_cases::isolate_transfer::run(3).await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/isolate.spec.ts`, test `Service Isolation: transfer / transfer provider out of group`.
    moving_provider_out_to_consumer_reactivates_it,
    origin: "packages/loader/tests/isolate.spec.ts",
    test: "Service Isolation: transfer / transfer provider out of group",
    setup: ["consumer is pending at root", "provider remains inside isolated group"],
    actions: ["move provider to root"],
    expected: ["consumer activates once against root provider generation"],
    body: |_case| { loader_cases::isolate_transfer::run(4).await; }
}
