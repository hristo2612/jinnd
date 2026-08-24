mod support;

use support::spec_case;

const SUBSYSTEM: support::Subsystem = support::Subsystem::Events;
const FACADE_GAP_REASON: &str = "the facade cannot observe event continuation, async-bail classification, plugin retries, config evaluation, or host loading";

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.emit()`; corrected by SOURCE-OF-TRUTH R9.
    emit_error_does_not_abort_remaining_listeners,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.emit() hazard absence / R9",
    setup: ["three ordered listeners where the middle listener fails"],
    actions: ["emit one typed event"],
    expected: ["first, second, and third listeners are all invoked", "dispatch reports the middle failure after completing the snapshot"]
}

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.bail()`; corrected by SOURCE-OF-TRUTH R9.
    async_listener_result_does_not_count_as_bailed,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.bail() async-result hazard absence / R9",
    setup: ["first bail listener returns a future", "second returns a synchronous value"],
    actions: ["dispatch one bail event"],
    expected: ["future object is ignored as a bail result", "second listener runs and supplies the result"]
}

spec_case! {
    /// Rule origin: SOURCE-OF-TRUTH R9, side-effectful service constructors stay absent.
    service_construction_cannot_mutate_context,
    origin: "rule: R9 / side-effectful service constructors",
    test: "service constructor hazard absence",
    setup: ["construct a service value before activation"],
    actions: ["compare kernel effects and ledger before and after construction", "activate through explicit plugin boundary"],
    expected: ["construction creates no effect or ledger entry", "mutations become possible only inside activation"]
}

spec_case! {
    /// Rule origin: SOURCE-OF-TRUTH R9, config evaluation with ambient authority stays absent.
    config_expression_lane_has_no_ambient_authority,
    origin: "rule: R9 / closed side-effect-free config subset",
    test: "ambient config evaluation hazard absence",
    setup: ["config expression attempts filesystem, network, environment, and process access"],
    actions: ["parse and validate at the profile boundary"],
    expected: ["all ambient-authority forms are rejected", "no capability call or ledger side effect occurs"]
}

spec_case! {
    /// Rule origin: SOURCE-OF-TRUTH R9, native-library unload stays absent.
    native_dynamic_library_backend_is_unrepresentable,
    origin: "rule: R9 / no native library unload",
    test: "native dylib hazard absence",
    setup: ["enumerate supported plugin backends and profile manifest forms"],
    actions: ["attempt to declare a native dynamic library artifact"],
    expected: ["manifest is rejected", "only sandboxed WASM and disabled-until-sandboxed subprocess backends exist"]
}

spec_case! {
    /// Rule origin: SOURCE-OF-TRUTH R9, silent service replacement stays absent.
    provider_generation_change_forces_consumer_unload_reload,
    origin: "rule: R9 / no silent service replacement",
    test: "silent replacement hazard absence",
    setup: ["active consumer owns provider generation 1"],
    actions: ["replace provider with generation 2", "wait for quiescence"],
    expected: ["consumer tears down using generation 1", "a new activation captures generation 2", "no activation observes both"]
}

spec_case! {
    /// Rule origin: SOURCE-OF-TRUTH R9, failed-fiber auto-retry stays absent.
    failed_fiber_does_not_retry_without_environment_change,
    origin: "rule: R9 / no auto-retry on unchanged environment",
    test: "failed fiber retry hazard absence",
    setup: ["plugin body increments an attempt counter then fails", "dependencies and config remain unchanged"],
    actions: ["advance virtual time repeatedly", "wait for quiescence repeatedly"],
    expected: ["fiber remains failed", "attempt counter stays exactly one", "no new transition or ledger retry event appears"]
}
