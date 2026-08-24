mod support;

use support::spec_case;

const SUBSYSTEM: support::Subsystem = support::Subsystem::Events;
const FACADE_GAP_REASON: &str = "the facade has no listener disposal, once-listener, payload filter, or per-mode failure observation contract";

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.on()`.
    listener_effect_receives_until_disposed,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.on()",
    setup: ["register one typed listener as an effect"],
    actions: ["emit twice", "dispose listener", "emit again"],
    expected: ["listener call count is exactly two"]
}

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.once()`.
    once_listener_disposes_before_second_dispatch,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.once()",
    setup: ["register one once-listener"],
    actions: ["emit twice", "dispose returned effect", "emit again"],
    expected: ["listener call count is exactly one"]
}

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.parallel()`.
    parallel_dispatch_filters_and_aggregates_all_listener_errors,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.parallel()",
    setup: ["register context-filtered listener", "register synchronous and delayed failing listeners"],
    actions: ["dispatch matching and nonmatching payloads", "dispatch to both failures"],
    expected: ["only matching context receives payload", "all listeners settle", "both errors are returned in one aggregate"]
}

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.emit()`; R9 changes its error expectation.
    emit_filters_without_aborting_after_listener_error,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.emit() (R9 hazard-corrected)",
    setup: ["register matching listener followed by a failing listener and a trailing listener"],
    actions: ["emit matching and nonmatching typed payloads", "emit when middle listener fails"],
    expected: ["filtering follows payload-to-listener context routing", "trailing listener still runs", "failure is reported after dispatch"]
}

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.serial()`.
    serial_dispatch_filters_orders_and_propagates_failure,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.serial()",
    setup: ["register ordered context-filtered listeners"],
    actions: ["serially dispatch matching and nonmatching payloads", "make one listener fail"],
    expected: ["matching listeners run in registration order", "dispatch returns listener error"]
}

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.bail()`; async-result semantics corrected by R9.
    bail_returns_first_synchronous_value_only,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.bail() (R9 hazard-corrected)",
    setup: ["register filtered listeners returning none, a future, and a synchronous value"],
    actions: ["dispatch matching and nonmatching payloads"],
    expected: ["a future object is not a bail value", "first synchronous non-none value stops dispatch"]
}

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.waterfall()`.
    waterfall_composes_until_middleware_declines_next,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.waterfall()",
    setup: ["register two additive middleware listeners", "then add a listener that does not call next"],
    actions: ["dispatch waterfall before and after terminal middleware"],
    expected: ["first result is 4", "second result is 3", "listeners after terminal middleware do not run"]
}
