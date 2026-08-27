mod support;

use support::spec_case;

const SUBSYSTEM: support::Subsystem = support::Subsystem::Context;
const V02_DEFERRED_BOUND: &str = "constitution 02 makes the append-only ledger the v0.1 diagnostic record; Cordis mutable logger buffers, exporters, and source-intercept inspection are not capability contracts";

spec_case! {
    /// TS origin: `packages/core/tests/logger.spec.ts`, test `keeps the bounded buffer in place and chronological`.
    ledger_diagnostic_buffer_is_stable_bounded_and_chronological,
    origin: "packages/core/tests/logger.spec.ts",
    test: "keeps the bounded buffer in place and chronological",
    setup: ["retain diagnostic buffer identity", "set capacity to two"],
    actions: ["append one, two, three", "shrink to one and append four", "set zero and append five"],
    expected: ["buffer allocation identity is stable", "observations are two-three, then four, then empty"]
}

spec_case! {
    /// TS origin: `packages/core/tests/logger.spec.ts`, test `disposes the exporter that registered the disposer`.
    exporter_effect_removes_only_its_own_sink,
    origin: "packages/core/tests/logger.spec.ts",
    test: "disposes the exporter that registered the disposer",
    setup: ["register two diagnostic exporters as effects"],
    actions: ["dispose first and emit", "dispose second and emit"],
    expected: ["first receives nothing after its disposal", "second receives exactly the first post-disposal message"]
}

spec_case! {
    /// TS origin: `packages/core/tests/logger.spec.ts`, test `uses fiber name when called from outside any service`.
    diagnostic_uses_fiber_name_outside_service_call,
    origin: "packages/core/tests/logger.spec.ts",
    test: "uses fiber name when called from outside any service",
    setup: ["root activation emits a diagnostic"],
    actions: ["inspect captured diagnostic"],
    expected: ["diagnostic source is root"]
}

spec_case! {
    /// TS origin: `packages/core/tests/logger.spec.ts`, test `honours explicit name argument`.
    explicit_diagnostic_source_overrides_fiber_name,
    origin: "packages/core/tests/logger.spec.ts",
    test: "honours explicit name argument",
    setup: ["root activation selects explicit source custom"],
    actions: ["emit and inspect diagnostic"],
    expected: ["diagnostic source is custom"]
}

spec_case! {
    /// TS origin: `packages/core/tests/logger.spec.ts`, test `honours intercept name`.
    diagnostic_intercept_overrides_default_source,
    origin: "packages/core/tests/logger.spec.ts",
    test: "honours intercept name",
    setup: ["derived context intercepts diagnostic source as intercepted"],
    actions: ["emit and inspect diagnostic"],
    expected: ["diagnostic source is intercepted"]
}

spec_case! {
    /// TS origin: `packages/core/tests/logger.spec.ts`, test `uses service name when called from inside a Service method (regression)`; translated to R4 handles.
    diagnostic_uses_service_name_inside_handle_call,
    origin: "packages/core/tests/logger.spec.ts",
    test: "uses service name when called from inside a Service method (R4 handle equivalent)",
    setup: ["service named foo:driver emits through a resolved handle"],
    actions: ["invoke service method"],
    expected: ["diagnostic source contains foo:driver", "source does not fall back to root"]
}

spec_case! {
    /// TS origin: `packages/core/tests/logger.spec.ts`, test `still lets outer caller intercept override the service-derived name`; translated to R4 handles.
    caller_intercept_overrides_handle_service_name,
    origin: "packages/core/tests/logger.spec.ts",
    test: "still lets outer caller intercept override the service-derived name (R4 handle equivalent)",
    setup: ["caller context intercepts source as caller-override", "service is named foo:driver"],
    actions: ["invoke service through caller-scoped handle"],
    expected: ["diagnostic source is caller-override", "foo:driver is not emitted"]
}

spec_case! {
    /// TS origin: `packages/core/tests/logger.spec.ts`, test `uses the innermost service name and restores the outer service`; translated to R4 handles.
    nested_handle_calls_restore_outer_diagnostic_scope,
    origin: "packages/core/tests/logger.spec.ts",
    test: "uses the innermost service name and restores the outer service (R4 handle equivalent)",
    setup: ["foo:driver calls bar:driver then emits itself"],
    actions: ["invoke foo through a scoped handle"],
    expected: ["ordered sources are bar:driver then foo:driver"]
}

spec_case! {
    /// TS origin: `packages/core/tests/logger.spec.ts`, test `uses service name when called from inside [Service.init] (unchanged behaviour)`.
    initialization_diagnostic_uses_plugin_identity,
    origin: "packages/core/tests/logger.spec.ts",
    test: "uses service name when called from inside [Service.init] (unchanged behaviour)",
    setup: ["plugin named foo:driver emits during initialization"],
    actions: ["activate and inspect diagnostics"],
    expected: ["diagnostic source contains foo:driver"]
}
