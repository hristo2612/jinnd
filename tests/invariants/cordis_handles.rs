mod support;

use support::spec_case;

spec_case! {
    /// TS origin: `packages/core/tests/shadow.spec.ts`, test `keeps caller metadata separate from the service shadow`; R4 handle equivalent.
    nested_service_handles_keep_caller_and_provider_scopes_distinct,
    origin: "packages/core/tests/shadow.spec.ts",
    test: "keeps caller metadata separate from the service shadow (R4 handle equivalent)",
    setup: ["outer service resolves inner through its activation snapshot"],
    actions: ["root caller invokes outer then outer invokes inner"],
    expected: ["inner handle caller is outer activation", "provider scopes remain inner and outer respectively"]
}

spec_case! {
    /// TS origin: `packages/core/tests/shadow.spec.ts`, test `exposes the caller without preserving shadow for noShadow services`; R4 handle equivalent.
    plain_capability_handle_carries_caller_without_proxy_state,
    origin: "packages/core/tests/shadow.spec.ts",
    test: "exposes the caller without preserving shadow for noShadow services (R4 handle equivalent)",
    setup: ["provide a plain typed capability and resolve it inside outer service"],
    actions: ["inspect the resolved handle"],
    expected: ["caller is outer activation", "no proxy or shadow context exists"]
}

spec_case! {
    /// TS origin: `packages/core/tests/shadow.spec.ts`, test `exposes the caller to callable services`; R4 plain-method equivalent.
    method_service_handle_exposes_explicit_caller_scope,
    origin: "packages/core/tests/shadow.spec.ts",
    test: "exposes the caller to callable services (R4 plain-method equivalent)",
    setup: ["replace callable service with a typed method contract", "outer service resolves it"],
    actions: ["outer invokes the method"],
    expected: ["method observes outer activation as caller"]
}

spec_case! {
    /// TS origin: `packages/core/tests/shadow.spec.ts`, test `strips service shadow before creating plugins`; R4 handle equivalent.
    service_spawn_uses_explicit_caller_context_without_proxy_leak,
    origin: "packages/core/tests/shadow.spec.ts",
    test: "strips service shadow before creating plugins (R4 handle equivalent)",
    setup: ["loader service is invoked through a caller-scoped handle"],
    actions: ["loader spawns provider then consumer from the caller context"],
    expected: ["consumer resolves provider", "no proxy metadata or error leaks into child activation"]
}

spec_case! {
    /// TS origin: `packages/core/tests/invoke.spec.ts`, test `functional service`; translated to plain typed methods per R4.
    service_method_merges_base_intercept_extension_and_call_config,
    origin: "packages/core/tests/invoke.spec.ts",
    test: "functional service (R4 plain-method equivalent)",
    setup: ["service base config a=1", "caller intercept b=2"],
    actions: ["call from root and intercepted handles", "derive extensions c=3 and d=4", "invoke with per-call config"],
    expected: ["results preserve precedence base, intercept, extension, call", "derived handles retain caller scope"]
}

spec_case! {
    /// TS origin: `packages/core/tests/invoke.spec.ts`, test `uses the service shadow for callable extensions`; translated to plain typed methods per R4.
    extended_service_handle_keeps_dependency_snapshot,
    origin: "packages/core/tests/invoke.spec.ts",
    test: "uses the service shadow for callable extensions (R4 plain-method equivalent)",
    setup: ["method service requires a typed dependency", "outer service holds its handle"],
    actions: ["invoke original and extended handles"],
    expected: ["both calls resolve the same dependency generation from the owned activation snapshot"]
}
