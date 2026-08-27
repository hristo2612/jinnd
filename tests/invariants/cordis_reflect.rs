mod support;

use support::spec_case;

const SUBSYSTEM: support::Subsystem = support::Subsystem::Context;
const V02_DEFERRED_BOUND: &str = "SOURCE-OF-TRUTH R3/R7 and constitution 01: v0.1 access is typed or WIT-brokered and exposes no arbitrary context-lineage or dynamic reflection contract";

spec_case! {
    /// TS origin: `packages/core/tests/reflect.spec.ts`, test `Context.is()`; translated to typed context lineage.
    derived_context_retains_kernel_context_identity,
    origin: "packages/core/tests/reflect.spec.ts",
    test: "Context.is() (typed context equivalent)",
    setup: ["derive a child context through an extension"],
    actions: ["query its kernel context identity"],
    expected: ["child is recognized as a context", "parent lineage is preserved"]
}

spec_case! {
    /// TS origin: `packages/core/tests/reflect.spec.ts`, test `access check`; translated to R3 typed access.
    service_access_requires_declared_read_or_provision_capability,
    origin: "packages/core/tests/reflect.spec.ts",
    test: "access check (typed capability equivalent)",
    setup: ["plugin declares neither read nor provision for one service"],
    actions: ["attempt resolve and provide", "declare provision then provide twice"],
    expected: ["undeclared operations are rejected", "first declared provide succeeds", "duplicate provider in same slot is rejected"]
}

spec_case! {
    /// TS origin: `packages/core/tests/reflect.spec.ts`, test `service injection`; translated to R3 typed access.
    only_service_contracts_are_resolvable_as_services,
    origin: "packages/core/tests/reflect.spec.ts",
    test: "service injection (typed capability equivalent)",
    setup: ["provide one service with an extension and one plain context value"],
    actions: ["resolve service, extension type, and plain context type"],
    expected: ["service resolves", "extension and plain context values do not masquerade as service slots"]
}

spec_case! {
    /// TS origin: `packages/core/tests/reflect.spec.ts`, test `service inject leak`.
    disposed_activation_cannot_reuse_its_dependency_snapshot,
    origin: "packages/core/tests/reflect.spec.ts",
    test: "service inject leak",
    setup: ["activate consumer with one resolved dependency snapshot"],
    actions: ["dispose consumer", "resolve through retained activation context"],
    expected: ["retained context returns InactiveContext", "service value does not leak past disposal"]
}
