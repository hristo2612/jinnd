mod support;

use support::spec_case;

spec_case! {
    /// TS origin: `packages/core/tests/associate.spec.ts`, test `service injection`; translated to typed nested capabilities.
    nested_service_contract_is_withdrawn_without_parent_loss,
    origin: "packages/core/tests/associate.spec.ts",
    test: "service injection (typed capability equivalent)",
    setup: ["provide parent capability and nested child capability"],
    actions: ["resolve both", "dispose child provider"],
    expected: ["parent remains resolvable with its original observation", "child becomes unavailable"]
}

spec_case! {
    /// TS origin: `packages/core/tests/associate.spec.ts`, test `property injection`; translated to R3 typed access.
    typed_extension_exposes_only_declared_capabilities,
    origin: "packages/core/tests/associate.spec.ts",
    test: "property injection (typed capability equivalent)",
    setup: ["provide parent and two declared extension contracts"],
    actions: ["resolve declared handles", "attempt undeclared dynamic lookup"],
    expected: ["declared values and method receiver are preserved", "undeclared lookup is rejected"]
}

spec_case! {
    /// TS origin: `packages/core/tests/associate.spec.ts`, test `associated type - service injection`; translated to explicit handles.
    associated_value_gains_extension_only_with_dependency_snapshot,
    origin: "packages/core/tests/associate.spec.ts",
    test: "associated type - service injection (R4 handle equivalent)",
    setup: ["service creates an associated session value", "extension capability is initially absent"],
    actions: ["create session before and during extension injection"],
    expected: ["first session has no extension", "injected session extension returns 42"]
}

spec_case! {
    /// TS origin: `packages/core/tests/associate.spec.ts`, test `associated type - accessor injection`; translated to explicit handles.
    associated_accessor_preserves_getter_setter_semantics,
    origin: "packages/core/tests/associate.spec.ts",
    test: "associated type - accessor injection (R4 handle equivalent)",
    setup: ["associated session has an injected typed accessor"],
    actions: ["read without dependency", "write 100 then read with dependency"],
    expected: ["unavailable accessor is rejected outside its snapshot", "setter transformation yields 101"]
}

spec_case! {
    /// TS origin: `packages/core/tests/associate.spec.ts`, test `inspect`.
    service_method_does_not_rewrite_argument_type_identity,
    origin: "packages/core/tests/associate.spec.ts",
    test: "inspect",
    setup: ["resolve service through a scope-carrying handle", "prepare a type-valued argument"],
    actions: ["pass argument through two nested service methods"],
    expected: ["both methods observe the original argument identity and debug representation"]
}
