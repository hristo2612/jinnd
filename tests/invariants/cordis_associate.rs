mod support;

use std::sync::Arc;

use jinnd_api::{ErrorCode, Kernel, Realm, ServiceContract};
use support::expect_ok;
use support::spec_case;

const SUBSYSTEM: support::Subsystem = support::Subsystem::Services;
const V02_DEFERRED_BOUND: &str = "SOURCE-OF-TRUTH R4 and constitution 01 Mechanical closure: v0.1 exposes owned WIT broker handles, not Cordis in-process associated-value or proxy-extension reflection";

#[derive(Debug)]
struct Parent(u8);

impl ServiceContract for Parent {
    type Observation = u8;
    const NAME: &'static str = "jinn.test/associate-parent";
    fn observe(&self) -> u8 {
        self.0
    }
}

#[derive(Debug)]
struct Child(u8);

impl ServiceContract for Child {
    type Observation = u8;
    const NAME: &'static str = "jinn.test/associate-child";
    fn observe(&self) -> u8 {
        self.0
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/associate.spec.ts`, test `service injection`; translated to typed nested capabilities.
    nested_service_contract_is_withdrawn_without_parent_loss,
    origin: "packages/core/tests/associate.spec.ts",
    test: "service injection (typed capability equivalent)",
    setup: ["provide parent capability and nested child capability"],
    actions: ["resolve both", "dispose child provider"],
    expected: ["parent remains resolvable with its original observation", "child becomes unavailable"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let root = kernel.root_context();
        let parent = expect_ok(kernel.provide(root, Realm::Root, Arc::new(Parent(7))).await, "parent provision");
        let child = expect_ok(kernel.provide(root, Realm::Root, Arc::new(Child(9))).await, "child provision");
        assert_eq!(expect_ok(kernel.resolve::<Parent>(root), "parent resolve").service.observe(), 7);
        assert_eq!(expect_ok(kernel.resolve::<Child>(root), "child resolve").service.observe(), 9);
        expect_ok(kernel.dispose_effect(child).await, "child withdrawal");
        assert_eq!(kernel.resolve::<Child>(root).err().map(|error| error.code), Some(ErrorCode::MissingDependency));
        assert_eq!(expect_ok(kernel.resolve::<Parent>(root), "parent remains").service.observe(), 7);
        expect_ok(kernel.dispose_effect(parent).await, "parent cleanup");
    }
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
