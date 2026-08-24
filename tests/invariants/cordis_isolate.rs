mod support;

use std::sync::Arc;

use jinnd_api::{ErrorCode, IsolationBinding, Kernel, Realm, ServiceContract};
use support::spec_case;

const SUBSYSTEM: support::Subsystem = support::Subsystem::Context;
const FACADE_GAP_REASON: &str =
    "the facade cannot withdraw a provided service effect or observe isolation-aware event routing";

#[derive(Debug)]
struct RootVisible(u8);

impl ServiceContract for RootVisible {
    type Observation = u8;

    const NAME: &'static str = "jinn.test/root-visible";

    fn observe(&self) -> Self::Observation {
        self.0
    }
}

/// Suite ruling derived from `packages/core/src/reflect.ts:80-94`, especially the
/// disagreeing-realm stop at line 92: an explicit descendant `Realm::Root` binding
/// is a real frozen-layer binding, not an erase/inherit sentinel. It therefore does
/// not reach across an intervening ancestor whose binding selects another realm.
#[tokio::test(flavor = "current_thread")]
async fn explicit_root_binding_does_not_erase_an_ancestor_isolation_boundary() {
    let kernel = jinnd_adapter::kernel();
    let root = kernel.root_context();
    let isolated = kernel.derive_context(
        root,
        vec![IsolationBinding {
            service: RootVisible::NAME.to_owned(),
            realm: Realm::Shared("isolated".to_owned()),
        }],
    );
    let rebound_to_root = kernel.derive_context(
        isolated,
        vec![IsolationBinding {
            service: RootVisible::NAME.to_owned(),
            realm: Realm::Root,
        }],
    );

    let installed = kernel
        .provide(root, Realm::Root, Arc::new(RootVisible(7)))
        .await;
    assert!(
        installed.is_ok(),
        "the root provider should be installed: {installed:?}"
    );

    let error = match kernel.resolve::<RootVisible>(rebound_to_root) {
        Err(error) => error,
        Ok(handle) => {
            panic!("the isolation mismatch must stop the walk before the root provider: {handle:?}")
        }
    };
    assert_eq!(error.code, ErrorCode::MissingDependency);
}

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
