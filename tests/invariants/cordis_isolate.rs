mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use jinnd_api::{
    ContextId, DispatchMode, ErrorCode, Event, IsolationBinding, Kernel, Realm, ServiceContract,
};
use support::{Listener, expect_ok, ready, spec_case};

#[derive(Debug)]
struct RootVisible(u8);

impl ServiceContract for RootVisible {
    type Observation = u8;

    const NAME: &'static str = "jinn.test/root-visible";

    fn observe(&self) -> Self::Observation {
        self.0
    }
}

#[derive(Debug)]
struct IsolatedEventSource;

impl ServiceContract for IsolatedEventSource {
    type Observation = ();

    const NAME: &'static str = "jinn.test/isolated-event-source";

    fn observe(&self) {}
}

#[derive(Clone, Debug)]
struct IsolatedEvent {
    target: ContextId,
}

impl Event for IsolatedEvent {
    type Output = ();

    const MODE: DispatchMode = DispatchMode::Emit;

    fn selects(&self, listener: ContextId) -> bool {
        listener == self.target
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
    expected: ["each consumer resolves only its own realm", "withdrawal removes only the matching slot"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let root = kernel.root_context();
        let one_realm = Realm::Local(jinnd_api::EntryId("one".to_owned()));
        let two_realm = Realm::Local(jinnd_api::EntryId("two".to_owned()));
        let one = kernel.derive_context(root, vec![IsolationBinding {
            service: RootVisible::NAME.to_owned(),
            realm: one_realm.clone(),
        }]);
        let two = kernel.derive_context(root, vec![IsolationBinding {
            service: RootVisible::NAME.to_owned(),
            realm: two_realm.clone(),
        }]);
        let root_effect = expect_ok(
            kernel.provide(root, Realm::Root, Arc::new(RootVisible(1))).await,
            "root slot should provide",
        );
        let one_effect = expect_ok(
            kernel.provide(one, one_realm, Arc::new(RootVisible(2))).await,
            "first local slot should provide",
        );
        let two_effect = expect_ok(
            kernel.provide(two, two_realm, Arc::new(RootVisible(3))).await,
            "second local slot should provide",
        );
        assert_eq!(expect_ok(kernel.resolve::<RootVisible>(root), "root").service.observe(), 1);
        assert_eq!(expect_ok(kernel.resolve::<RootVisible>(one), "one").service.observe(), 2);
        assert_eq!(expect_ok(kernel.resolve::<RootVisible>(two), "two").service.observe(), 3);

        expect_ok(kernel.dispose_effect(one_effect).await, "withdraw first local slot");
        assert_eq!(kernel.resolve::<RootVisible>(one).err().map(|error| error.code), Some(ErrorCode::MissingDependency));
        assert_eq!(expect_ok(kernel.resolve::<RootVisible>(two), "two remains").service.observe(), 3);
        assert_eq!(expect_ok(kernel.resolve::<RootVisible>(root), "root remains").service.observe(), 1);
        expect_ok(kernel.dispose_effect(two_effect).await, "withdraw second local slot");
        expect_ok(kernel.dispose_effect(root_effect).await, "withdraw root slot");
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/isolate.spec.ts`, test `shared label`.
    shared_realm_label_connects_separate_derived_contexts,
    origin: "packages/core/tests/isolate.spec.ts",
    test: "shared label",
    setup: ["two child contexts map a service to the same shared realm"],
    actions: ["provide and withdraw a value through the first child"],
    expected: ["both children resolve the same generation", "withdrawal removes the shared slot from both", "root realm remains independent"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let root = kernel.root_context();
        let shared = Realm::Shared("shared".to_owned());
        let binding = IsolationBinding {
            service: RootVisible::NAME.to_owned(),
            realm: shared.clone(),
        };
        let one = kernel.derive_context(root, vec![binding.clone()]);
        let two = kernel.derive_context(root, vec![binding]);
        let effect = expect_ok(
            kernel.provide(one, shared, Arc::new(RootVisible(9))).await,
            "shared slot should provide",
        );
        let first = expect_ok(kernel.resolve::<RootVisible>(one), "first child");
        let second = expect_ok(kernel.resolve::<RootVisible>(two), "second child");
        assert_eq!(first.service.observe(), 9);
        assert_eq!(second.service.observe(), 9);
        assert_eq!(first.generation, second.generation);
        assert_eq!(kernel.resolve::<RootVisible>(root).err().map(|error| error.code), Some(ErrorCode::MissingDependency));
        expect_ok(kernel.dispose_effect(effect).await, "withdraw shared slot");
        assert_eq!(kernel.resolve::<RootVisible>(one).err().map(|error| error.code), Some(ErrorCode::MissingDependency));
        assert_eq!(kernel.resolve::<RootVisible>(two).err().map(|error| error.code), Some(ErrorCode::MissingDependency));
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/isolate.spec.ts`, test `isolated event`.
    event_payload_filter_routes_to_matching_isolated_context,
    origin: "packages/core/tests/isolate.spec.ts",
    test: "isolated event",
    setup: ["root and isolated child register listeners", "service is provided inside isolated child"],
    actions: ["service emits a typed payload scoped to its caller context"],
    expected: ["child listener receives one event", "root listener receives none"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let root = kernel.root_context();
        let realm = Realm::Shared("isolated-event".to_owned());
        let child = kernel.derive_context(
            root,
            vec![IsolationBinding {
                service: IsolatedEventSource::NAME.to_owned(),
                realm: realm.clone(),
            }],
        );
        expect_ok(
            kernel
                .provide(child, realm, Arc::new(IsolatedEventSource))
                .await,
            "the isolated event source should be provided",
        );
        let source = expect_ok(
            kernel.resolve::<IsolatedEventSource>(child),
            "the isolated child should resolve its source",
        );
        assert_eq!(source.caller, child);
        let root_error = match kernel.resolve::<IsolatedEventSource>(root) {
            Ok(_) => panic!("the root must not resolve the child's isolated source"),
            Err(error) => error,
        };
        assert_eq!(root_error.code, ErrorCode::MissingDependency);

        let root_calls = Arc::new(AtomicUsize::new(0));
        let root_listener_calls = Arc::clone(&root_calls);
        expect_ok(
            kernel.listen(
                root,
                Listener(move |_caller, _event: IsolatedEvent| {
                    root_listener_calls.fetch_add(1, Ordering::SeqCst);
                    ready(Ok(()))
                }),
            ),
            "the root listener should register",
        );
        let child_calls = Arc::new(AtomicUsize::new(0));
        let child_listener_calls = Arc::clone(&child_calls);
        expect_ok(
            kernel.listen(
                child,
                Listener(move |_caller, _event: IsolatedEvent| {
                    child_listener_calls.fetch_add(1, Ordering::SeqCst);
                    ready(Ok(()))
                }),
            ),
            "the child listener should register",
        );

        let report = expect_ok(
            kernel
                .dispatch_report(
                    source.caller,
                    IsolatedEvent {
                        target: source.caller,
                    },
                )
                .await,
            "the isolated event should settle",
        );
        assert!(report.failures.is_empty());
        assert_eq!(child_calls.load(Ordering::SeqCst), 1);
        assert_eq!(root_calls.load(Ordering::SeqCst), 0);
    }
}
