//! Provision and resolution through the typed surface: effects, boundaries,
//! generations (R3, R5).

#![cfg(not(feature = "loom"))]

mod support;

use jinnd_api::{ErrorCode, Realm, ServiceContract};
use jinnd_context::ContextTree;
use jinnd_effects::EffectScope;
use jinnd_registry::Registry;
use support::{Counter, KERNEL_SCOPE, provide_counter};

#[tokio::test(flavor = "current_thread")]
async fn a_provided_service_resolves_with_caller_provider_and_realm() {
    let tree: ContextTree = ContextTree::new();
    let registry = Registry::new();
    let mut scope = EffectScope::new();

    let generation = provide_counter(&registry, &mut scope, &tree.root(), 7);
    let resolved = registry.resolve::<Counter, ()>(&tree.root());

    let Ok(handle) = resolved else {
        panic!("a root provision must resolve from root: {resolved:?}");
    };
    assert_eq!(handle.service.observe(), 7);
    assert_eq!(handle.caller, tree.root().id());
    assert_eq!(handle.provider, KERNEL_SCOPE);
    assert_eq!(handle.generation, generation);
    assert_eq!(handle.realm, Realm::Root);
}

#[tokio::test(flavor = "current_thread")]
async fn a_child_context_resolves_through_its_ancestors() {
    let tree: ContextTree = ContextTree::new();
    let registry = Registry::new();
    let mut scope = EffectScope::new();

    provide_counter(&registry, &mut scope, &tree.root(), 3);
    let child = tree.root().derive().build();

    let observed = registry
        .resolve::<Counter, ()>(&child)
        .map(|handle| handle.service.observe());
    assert_eq!(observed.ok(), Some(3));
}

#[tokio::test(flavor = "current_thread")]
async fn resolution_stops_at_an_isolation_boundary() {
    let tree: ContextTree = ContextTree::new();
    let registry = Registry::new();
    let mut scope = EffectScope::new();

    provide_counter(&registry, &mut scope, &tree.root(), 9);
    let name = tree.name(Counter::NAME);
    let isolated = tree
        .root()
        .derive()
        .isolate(name, tree.realm(&Realm::Shared("island".to_owned())))
        .build();

    let error = match registry.resolve::<Counter, ()>(&isolated) {
        Err(error) => error,
        Ok(handle) => panic!("an isolated realm must not see the root provider: {handle:?}"),
    };
    assert_eq!(error.code, ErrorCode::MissingDependency);
}

#[tokio::test(flavor = "current_thread")]
async fn an_unprovided_service_is_a_missing_dependency() {
    let tree: ContextTree = ContextTree::new();
    let registry = Registry::new();

    let error = match registry.resolve::<Counter, ()>(&tree.root()) {
        Err(error) => error,
        Ok(handle) => panic!("nothing was provided: {handle:?}"),
    };
    assert_eq!(error.code, ErrorCode::MissingDependency);
}

#[tokio::test(flavor = "current_thread")]
async fn replaying_the_owning_scope_withdraws_the_slot() {
    let tree: ContextTree = ContextTree::new();
    let registry = Registry::new();
    let mut scope = EffectScope::new();

    provide_counter(&registry, &mut scope, &tree.root(), 1);
    assert!(registry.resolve::<Counter, ()>(&tree.root()).is_ok());

    let report = scope.replay().await;
    assert!(
        report.is_clean(),
        "withdrawal must replay cleanly: {report:?}"
    );
    let error = match registry.resolve::<Counter, ()>(&tree.root()) {
        Err(error) => error,
        Ok(handle) => panic!("the slot must disappear through undo replay (R5): {handle:?}"),
    };
    assert_eq!(error.code, ErrorCode::MissingDependency);
}

#[tokio::test(flavor = "current_thread")]
async fn reprovision_replaces_under_a_newer_generation() {
    let tree: ContextTree = ContextTree::new();
    let registry = Registry::new();
    let mut scope = EffectScope::new();

    let first = provide_counter(&registry, &mut scope, &tree.root(), 1);
    let second = provide_counter(&registry, &mut scope, &tree.root(), 2);
    assert!(second > first, "replacement is never silent (R9)");

    let observed = registry
        .resolve::<Counter, ()>(&tree.root())
        .map(|handle| (handle.service.observe(), handle.generation));
    assert_eq!(observed.ok(), Some((2, second)));
}

#[tokio::test(flavor = "current_thread")]
async fn a_stale_undo_does_not_withdraw_a_newer_provider() {
    let tree: ContextTree = ContextTree::new();
    let registry = Registry::new();
    let mut first_scope = EffectScope::new();
    let mut second_scope = EffectScope::new();

    provide_counter(&registry, &mut first_scope, &tree.root(), 1);
    let second = provide_counter(&registry, &mut second_scope, &tree.root(), 2);

    let report = first_scope.replay().await;
    assert!(report.is_clean());
    let observed = registry
        .resolve::<Counter, ()>(&tree.root())
        .map(|handle| (handle.service.observe(), handle.generation));
    assert_eq!(
        observed.ok(),
        Some((2, second)),
        "withdrawing generation one must leave generation two in place (I1)"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_dying_provider_waits_for_its_dependents_lease() {
    let tree: ContextTree = ContextTree::new();
    let registry = Registry::new();
    let mut scope = EffectScope::new();

    provide_counter(&registry, &mut scope, &tree.root(), 5);
    let leased = registry.lease::<Counter, ()>(&tree.root());
    let Ok((handle, guard)) = leased else {
        panic!("the provided service must lease: {leased:?}");
    };

    let withdrawal = tokio::spawn(async move { scope.replay().await });
    tokio::task::yield_now().await;
    assert!(
        !withdrawal.is_finished(),
        "the provider must wait for its dependent's lease (I2)"
    );
    // The dependent may still call the dying service while it tears down (I2).
    assert_eq!(handle.service.observe(), 5);

    drop(guard);
    let Ok(report) = withdrawal.await else {
        panic!("the withdrawal task must not panic");
    };
    assert!(
        report.is_clean(),
        "the drained withdrawal completes: {report:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_named_realm_connects_same_realm_contexts_across_subtrees() {
    let tree: ContextTree = ContextTree::new();
    let registry = Registry::new();
    let mut scope = EffectScope::new();

    // Provider and consumer live in unrelated subtrees; only the shared realm
    // label connects them (LAW §3 isolation: realms are the visibility unit,
    // never tree position).
    let name = tree.name(Counter::NAME);
    let realm = Realm::Shared("island".to_owned());
    let provider_home = tree
        .root()
        .derive()
        .isolate(name, tree.realm(&realm))
        .build();
    let consumer_home = tree
        .root()
        .derive()
        .isolate(name, tree.realm(&realm))
        .build();

    let provision = registry.provide::<Counter, ()>(
        &provider_home,
        &realm,
        KERNEL_SCOPE,
        std::sync::Arc::new(Counter(11)),
        &registry.vitality(true),
    );
    let registered = scope.register("provide counter".to_owned(), provision.undo);
    assert!(
        registered.is_ok(),
        "the provision undo must register: {registered:?}"
    );

    let observed = registry
        .resolve::<Counter, ()>(&consumer_home)
        .map(|handle| (handle.service.observe(), handle.realm.clone()));
    assert_eq!(observed.ok(), Some((11, realm)));

    // The root realm stays positional: the root context does not see it.
    let error = match registry.resolve::<Counter, ()>(&tree.root()) {
        Err(error) => error,
        Ok(handle) => panic!("the root realm must not see a named realm: {handle:?}"),
    };
    assert_eq!(error.code, ErrorCode::MissingDependency);
}
