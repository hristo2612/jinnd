use jinnd_api::{FiberState, Kernel};

use crate::loader_fixture::{
    COUNT, activations, child, disabled, entry, fiber, group, id, log, reconcile, register, state,
};

fn tree(
    outer_disabled: bool,
    inner_disabled: bool,
) -> Vec<jinnd_api::ProfileEntry<crate::loader_fixture::Config>> {
    let outer = if outer_disabled {
        disabled(group("outer"))
    } else {
        group("outer")
    };
    let inner = child(
        if inner_disabled {
            disabled(group("inner"))
        } else {
            group("inner")
        },
        "outer",
    );
    vec![
        outer,
        inner,
        child(entry("outer-child", COUNT, 1), "outer"),
        child(entry("inner-child", COUNT, 2), "inner"),
    ]
}

pub async fn nested_initialize() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    reconcile(&kernel, tree(false, false)).await;
    assert_eq!(activations(&log, "outer-child"), 1);
    assert_eq!(activations(&log, "inner-child"), 1);
    let persisted = kernel
        .persisted_profile::<crate::loader_fixture::Config>()
        .unwrap_or_else(|| panic!("group tree should persist"));
    assert_eq!(persisted.entries.len(), 4);
}

pub async fn disable_inner() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    reconcile(&kernel, tree(false, false)).await;
    let outer = fiber(&kernel, "outer-child");
    reconcile(&kernel, tree(false, true)).await;
    assert_eq!(kernel.entry_fiber(&id("outer-child")), Some(outer));
    assert_eq!(kernel.state(outer), FiberState::Active);
    assert!(kernel.entry_fiber(&id("inner-child")).is_none());
    assert_eq!(activations(&log, "outer-child"), 1);
}

pub async fn disable_outer() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    reconcile(&kernel, tree(false, true)).await;
    reconcile(&kernel, tree(true, true)).await;
    assert!(kernel.entry_fiber(&id("outer-child")).is_none());
    assert!(kernel.entry_fiber(&id("inner-child")).is_none());
    let persisted = kernel
        .persisted_profile::<crate::loader_fixture::Config>()
        .unwrap_or_else(|| panic!("disabled tree should persist"));
    assert_eq!(persisted.entries.len(), 4);
}

pub async fn enable_inner_under_disabled() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    reconcile(&kernel, tree(true, true)).await;
    reconcile(&kernel, tree(true, false)).await;
    assert_eq!(activations(&log, "outer-child"), 0);
    assert_eq!(activations(&log, "inner-child"), 0);
    assert!(state(&kernel, "inner-child").is_none());
}

pub async fn enable_outer() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    reconcile(&kernel, tree(true, false)).await;
    reconcile(&kernel, tree(false, false)).await;
    assert_eq!(activations(&log, "outer-child"), 1);
    assert_eq!(activations(&log, "inner-child"), 1);
    assert_eq!(state(&kernel, "outer-child"), Some(FiberState::Active));
    assert_eq!(state(&kernel, "inner-child"), Some(FiberState::Active));
}

fn transfer_tree(
    parent: Option<&str>,
) -> Vec<jinnd_api::ProfileEntry<crate::loader_fixture::Config>> {
    let alpha = group("alpha");
    let beta = disabled(child(group("beta"), "alpha"));
    let gamma = child(group("gamma"), "beta");
    let plugin = match parent {
        Some(parent) => child(entry("plugin", COUNT, 1), parent),
        None => entry("plugin", COUNT, 1),
    };
    vec![alpha, beta, gamma, plugin]
}

pub async fn transfer_initial() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    reconcile(&kernel, transfer_tree(None)).await;
    assert_eq!(activations(&log, "plugin"), 1);
    let persisted = kernel
        .persisted_profile::<crate::loader_fixture::Config>()
        .unwrap_or_else(|| panic!("transfer tree should persist"));
    assert_eq!(persisted.entries.len(), 4);
}

pub async fn move_enabled_to_enabled() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    reconcile(&kernel, transfer_tree(None)).await;
    let original = fiber(&kernel, "plugin");
    reconcile(&kernel, transfer_tree(Some("alpha"))).await;
    assert_eq!(kernel.entry_fiber(&id("plugin")), Some(original));
    assert_eq!(activations(&log, "plugin"), 1);
}

pub async fn move_enabled_to_disabled() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    reconcile(&kernel, transfer_tree(Some("alpha"))).await;
    reconcile(&kernel, transfer_tree(Some("beta"))).await;
    assert!(kernel.entry_fiber(&id("plugin")).is_none());
    assert_eq!(activations(&log, "plugin"), 1);
}

pub async fn move_disabled_to_disabled() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    reconcile(&kernel, transfer_tree(Some("beta"))).await;
    reconcile(&kernel, transfer_tree(Some("gamma"))).await;
    assert!(kernel.entry_fiber(&id("plugin")).is_none());
    assert_eq!(activations(&log, "plugin"), 0);
}

pub async fn move_disabled_to_root() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    reconcile(&kernel, transfer_tree(Some("gamma"))).await;
    reconcile(&kernel, transfer_tree(None)).await;
    assert_eq!(activations(&log, "plugin"), 1);
    assert_eq!(state(&kernel, "plugin"), Some(FiberState::Active));
}
