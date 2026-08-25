use jinnd_api::Kernel;

use crate::loader_fixture::{
    COUNT, activations, child, disabled, entry, fiber, group, id, log, profile, reconcile, register,
};
use crate::support::expect_ok;

pub async fn initial_profile() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    reconcile(
        &kernel,
        vec![
            entry("foo", COUNT, 1),
            group("group"),
            child(entry("bar", COUNT, 2), "group"),
            disabled(child(entry("qux", COUNT, 4), "group")),
        ],
    )
    .await;

    assert_eq!(activations(&log, "foo"), 1);
    assert_eq!(activations(&log, "bar"), 1);
    assert_eq!(activations(&log, "qux"), 0);
    assert!(kernel.entry_fiber(&id("qux")).is_none());
    let persisted = kernel
        .persisted_profile::<crate::loader_fixture::Config>()
        .unwrap_or_else(|| panic!("profile should be persisted"));
    assert_eq!(persisted.entries.len(), 4);
}

pub async fn reconcile_by_id() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    reconcile(
        &kernel,
        vec![
            entry("foo", COUNT, 1),
            entry("bar", COUNT, 2),
            disabled(entry("qux", COUNT, 4)),
        ],
    )
    .await;
    let unchanged_fiber = fiber(&kernel, "foo");

    let report = expect_ok(
        kernel
            .reconcile(profile(vec![
                entry("foo", COUNT, 1),
                entry("qux", COUNT, 4),
            ]))
            .await,
        "second reconcile should settle",
    );
    expect_ok(
        kernel.wait_for_quiescence().await,
        "second reconcile should quiesce",
    );
    assert_eq!(report.unchanged, vec![id("foo")]);
    assert_eq!(report.disposed, vec![id("bar")]);
    assert_eq!(report.created, vec![id("qux")]);
    assert_eq!(kernel.entry_fiber(&id("foo")), Some(unchanged_fiber));
    assert_eq!(activations(&log, "foo"), 1);
    assert_eq!(activations(&log, "qux"), 1);
}

pub async fn runtime_update() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    reconcile(
        &kernel,
        vec![entry("one", COUNT, 1), entry("four", COUNT, 4)],
    )
    .await;
    let sibling = fiber(&kernel, "four");
    let config = crate::loader_fixture::Config {
        entry: "one".to_owned(),
        value: 3,
    };
    expect_ok(
        kernel.update_entry(&id("one"), config).await,
        "runtime update should settle",
    );

    let persisted = kernel
        .persisted_profile::<crate::loader_fixture::Config>()
        .unwrap_or_else(|| panic!("profile should be persisted"));
    let one = persisted
        .entries
        .iter()
        .find(|entry| entry.id == id("one"))
        .unwrap_or_else(|| panic!("updated entry should remain persisted"));
    let four = persisted
        .entries
        .iter()
        .find(|entry| entry.id == id("four"))
        .unwrap_or_else(|| panic!("sibling should remain persisted"));
    assert_eq!(one.config.value, 3);
    assert_eq!(four.config.value, 4);
    assert_eq!(activations(&log, "one"), 2);
    assert_eq!(activations(&log, "four"), 1);
    assert_eq!(kernel.entry_fiber(&id("four")), Some(sibling));
}

pub async fn runtime_disposal() {
    let kernel = jinnd_adapter::kernel();
    let log = log();
    register(&kernel, &log);
    reconcile(
        &kernel,
        vec![entry("one", COUNT, 3), entry("four", COUNT, 4)],
    )
    .await;
    expect_ok(
        kernel
            .dispose_entry::<crate::loader_fixture::Config>(&id("one"))
            .await,
        "runtime disposal should settle",
    );

    let persisted = kernel
        .persisted_profile::<crate::loader_fixture::Config>()
        .unwrap_or_else(|| panic!("profile should be persisted"));
    let one = persisted
        .entries
        .iter()
        .find(|entry| entry.id == id("one"))
        .unwrap_or_else(|| panic!("disposed entry should remain persisted"));
    let four = persisted
        .entries
        .iter()
        .find(|entry| entry.id == id("four"))
        .unwrap_or_else(|| panic!("sibling should remain persisted"));
    assert!(one.disabled);
    assert_eq!(one.config.value, 3);
    assert!(!four.disabled);
    assert_eq!(activations(&log, "four"), 1);
    assert!(kernel.entry_fiber(&id("one")).is_none());
}
