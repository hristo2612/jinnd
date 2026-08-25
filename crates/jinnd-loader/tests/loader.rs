//! End-to-end loader behavior over real fibers: reconcile-by-id, bidirectional
//! persistence, contained per-entry faults (R1, R5, R11; I1/I4 seeds).

mod common;

use common::Grab;
use common::{activations, deactivations, entry, fixture, id, profile};
use jinnd_api::{ErrorCode, FiberState};

#[tokio::test(flavor = "current_thread")]
async fn initial_reconcile_activates_only_effectively_enabled_entries() {
    let (loader, _registry, log) = fixture();

    let mut bar = entry("bar", "test/count", 2);
    bar.parent = Some(id("group"));
    let mut qux = entry("qux", "test/count", 4);
    qux.disabled = true;
    qux.parent = Some(id("group"));
    let document = profile(vec![
        entry("foo", "test/count", 1),
        entry("group", jinnd_api::GROUP_PACKAGE, 0),
        bar,
        qux,
    ]);

    let report = loader.reconcile(document).await.grab();
    loader.quiesce().await;

    assert_eq!(report.created, vec![id("foo"), id("bar")]);
    assert!(report.errors.is_empty());
    assert_eq!(activations(&log, "foo"), 1);
    assert_eq!(activations(&log, "bar"), 1);
    assert_eq!(activations(&log, "qux"), 0);
    assert!(loader.entry_fiber(&id("foo")).is_some());
    assert!(loader.entry_fiber(&id("qux")).is_none());
    // Entry ids are retained in the persisted document.
    let persisted = loader.persisted::<u32>().grab();
    assert_eq!(persisted.entries.len(), 4);
}

#[tokio::test(flavor = "current_thread")]
async fn reconcile_by_id_swaps_only_affected_entries() {
    let (loader, _registry, log) = fixture();

    let mut qux = entry("qux", "test/count", 4);
    qux.disabled = true;
    let initial = profile(vec![
        entry("foo", "test/count", 1),
        entry("bar", "test/count", 2),
        qux,
    ]);
    loader.reconcile(initial).await.grab();
    loader.quiesce().await;
    let foo_fiber = loader.entry_fiber(&id("foo")).grab();

    // Final profile: foo unchanged, bar gone, qux enabled.
    let final_profile = profile(vec![
        entry("foo", "test/count", 1),
        entry("qux", "test/count", 4),
    ]);
    let report = loader.reconcile(final_profile).await.grab();
    loader.quiesce().await;

    assert_eq!(report.unchanged, vec![id("foo")]);
    assert_eq!(report.disposed, vec![id("bar")]);
    assert_eq!(report.created, vec![id("qux")]);
    // foo did not restart: same fiber uid, one activation ever.
    assert_eq!(loader.entry_fiber(&id("foo")).grab(), foo_fiber);
    assert_eq!(activations(&log, "foo"), 1);
    assert_eq!(deactivations(&log, "bar"), 1);
    assert_eq!(activations(&log, "qux"), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn config_update_restarts_one_fiber_and_writes_back() {
    let (loader, _registry, log) = fixture();
    loader
        .reconcile(profile(vec![
            entry("one", "test/count", 1),
            entry("four", "test/count", 4),
        ]))
        .await
        .grab();
    loader.quiesce().await;
    let sibling = loader.entry_fiber(&id("four")).grab();

    loader.update_entry(&id("one"), 3u32).await.grab();
    loader.quiesce().await;

    // The runtime change wrote back to the matching persisted entry only.
    let persisted = loader.persisted::<u32>().grab();
    let one = persisted.entries.iter().find(|e| e.id == id("one")).grab();
    let four = persisted.entries.iter().find(|e| e.id == id("four")).grab();
    assert_eq!(one.config, 3);
    assert_eq!(four.config, 4);
    // The updated fiber reloaded with the new config; the sibling did not move.
    assert_eq!(activations(&log, "one"), 2);
    assert_eq!(activations(&log, "four"), 1);
    assert_eq!(loader.entry_fiber(&id("four")).grab(), sibling);
}

#[tokio::test(flavor = "current_thread")]
async fn runtime_disposal_writes_disabled_back_and_keeps_config() {
    let (loader, _registry, log) = fixture();
    loader
        .reconcile(profile(vec![
            entry("one", "test/count", 3),
            entry("four", "test/count", 4),
        ]))
        .await
        .grab();
    loader.quiesce().await;

    loader.dispose_entry::<u32>(&id("one")).await.grab();

    let persisted = loader.persisted::<u32>().grab();
    let one = persisted.entries.iter().find(|e| e.id == id("one")).grab();
    let four = persisted.entries.iter().find(|e| e.id == id("four")).grab();
    assert!(one.disabled);
    assert_eq!(one.config, 3);
    assert!(!four.disabled);
    assert_eq!(deactivations(&log, "one"), 1);
    assert_eq!(activations(&log, "four"), 1);
    assert!(loader.entry_fiber(&id("one")).is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn group_disable_enable_drives_exactly_the_effective_subtree() {
    let (loader, _registry, log) = fixture();

    let outer = entry("outer", jinnd_api::GROUP_PACKAGE, 0);
    let mut inner = entry("inner", jinnd_api::GROUP_PACKAGE, 0);
    inner.parent = Some(id("outer"));
    let mut outer_child = entry("outer-child", "test/count", 1);
    outer_child.parent = Some(id("outer"));
    let mut inner_child = entry("inner-child", "test/count", 2);
    inner_child.parent = Some(id("inner"));

    loader
        .reconcile(profile(vec![
            outer.clone(),
            inner.clone(),
            outer_child.clone(),
            inner_child.clone(),
        ]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(activations(&log, "outer-child"), 1);
    assert_eq!(activations(&log, "inner-child"), 1);

    // Disable inner: only the inner subtree disposes.
    let mut disabled_inner = inner.clone();
    disabled_inner.disabled = true;
    loader
        .reconcile(profile(vec![
            outer.clone(),
            disabled_inner.clone(),
            outer_child.clone(),
            inner_child.clone(),
        ]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(deactivations(&log, "inner-child"), 1);
    assert_eq!(deactivations(&log, "outer-child"), 0);

    // Disable outer: the remaining enabled subtree disposes once.
    let mut disabled_outer = outer.clone();
    disabled_outer.disabled = true;
    loader
        .reconcile(profile(vec![
            disabled_outer.clone(),
            disabled_inner.clone(),
            outer_child.clone(),
            inner_child.clone(),
        ]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(deactivations(&log, "outer-child"), 1);
    assert_eq!(deactivations(&log, "inner-child"), 1);

    // Enable inner under the disabled outer: nothing activates.
    loader
        .reconcile(profile(vec![
            disabled_outer,
            inner.clone(),
            outer_child.clone(),
            inner_child.clone(),
        ]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(activations(&log, "inner-child"), 1);

    // Enable outer: both children activate exactly once more.
    loader
        .reconcile(profile(vec![outer, inner, outer_child, inner_child]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(activations(&log, "outer-child"), 2);
    assert_eq!(activations(&log, "inner-child"), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn unregistered_package_is_a_contained_per_entry_fault() {
    let (loader, _registry, log) = fixture();
    let report = loader
        .reconcile(profile(vec![
            entry("good", "test/count", 1),
            entry("bad", "test/unknown", 2),
        ]))
        .await
        .grab();
    loader.quiesce().await;

    assert_eq!(activations(&log, "good"), 1);
    assert_eq!(activations(&log, "bad"), 0);
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].entry, id("bad"));
    assert_eq!(report.errors[0].error.code, ErrorCode::InvalidProfile);
    assert!(loader.entry_fiber(&id("bad")).is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn moving_an_entry_between_enabled_parents_preserves_its_activation() {
    let (loader, _registry, log) = fixture();
    let alpha = entry("alpha", jinnd_api::GROUP_PACKAGE, 0);
    let plugin = entry("plugin", "test/count", 1);
    loader
        .reconcile(profile(vec![alpha.clone(), plugin.clone()]))
        .await
        .grab();
    loader.quiesce().await;
    let fiber = loader.entry_fiber(&id("plugin")).grab();

    let mut moved = plugin.clone();
    moved.parent = Some(id("alpha"));
    loader
        .reconcile(profile(vec![alpha.clone(), moved.clone()]))
        .await
        .grab();
    loader.quiesce().await;

    // Same fiber, no extra activation or disposal (epoch identity coalesced).
    assert_eq!(loader.entry_fiber(&id("plugin")).grab(), fiber);
    assert_eq!(activations(&log, "plugin"), 1);
    assert_eq!(deactivations(&log, "plugin"), 0);

    // Moving into a disabled parent disposes once and stays addressable.
    let mut beta = entry("beta", jinnd_api::GROUP_PACKAGE, 0);
    beta.disabled = true;
    let mut into_beta = plugin.clone();
    into_beta.parent = Some(id("beta"));
    loader
        .reconcile(profile(vec![alpha, beta, into_beta]))
        .await
        .grab();
    loader.quiesce().await;
    assert_eq!(deactivations(&log, "plugin"), 1);
    assert!(loader.entry_fiber(&id("plugin")).is_none());
    let persisted = loader.persisted::<u32>().grab();
    assert!(persisted.entries.iter().any(|e| e.id == id("plugin")));
}

#[tokio::test(flavor = "current_thread")]
async fn pending_consumer_waits_without_failing() {
    let (loader, _registry, log) = fixture();
    loader
        .reconcile(profile(vec![entry("watcher", "test/consumer", 0)]))
        .await
        .grab();
    loader.quiesce().await;

    let fiber = loader.entry_fiber(&id("watcher")).grab();
    assert_eq!(loader.fiber_state(fiber), Some(FiberState::Pending));
    assert_eq!(activations(&log, "watcher"), 0);
}
