use std::sync::Arc;

use crate::harness::{
    booted, bystander, declared, entry, home, paths, provider, reload, settle, slow_provider,
    state, undeclared, until_loaded, until_state, witness_gate, write_profile,
};
use crate::ledger::{COUNTER, active_sequence, calls, errors, events, failed, loads};
use jinnd_api::FiberState;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declared_consumer_rests_pending_until_every_declared_provider_is_active() {
    // Consumer first is forced by booting it alone. The provider's
    // provision lands while it is still Loading; the consumer must wait.
    let consumer_home = home("consumer-first");
    let alone = [declared("consumer", "inject-counter")];
    let (consumer_paths, hash) = paths(&consumer_home, &alone);
    let daemon = Arc::new(booted(consumer_paths).await);
    settle(&daemon).await;
    assert_eq!(state(&daemon, "consumer"), Some(FiberState::Pending));
    assert_eq!(loads(&events(&daemon).await, "consumer"), 0);
    let both = [
        declared("consumer", "inject-counter"),
        slow_provider("provider"),
    ];
    write_profile(&consumer_home, &both, &hash);
    let reloading = tokio::spawn({
        let daemon = Arc::clone(&daemon);
        async move { daemon.reload().await }
    });
    assert!(witness_gate(&daemon, "provider", "consumer").await > 0);
    let report = reloading
        .await
        .unwrap_or_else(|error| panic!("reload task: {error}"))
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    assert!(
        report.errors.is_empty(),
        "clean reload: {:?}",
        report.errors
    );
    until_state(&daemon, "consumer", FiberState::Active).await;
    let records = events(&daemon).await;
    assert_eq!(loads(&records, "consumer"), 1);
    assert_eq!(calls(&records, "consumer", "get"), 1);
    assert!(!failed(&records, "consumer"));
    assert!(errors(&records, "consumer").is_empty());
    // Cross-fiber transition rows are committed in sync-batch order, not
    // causality order. `witness_gate` is the causal proof: it observes the
    // provider's Loading window and requires zero consumer loads there.
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));

    // Provider first is forced by booting it alone, then adding the
    // consumer. The one consumer activation starts after provider Active.
    let provider_home = home("provider-first");
    let first = [provider("provider")];
    let (provider_paths, hash) = paths(&provider_home, &first);
    let daemon = booted(provider_paths).await;
    until_state(&daemon, "provider", FiberState::Active).await;
    let both = [provider("provider"), declared("consumer", "inject-counter")];
    reload(&daemon, &provider_home, &both, &hash).await;
    until_state(&daemon, "consumer", FiberState::Active).await;
    let records = events(&daemon).await;
    assert_eq!(loads(&records, "consumer"), 1);
    assert!(!failed(&records, "consumer"));
    assert!(active_sequence(&records, "provider") < active_sequence(&records, "consumer"));
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_undeclared_entry_is_unchanged_by_this_packet() {
    let home = home("undeclared");
    let alone = [undeclared("legacy", "inject-counter")];
    let (paths, hash) = paths(&home, &alone);
    let daemon = booted(paths).await;
    until_state(&daemon, "legacy", FiberState::Failed).await;
    let records = events(&daemon).await;
    assert_eq!(loads(&records, "legacy"), 1);
    assert_eq!(calls(&records, "legacy", "get"), 1);

    let with_provider = [undeclared("legacy", "inject-counter"), provider("provider")];
    reload(&daemon, &home, &with_provider, &hash).await;
    until_state(&daemon, "provider", FiberState::Active).await;
    settle(&daemon).await;
    assert_eq!(state(&daemon, "legacy"), Some(FiberState::Failed));
    assert_eq!(loads(&events(&daemon).await, "legacy"), 1);

    let with_holder = [
        undeclared("legacy", "inject-counter"),
        provider("provider"),
        undeclared("holder", "inject-counter"),
    ];
    reload(&daemon, &home, &with_holder, &hash).await;
    until_state(&daemon, "holder", FiberState::Active).await;
    let replaced = [
        undeclared("legacy", "inject-counter"),
        entry(
            "provider",
            serde_json::json!([COUNTER]),
            serde_json::json!([]),
            "provider:v2",
        ),
        undeclared("holder", "inject-counter"),
    ];
    reload(&daemon, &home, &replaced, &hash).await;
    until_loaded(&daemon, "provider", 2).await;
    settle(&daemon).await;
    assert_eq!(loads(&events(&daemon).await, "holder"), 1);
    assert_eq!(state(&daemon, "holder"), Some(FiberState::Active));
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declaration_without_a_grant_is_a_contained_entry_fault() {
    let home = home("ungranted");
    let entries = [
        provider("provider"),
        entry(
            "ungranted",
            serde_json::json!([]),
            serde_json::json!([COUNTER]),
            "inject-counter",
        ),
        bystander("sibling", "plain"),
    ];
    let (paths, _) = paths(&home, &entries);
    let daemon = booted(paths).await;
    until_state(&daemon, "ungranted", FiberState::Failed).await;
    until_state(&daemon, "sibling", FiberState::Active).await;
    let records = events(&daemon).await;
    assert_eq!(
        calls(&records, "ungranted", "get"),
        0,
        "the guest body made no call and nothing was widened"
    );
    assert!(
        errors(&records, "ungranted")
            .iter()
            .any(|message| message.contains("declared but not granted")),
        "the refusal is on the entry's record"
    );
    assert_eq!(state(&daemon, "provider"), Some(FiberState::Active));
    assert_eq!(state(&daemon, "sibling"), Some(FiberState::Active));
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}
