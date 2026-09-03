//! (a), the ratchet, and the contained fault: what a declaration gates,
//! what its absence leaves exactly as it was, and what a bad one costs.

use std::sync::Arc;

use jinnd_api::FiberState;
use jinnd_daemon::Daemon;

use crate::harness::{
    booted, bystander, declared, entry, home, paths, provider, reload, settle, slow_provider,
    state, undeclared, until_loaded, until_state, witness_gate, write_profile,
};
use crate::ledger::{COUNTER, calls, errors, events, failed, loads};

/// The consumer-first order, FORCED: the consumer boots ALONE, so no
/// scheduling luck can put the provider first. It rests `Pending` — no
/// `Failed`, no crossing, no `missing-dependency` — until the provider
/// lands by profile edit; while that provider is provided but still
/// `Loading` it keeps waiting (provision is not readiness, #45 seq 44 vs
/// 76); then it activates exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declared_consumer_rests_pending_until_its_provider_is_active_consumer_first() {
    let home = home("consumer-first");
    let alone = [declared("consumer", "inject-counter")];
    let (paths, hash) = paths(&home, &alone);
    let daemon = Arc::new(booted(paths).await);
    settle(&daemon).await;
    let records = events(&daemon).await;
    assert_eq!(
        state(&daemon, "consumer"),
        Some(FiberState::Pending),
        "the declared consumer waits instead of failing"
    );
    assert_eq!(
        loads(&records, "consumer"),
        0,
        "no activation was attempted"
    );
    assert_eq!(
        calls(&records, "consumer", "get"),
        0,
        "no crossing met an absent provider"
    );
    assert!(!failed(&records, "consumer"), "never Failed");
    // The environment moves: a slow provider lands. The reload runs beside
    // the witness so the provided-but-Loading window is observed.
    let both = [
        declared("consumer", "inject-counter"),
        slow_provider("provider"),
    ];
    write_profile(&home, &both, &hash);
    let reloading = tokio::spawn({
        let daemon = Arc::clone(&daemon);
        async move { daemon.reload().await }
    });
    let observed = witness_gate(&daemon, "provider", "consumer").await;
    assert!(
        observed > 0,
        "the provided-but-Loading window was exercised"
    );
    let report = reloading
        .await
        .unwrap_or_else(|error| panic!("{error}"))
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    assert!(
        report.errors.is_empty(),
        "clean reload: {:?}",
        report.errors
    );
    until_state(&daemon, "consumer", FiberState::Active).await;
    let records = events(&daemon).await;
    assert_eq!(
        loads(&records, "consumer"),
        1,
        "exactly one consumer activation"
    );
    assert!(
        !failed(&records, "consumer"),
        "the consumer never rested Failed"
    );
    assert_eq!(
        calls(&records, "consumer", "get"),
        1,
        "one crossing, answered"
    );
    assert!(
        errors(&records, "consumer").is_empty(),
        "no error on the consumer's record"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
}

/// Both document orders of ONE boot, witnessed DURING the boot: whichever
/// the loader spawns first, the consumer does not load while the slow
/// provider is provided but `Loading`, and nothing fails. The
/// kernel-supplied `jinn:fs` is declared too: trivially ready.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declared_consumer_activates_after_its_provider_in_either_document_order() {
    for (name, first) in [("provider-first", true), ("consumer-last", false)] {
        let home = home(name);
        let consumer = entry(
            "consumer",
            serde_json::json!([COUNTER, "jinn:fs"]),
            serde_json::json!(["jinn:fs", COUNTER]),
            "inject-counter",
        );
        let entries = if first {
            [slow_provider("provider"), consumer]
        } else {
            [consumer, slow_provider("provider")]
        };
        let (paths, _) = paths(&home, &entries);
        let daemon =
            Arc::new(Daemon::open(paths).unwrap_or_else(|error| panic!("open: {error:?}")));
        let booting = tokio::spawn({
            let daemon = Arc::clone(&daemon);
            async move { daemon.boot().await }
        });
        let observed = witness_gate(&daemon, "provider", "consumer").await;
        assert!(
            observed > 0,
            "{name}: the provided-but-Loading window was exercised"
        );
        let report = booting
            .await
            .unwrap_or_else(|error| panic!("{error}"))
            .unwrap_or_else(|error| panic!("boot: {error:?}"));
        assert!(
            report.errors.is_empty(),
            "{name}: clean boot: {:?}",
            report.errors
        );
        until_state(&daemon, "consumer", FiberState::Active).await;
        let records = events(&daemon).await;
        assert_eq!(
            loads(&records, "consumer"),
            1,
            "{name}: one consumer activation"
        );
        assert!(!failed(&records, "consumer"), "{name}: never Failed");
        assert_eq!(
            calls(&records, "consumer", "get"),
            1,
            "{name}: one answered crossing"
        );
        daemon
            .shutdown()
            .await
            .unwrap_or_else(|error| panic!("{error:?}"));
    }
}

/// The ratchet: an entry with no `injects` is exactly today's — the
/// resolve answers from the grant, the first call meets no provider and
/// fails the activation, the fiber rests `Failed`, and a provider landing
/// later re-arms nothing (R9). Its `Active` sibling is untouched by a
/// provider replacement, as at `d7440e2`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_undeclared_entry_is_unchanged_by_this_packet() {
    let home = home("undeclared");
    let alone = [undeclared("legacy", "inject-counter")];
    let (paths, hash) = paths(&home, &alone);
    let daemon = booted(paths).await;
    until_state(&daemon, "legacy", FiberState::Failed).await;
    let records = events(&daemon).await;
    assert_eq!(loads(&records, "legacy"), 1);
    assert_eq!(
        calls(&records, "legacy", "get"),
        1,
        "the first call met no provider"
    );
    // The provider lands: nothing re-arms an undeclared Failed fiber.
    let both = [undeclared("legacy", "inject-counter"), provider("provider")];
    reload(&daemon, &home, &both, &hash).await;
    until_state(&daemon, "provider", FiberState::Active).await;
    settle(&daemon).await;
    assert_eq!(state(&daemon, "legacy"), Some(FiberState::Failed));
    assert_eq!(
        loads(&events(&daemon).await, "legacy"),
        1,
        "no retry without a declaration"
    );
    // An undeclared holder arriving AGAINST a live provider (the order
    // forced by two edits, as today's profiles must) activates today.
    let held = [
        undeclared("legacy", "inject-counter"),
        provider("provider"),
        undeclared("holder", "inject-counter"),
    ];
    reload(&daemon, &home, &held, &hash).await;
    until_state(&daemon, "holder", FiberState::Active).await;
    // The provider is replaced: an undeclared Active holder is untouched.
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
    let records = events(&daemon).await;
    assert_eq!(
        loads(&records, "holder"),
        1,
        "an undeclared holder does not reload"
    );
    assert_eq!(state(&daemon, "holder"), Some(FiberState::Active));
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
}

/// A declaration the entry holds no grant for, and one that is not a
/// contract name at all: each is a per-entry fault refused ON THE RECORD
/// at admission — the entry loads nothing (no crossing of its own), its
/// siblings load normally, and nothing is widened (R11, constitution 01).
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
        entry(
            "malformed",
            serde_json::json!([COUNTER]),
            serde_json::json!([{ "scope": 7 }]),
            "inject-counter",
        ),
        bystander("bystander", "plain"),
    ];
    let (paths, _) = paths(&home, &entries);
    let daemon = booted(paths).await;
    until_state(&daemon, "ungranted", FiberState::Failed).await;
    until_state(&daemon, "malformed", FiberState::Failed).await;
    until_state(&daemon, "bystander", FiberState::Active).await;
    let records = events(&daemon).await;
    for faulted in ["ungranted", "malformed"] {
        let recorded = errors(&records, faulted);
        assert!(
            recorded
                .iter()
                .any(|message| message.contains(COUNTER) || message.contains("scope")),
            "{faulted}: the refusal names what was refused: {recorded:?}"
        );
        assert_eq!(
            calls(&records, faulted, "get"),
            0,
            "{faulted} loaded nothing"
        );
    }
    assert_eq!(state(&daemon, "provider"), Some(FiberState::Active));
    assert!(!failed(&records, "bystander") && !failed(&records, "provider"));
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
}
