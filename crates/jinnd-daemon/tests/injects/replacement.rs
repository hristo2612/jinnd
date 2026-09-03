//! (b), (c), and withdrawal: what a declared provider's movement does to
//! its consumer — and what it does NOT do to anyone else.

use jinnd_api::{FiberState, TransitionCause};

use crate::harness::{
    COUNTER, booted, bystander, calls, declared, entry, events, failed, home, loads, paths,
    provider, reload, settle, state, transitions, until_loaded, until_state,
};

/// #46: replacing the provider (a config edit that restarts its entry)
/// reloads the declared consumer EXACTLY once, `Active → Unloading` under
/// `DependencyChanged`, back to `Active` against the new generation — one
/// crossing per incarnation; an undeclared sibling's incarnation is
/// unchanged (I1, R11).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacing_a_declared_provider_reloads_its_consumer_exactly_once_and_no_sibling() {
    let home = home("replace");
    let entries = [
        provider("provider"),
        declared("consumer", "inject-counter"),
        bystander("bystander", "plain"),
    ];
    let (paths, hash) = paths(&home, &entries);
    let daemon = booted(paths).await;
    until_state(&daemon, "consumer", FiberState::Active).await;
    let replaced = [
        entry(
            "provider",
            serde_json::json!([COUNTER]),
            serde_json::json!([]),
            "provider:v2",
        ),
        declared("consumer", "inject-counter"),
        bystander("bystander", "plain"),
    ];
    reload(&daemon, &home, &replaced, &hash).await;
    until_loaded(&daemon, "consumer", 2).await;
    until_state(&daemon, "consumer", FiberState::Active).await;
    settle(&daemon).await;
    let records = events(&daemon).await;
    assert_eq!(loads(&records, "consumer"), 2, "one reload, no more");
    assert!(!failed(&records, "consumer"));
    assert_eq!(
        calls(&records, "consumer", "get"),
        2,
        "one crossing per incarnation"
    );
    let unload = transitions(&records, "consumer")
        .into_iter()
        .find(|transition| transition.to == FiberState::Unloading)
        .unwrap_or_else(|| panic!("the consumer unloaded"));
    assert_eq!(unload.from, FiberState::Active);
    assert_eq!(unload.cause, TransitionCause::DependencyChanged);
    assert_eq!(
        loads(&records, "bystander"),
        1,
        "the undeclared sibling never moved"
    );
    assert_eq!(loads(&records, "provider"), 2);
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
}

/// Ruling 2 on the string lane: a consumer that failed ON ITS OWN ACCOUNT
/// against a live provider rests `Failed` across an unrelated sibling's
/// restart, and re-arms exactly once — under `DependencyChanged` — when
/// the declared provider's generation moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_consumer_re_arms_when_a_declared_provider_moves_and_never_before() {
    let home = home("re-arm");
    let entries = [
        provider("provider"),
        declared("consumer", "inject-counter-bad"),
        bystander("bystander", "plain"),
    ];
    let (paths, hash) = paths(&home, &entries);
    let daemon = booted(paths).await;
    until_state(&daemon, "consumer", FiberState::Failed).await;
    assert_eq!(loads(&events(&daemon).await, "consumer"), 1);
    // An unrelated sibling restarts: nothing declared moved.
    let sibling = [
        provider("provider"),
        declared("consumer", "inject-counter-bad"),
        bystander("bystander", "plain:v2"),
    ];
    reload(&daemon, &home, &sibling, &hash).await;
    until_loaded(&daemon, "bystander", 2).await;
    settle(&daemon).await;
    assert_eq!(state(&daemon, "consumer"), Some(FiberState::Failed));
    assert_eq!(loads(&events(&daemon).await, "consumer"), 1, "never before");
    // The declared provider moves: the failure is retried once, against a
    // CHANGED environment.
    let moved = [
        entry(
            "provider",
            serde_json::json!([COUNTER]),
            serde_json::json!([]),
            "provider:v2",
        ),
        declared("consumer", "inject-counter-bad"),
        bystander("bystander", "plain:v2"),
    ];
    reload(&daemon, &home, &moved, &hash).await;
    until_loaded(&daemon, "consumer", 2).await;
    until_state(&daemon, "consumer", FiberState::Failed).await;
    settle(&daemon).await;
    let records = events(&daemon).await;
    assert_eq!(loads(&records, "consumer"), 2, "re-armed exactly once");
    let re_arm = transitions(&records, "consumer")
        .into_iter()
        .filter(|transition| transition.to == FiberState::Loading)
        .nth(1)
        .unwrap_or_else(|| panic!("a second load"));
    assert_eq!(re_arm.cause, TransitionCause::DependencyChanged);
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
}

/// A provider withdrawn with no successor takes the epoch to `None`: the
/// consumer unloads cleanly to `Pending` and WAITS — never `Failed`, no
/// activation attempted — and a later provision reactivates it (I3).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_provider_withdrawn_without_successor_parks_its_consumer_pending() {
    let home = home("withdrawn");
    let entries = [provider("provider"), declared("consumer", "inject-counter")];
    let (paths, hash) = paths(&home, &entries);
    let daemon = booted(paths).await;
    until_state(&daemon, "consumer", FiberState::Active).await;
    let gone = [declared("consumer", "inject-counter")];
    reload(&daemon, &home, &gone, &hash).await;
    until_state(&daemon, "consumer", FiberState::Pending).await;
    settle(&daemon).await;
    let records = events(&daemon).await;
    assert_eq!(
        loads(&records, "consumer"),
        1,
        "nothing was attempted against nothing"
    );
    assert!(!failed(&records, "consumer"));
    let unload = transitions(&records, "consumer")
        .into_iter()
        .find(|transition| transition.to == FiberState::Unloading)
        .unwrap_or_else(|| panic!("the consumer unloaded"));
    assert_eq!(unload.cause, TransitionCause::DependencyChanged);
    reload(&daemon, &home, &entries, &hash).await;
    until_state(&daemon, "consumer", FiberState::Active).await;
    assert_eq!(
        loads(&events(&daemon).await, "consumer"),
        2,
        "reactivated once"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
}
