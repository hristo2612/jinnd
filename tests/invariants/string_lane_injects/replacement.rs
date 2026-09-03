use std::sync::{Arc, Mutex};

use jinnd_api::{ErrorCode, FiberId, FiberState, KernelFuture, LedgerEventKind, TransitionCause};
use jinnd_wasm::{Broker, LedgerSink, Peer, PeerId};

use crate::harness::{
    booted, bystander, declared, entry, home, paths, provider, reload, settle, state, until_loaded,
    until_state,
};
use crate::ledger::{COUNTER, calls, events, failed, loads, transitions};

#[derive(Default)]
struct Captured(Mutex<Vec<LedgerEventKind>>);

impl LedgerSink for Captured {
    fn append(&self, kind: LedgerEventKind, _fiber: Option<FiberId>) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(kind);
    }
}

struct Answer(&'static [u8]);

impl Peer for Answer {
    fn call(
        &self,
        _caller: PeerId,
        _contract: &str,
        _operation: &str,
        _payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let answer = self.0.to_vec();
        Box::pin(async move { Ok(answer) })
    }
}

async fn old_consumer_handle_is_refused_after_provider_replacement() {
    let ledger = Arc::new(Captured::default());
    let broker = Broker::new(ledger.clone());
    let consumer = broker.register_peer(Some(FiberId(41)));
    let first = broker.register_peer(Some(FiberId(42)));
    broker.grant(consumer, COUNTER);
    broker.grant(first, COUNTER);
    broker
        .provide(first, COUNTER, Arc::new(Answer(b"old")))
        .unwrap_or_else(|error| panic!("first provision: {error:?}"));
    let retained = broker
        .resolve(consumer, COUNTER)
        .unwrap_or_else(|error| panic!("consumer resolve: {error:?}"));
    broker.withdraw(first, COUNTER);
    let second = broker.register_peer(Some(FiberId(43)));
    broker.grant(second, COUNTER);
    broker
        .provide(second, COUNTER, Arc::new(Answer(b"new")))
        .unwrap_or_else(|error| panic!("replacement provision: {error:?}"));
    let refused = broker
        .call(consumer, retained, "get", Vec::new())
        .await
        .expect_err("the old consumer handle is never retargeted");
    assert_eq!(refused.code, ErrorCode::EffectFailed);
    assert!(refused.message.contains("stale handle"));
    assert!(
        ledger
            .0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .any(|kind| matches!(kind, LedgerEventKind::StaleHandleRefused { contract } if contract == COUNTER))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacing_a_declared_provider_reloads_its_consumer_exactly_once_and_no_sibling() {
    let home = home("replace");
    let entries = [
        provider("provider"),
        declared("consumer", "inject-counter"),
        bystander("sibling", "plain"),
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
        bystander("sibling", "plain"),
    ];
    reload(&daemon, &home, &replaced, &hash).await;
    until_loaded(&daemon, "consumer", 2).await;
    until_state(&daemon, "consumer", FiberState::Active).await;
    settle(&daemon).await;
    let records = events(&daemon).await;
    assert_eq!(loads(&records, "consumer"), 2, "incarnation +1 exactly");
    assert_eq!(calls(&records, "consumer", "get"), 2);
    assert_eq!(loads(&records, "sibling"), 1);
    let unload = transitions(&records, "consumer")
        .into_iter()
        .find(|transition| transition.to == FiberState::Unloading)
        .unwrap_or_else(|| panic!("the consumer unloaded"));
    assert_eq!(unload.from, FiberState::Active);
    assert_eq!(unload.cause, TransitionCause::DependencyChanged);
    old_consumer_handle_is_refused_after_provider_replacement().await;
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_consumer_re_arms_when_a_declared_provider_moves_and_never_before() {
    let home = home("rearm");
    let entries = [
        provider("provider"),
        declared("consumer", "inject-counter-bad"),
        bystander("sibling", "plain"),
    ];
    let (paths, hash) = paths(&home, &entries);
    let daemon = booted(paths).await;
    until_state(&daemon, "consumer", FiberState::Failed).await;
    assert_eq!(loads(&events(&daemon).await, "consumer"), 1);

    // An unchanged reconcile represents the same provider identity; an
    // unrelated sibling restart likewise does not move the declaration.
    reload(&daemon, &home, &entries, &hash).await;
    let sibling_changed = [
        provider("provider"),
        declared("consumer", "inject-counter-bad"),
        bystander("sibling", "plain:v2"),
    ];
    reload(&daemon, &home, &sibling_changed, &hash).await;
    until_loaded(&daemon, "sibling", 2).await;
    settle(&daemon).await;
    assert_eq!(state(&daemon, "consumer"), Some(FiberState::Failed));
    assert_eq!(loads(&events(&daemon).await, "consumer"), 1);

    let moved = [
        entry(
            "provider",
            serde_json::json!([COUNTER]),
            serde_json::json!([]),
            "provider:v2",
        ),
        declared("consumer", "inject-counter-bad"),
        bystander("sibling", "plain:v2"),
    ];
    reload(&daemon, &home, &moved, &hash).await;
    until_loaded(&daemon, "consumer", 2).await;
    until_state(&daemon, "consumer", FiberState::Failed).await;
    settle(&daemon).await;
    let records = events(&daemon).await;
    assert_eq!(loads(&records, "consumer"), 2);
    let rearm = transitions(&records, "consumer")
        .into_iter()
        .filter(|transition| transition.to == FiberState::Loading)
        .nth(1)
        .unwrap_or_else(|| panic!("the second activation is recorded"));
    assert_eq!(rearm.cause, TransitionCause::DependencyChanged);
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

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
    assert_eq!(loads(&records, "consumer"), 1);
    assert!(!failed(&records, "consumer"));
    assert!(transitions(&records, "consumer").iter().any(|transition| {
        transition.to == FiberState::Unloading
            && transition.cause == TransitionCause::DependencyChanged
    }));
    reload(&daemon, &home, &entries, &hash).await;
    until_state(&daemon, "consumer", FiberState::Active).await;
    assert_eq!(loads(&events(&daemon).await, "consumer"), 2);
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}
