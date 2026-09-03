//! Verifier-owned M2-K25 delivery cases. These drive the production daemon
//! and checked-in Tier A guest rather than the wasm host's private rig.

#![allow(dead_code)]

#[path = "string_lane_injects/fixture.rs"]
mod fixture;
#[path = "string_lane_injects/harness.rs"]
mod harness;
#[path = "string_lane_injects/ledger.rs"]
mod ledger;

use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use harness::{booted, entry, home, paths, reload, settle, state, until_loaded, until_state};
use jinnd_api::{DispatchMode, FiberState, LedgerEventKind};
use ledger::{COUNTER, errors, events, loads, transitions};

const TOPIC: &str = "jinn:test/topic";

fn bus_entry(id: &str, mode: &str) -> serde_json::Value {
    entry(id, serde_json::json!([TOPIC]), serde_json::json!([]), mode)
}

fn emitting(id: &str, mode: &str) -> serde_json::Value {
    bus_entry(id, mode)
}

fn trace(records: &[jinnd_api::LedgerRecord], entry: &str) -> Option<(u32, u32)> {
    records.iter().find_map(|record| {
        if !record
            .entry
            .as_ref()
            .is_some_and(|candidate| candidate.0 == entry)
        {
            return None;
        }
        match &record.kind {
            LedgerEventKind::DispatchTrace {
                topic,
                mode: DispatchMode::Emit,
                listeners,
                failures,
                ..
            } if topic == TOPIC => Some((*listeners, *failures)),
            _ => None,
        }
    })
}

fn failed_sequence(records: &[jinnd_api::LedgerRecord], entry: &str) -> u64 {
    records
        .iter()
        .find(|record| {
            record
                .entry
                .as_ref()
                .is_some_and(|candidate| candidate.0 == entry)
                && matches!(&record.kind, LedgerEventKind::FiberTransition(transition)
                    if transition.to == FiberState::Failed)
        })
        .map(|record| record.sequence)
        .unwrap_or_else(|| panic!("{entry} has a Failed row: {records:?}"))
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|address| address.port())
        .unwrap_or_else(|error| panic!("free port: {error}"))
}

fn connect_until(port: u16, wanted: bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let connected = TcpStream::connect(("127.0.0.1", port)).is_ok();
        if connected == wanted {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "port {port} connection state becomes {wanted}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_looping_listener_dies_on_its_own_slot_and_the_emitter_survives() {
    let test_home = home("m2-k25-loop");
    let initial = [
        bus_entry("listener", "listener-spin"),
        bus_entry("sibling", "listener"),
    ];
    let (daemon_paths, hash) = paths(&test_home, &initial);
    let daemon = booted(daemon_paths).await;
    until_state(&daemon, "listener", FiberState::Active).await;
    until_state(&daemon, "sibling", FiberState::Active).await;

    let walked = [
        bus_entry("listener", "listener-spin"),
        bus_entry("sibling", "listener"),
        emitting("emitter", "emitter"),
    ];
    reload(&daemon, &test_home, &walked, &hash).await;
    until_state(&daemon, "listener", FiberState::Failed).await;
    until_state(&daemon, "emitter", FiberState::Active).await;
    let records = events(&daemon).await;

    assert_eq!(trace(&records, "emitter"), Some((2, 1)));
    assert_eq!(state(&daemon, "sibling"), Some(FiberState::Active));
    assert!(
        errors(&records, "emitter").is_empty(),
        "emitter errors: {records:?}"
    );
    assert!(
        errors(&records, "listener")
            .iter()
            .any(|message| message.contains("deadline"))
    );
    let listener_path: Vec<_> = transitions(&records, "listener")
        .into_iter()
        .map(|transition| (transition.to, format!("{:?}", transition.cause)))
        .collect();
    assert!(
        listener_path.windows(2).any(|pair| {
            pair[0] == (FiberState::Unloading, "BodyFaulted".to_owned())
                && pair[1] == (FiberState::Failed, "BodyFaulted".to_owned())
        }),
        "listener transition path: {listener_path:?}"
    );

    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delivery_over_its_declared_fuel_budget_ends_deterministically() {
    let mut observed = Vec::new();
    for run in 0..3 {
        let test_home = home(&format!("m2-k25-fuel-{run}"));
        let initial = [
            bus_entry("budgeted", "listener-budget"),
            bus_entry("other", "listener"),
        ];
        let (daemon_paths, hash) = paths(&test_home, &initial);
        let daemon = booted(daemon_paths).await;
        until_state(&daemon, "budgeted", FiberState::Active).await;
        let walked = [
            bus_entry("budgeted", "listener-budget"),
            bus_entry("other", "listener"),
            emitting("emitter", "emitter"),
        ];
        let started = Instant::now();
        reload(&daemon, &test_home, &walked, &hash).await;
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "fuel bound was not prompt"
        );
        until_state(&daemon, "budgeted", FiberState::Failed).await;
        let records = events(&daemon).await;
        assert_eq!(trace(&records, "emitter"), Some((2, 1)));
        assert_eq!(state(&daemon, "other"), Some(FiberState::Active));
        let message = errors(&records, "budgeted")
            .into_iter()
            .find(|message| message.contains("fuel"))
            .unwrap_or_else(|| panic!("budget fault is recorded: {records:?}"));
        observed.push(message);
        daemon
            .shutdown()
            .await
            .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
    }
    assert!(
        observed.windows(2).all(|pair| pair[0] == pair[1]),
        "deterministic rows: {observed:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_emitter_is_charged_nothing_for_a_walk() {
    let test_home = home("m2-k25-emitter-clock");
    let mut initial: Vec<_> = (0..43)
        .map(|index| bus_entry(&format!("slow-{index}"), "listener-slow"))
        .collect();
    let (daemon_paths, hash) = paths(&test_home, &initial);
    let daemon = booted(daemon_paths).await;
    initial.push(emitting("emitter", "emitter-after-walk-spin"));
    reload(&daemon, &test_home, &initial, &hash).await;
    let records = events(&daemon).await;
    assert_eq!(
        trace(&records, "emitter"),
        Some((43, 0)),
        "the >5s walk completed: {records:?}"
    );
    until_state(&daemon, "emitter", FiberState::Failed).await;
    assert!(
        errors(&events(&daemon).await, "emitter")
            .iter()
            .any(|message| message.contains("deadline"))
    );
    for index in 0..43 {
        assert_eq!(
            state(&daemon, &format!("slow-{index}")),
            Some(FiberState::Active)
        );
    }
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dead_instance_releases_its_kernel_registrations() {
    let test_home = home("m2-k25-net-release");
    let port = free_port();
    let net_grant =
        serde_json::json!({ "contract": "jinn:net", "scope": { "bind": [port, port] } });
    let listener = entry(
        "listener",
        serde_json::json!([TOPIC, net_grant]),
        serde_json::json!([]),
        &format!("net-listener-spin:{port}"),
    );
    let initial = [listener.clone()];
    let (daemon_paths, hash) = paths(&test_home, &initial);
    let daemon = booted(daemon_paths).await;
    until_state(&daemon, "listener", FiberState::Active).await;
    connect_until(port, true);

    let walked = [listener, emitting("emitter", "emitter")];
    reload(&daemon, &test_home, &walked, &hash).await;
    until_state(&daemon, "listener", FiberState::Failed).await;
    connect_until(port, false);
    let records = events(&daemon).await;
    let failed = failed_sequence(&records, "listener");
    assert!(
        records.iter().any(|record| record.sequence > failed
            && record
                .entry
                .as_ref()
                .is_some_and(|entry| entry.0 == "listener")
            && matches!(record.kind, LedgerEventKind::NetClosed { .. })),
        "NetClosed follows Failed: {records:?}"
    );
    assert!(
        records.iter().any(|record| record.sequence > failed
            && record
                .entry
                .as_ref()
                .is_some_and(|entry| entry.0 == "listener")
            && matches!(
                record.kind,
                LedgerEventKind::EffectWithdrawn { clean: false, .. }
            )),
        "dead guest inverse is honestly unclean: {records:?}"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

async fn unbudgeted_case(
    name: &str,
    mode: &str,
) -> (FiberState, FiberState, Vec<jinnd_api::LedgerRecord>) {
    let test_home = home(name);
    let initial = [bus_entry("listener", mode)];
    let (daemon_paths, hash) = paths(&test_home, &initial);
    let daemon = booted(daemon_paths).await;
    until_state(&daemon, "listener", FiberState::Active).await;
    let walked = [bus_entry("listener", mode), emitting("emitter", "emitter")];
    reload(&daemon, &test_home, &walked, &hash).await;
    settle(&daemon).await;
    let answer = (
        state(&daemon, "listener").unwrap(),
        state(&daemon, "emitter").unwrap(),
        events(&daemon).await,
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
    answer
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unbudgeted_listen_is_unchanged_in_bound() {
    let (listener, emitter, records) =
        unbudgeted_case("m2-k25-four-seconds", "listener-delay:4000").await;
    assert_eq!(
        (listener, emitter),
        (FiberState::Active, FiberState::Active)
    );
    assert_eq!(trace(&records, "emitter"), Some((1, 0)));

    let (listener, emitter, records) =
        unbudgeted_case("m2-k25-six-seconds", "listener-delay:6000").await;
    assert_eq!(
        (listener, emitter),
        (FiberState::Failed, FiberState::Active)
    );
    assert_eq!(trace(&records, "emitter"), Some((1, 1)));
    assert!(
        errors(&records, "listener")
            .iter()
            .any(|message| message.contains("deadline"))
    );
    assert!(errors(&records, "emitter").is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_listener_is_not_retried_and_re_arms_only_when_declared_input_moves() {
    let test_home = home("m2-k25-rearm");
    let provider = entry(
        "provider",
        serde_json::json!([COUNTER]),
        serde_json::json!([]),
        "provider",
    );
    let listener = entry(
        "listener",
        serde_json::json!([TOPIC, COUNTER]),
        serde_json::json!([COUNTER]),
        "listener-spin",
    );
    let sibling = entry(
        "sibling",
        serde_json::json!([]),
        serde_json::json!([]),
        "plain",
    );
    let initial = [provider.clone(), listener.clone(), sibling.clone()];
    let (daemon_paths, hash) = paths(&test_home, &initial);
    let daemon = booted(daemon_paths).await;
    until_state(&daemon, "listener", FiberState::Active).await;
    let walked = [
        provider.clone(),
        listener.clone(),
        sibling.clone(),
        emitting("emitter", "emitter"),
    ];
    reload(&daemon, &test_home, &walked, &hash).await;
    until_state(&daemon, "listener", FiberState::Failed).await;
    assert_eq!(loads(&events(&daemon).await, "listener"), 1);

    let sibling_moved = [
        provider.clone(),
        listener.clone(),
        entry(
            "sibling",
            serde_json::json!([]),
            serde_json::json!([]),
            "plain:v2",
        ),
        emitting("emitter", "emitter"),
    ];
    reload(&daemon, &test_home, &sibling_moved, &hash).await;
    settle(&daemon).await;
    assert_eq!(state(&daemon, "listener"), Some(FiberState::Failed));
    assert_eq!(loads(&events(&daemon).await, "listener"), 1);

    let provider_moved = [
        entry(
            "provider",
            serde_json::json!([COUNTER]),
            serde_json::json!([]),
            "provider:v2",
        ),
        listener,
        entry(
            "sibling",
            serde_json::json!([]),
            serde_json::json!([]),
            "plain:v2",
        ),
        emitting("emitter", "emitter"),
    ];
    reload(&daemon, &test_home, &provider_moved, &hash).await;
    until_loaded(&daemon, "listener", 2).await;
    until_state(&daemon, "listener", FiberState::Active).await;
    assert_eq!(loads(&events(&daemon).await, "listener"), 2);
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_zero_budget_is_refused_at_listen_on_the_record() {
    let test_home = home("m2-k25-zero");
    let listener = bus_entry("listener", "listener-zero-with-existing");
    let sibling = bus_entry("sibling", "listener");
    let initial = [listener.clone(), sibling.clone()];
    let (daemon_paths, hash) = paths(&test_home, &initial);
    let daemon = booted(daemon_paths).await;
    until_state(&daemon, "listener", FiberState::Active).await;
    let walked = [listener, sibling, emitting("emitter", "emitter")];
    reload(&daemon, &test_home, &walked, &hash).await;
    let records = events(&daemon).await;
    assert!(
        records.iter().any(|record| {
            record
                .entry
                .as_ref()
                .is_some_and(|entry| entry.0 == "listener")
                && matches!(&record.kind, LedgerEventKind::ErrorRecorded { error }
                if format!("{:?}", error.code) == "InvalidProfile")
        }),
        "zero refusal is typed and attributed: {records:?}"
    );
    assert_eq!(
        trace(&records, "emitter"),
        Some((2, 0)),
        "the listener's prior registration and sibling stand"
    );
    assert_eq!(state(&daemon, "listener"), Some(FiberState::Active));
    assert_eq!(state(&daemon, "sibling"), Some(FiberState::Active));
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}
