use std::path::PathBuf;
use std::time::{Duration, Instant};

use jinnd_api::FiberState;
use jinnd_daemon::{Daemon, DaemonPaths};

use crate::fixture;
use crate::ledger::{COUNTER, events, loads, provided};

pub(crate) struct Home(pub(crate) PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(crate) fn home(name: &str) -> Home {
    let root = std::env::temp_dir().join(format!(
        "jinnd-invariant-injects-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts"))
        .unwrap_or_else(|error| panic!("test home: {error}"));
    Home(root)
}

pub(crate) fn entry(
    id: &str,
    grants: serde_json::Value,
    injects: serde_json::Value,
    mode: &str,
) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "package": "demo/counter-plugin",
        "version": "0.0.1",
        "hash": "",
        "config": { "grants": grants, "injects": injects, "data": mode },
    })
}

pub(crate) fn provider(id: &str) -> serde_json::Value {
    entry(
        id,
        serde_json::json!([COUNTER]),
        serde_json::json!([]),
        "provider",
    )
}

pub(crate) fn slow_provider(id: &str) -> serde_json::Value {
    entry(
        id,
        serde_json::json!([COUNTER, "jinn:clock"]),
        serde_json::json!([]),
        "provider-slow",
    )
}

pub(crate) fn declared(id: &str, mode: &str) -> serde_json::Value {
    entry(
        id,
        serde_json::json!([COUNTER]),
        serde_json::json!([COUNTER]),
        mode,
    )
}

pub(crate) fn undeclared(id: &str, mode: &str) -> serde_json::Value {
    entry(
        id,
        serde_json::json!([COUNTER]),
        serde_json::json!([]),
        mode,
    )
}

pub(crate) fn bystander(id: &str, mode: &str) -> serde_json::Value {
    entry(id, serde_json::json!([]), serde_json::json!([]), mode)
}

pub(crate) fn write_profile(home: &Home, entries: &[serde_json::Value], hash: &str) {
    let entries: Vec<serde_json::Value> = entries
        .iter()
        .map(|entry| {
            let mut entry = entry.clone();
            entry["hash"] = serde_json::Value::String(hash.to_owned());
            entry
        })
        .collect();
    let profile = serde_json::json!({ "entries": entries });
    std::fs::write(
        home.0.join("profile.json"),
        serde_json::to_string_pretty(&profile)
            .unwrap_or_else(|error| panic!("profile json: {error}")),
    )
    .unwrap_or_else(|error| panic!("profile write: {error}"));
}

pub(crate) fn paths(home: &Home, entries: &[serde_json::Value]) -> (DaemonPaths, String) {
    let (bytes, hash) = fixture::pinned();
    std::fs::write(home.0.join("artifacts/counter-plugin.wasm"), bytes)
        .unwrap_or_else(|error| panic!("fixture write: {error}"));
    write_profile(home, entries, &hash);
    (
        DaemonPaths {
            profile: home.0.join("profile.json"),
            ledger: home.0.join("ledger.sqlite"),
            artifacts: home.0.join("artifacts"),
            data: home.0.join("data"),
        },
        hash,
    )
}

pub(crate) async fn booted(paths: DaemonPaths) -> Daemon {
    let daemon = Daemon::open(paths).unwrap_or_else(|error| panic!("open: {error:?}"));
    let report = daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot: {error:?}"));
    assert!(report.errors.is_empty(), "clean boot: {:?}", report.errors);
    daemon
}

pub(crate) async fn reload(
    daemon: &Daemon,
    home: &Home,
    entries: &[serde_json::Value],
    hash: &str,
) {
    write_profile(home, entries, hash);
    let report = daemon
        .reload()
        .await
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    assert!(
        report.errors.is_empty(),
        "clean reload: {:?}",
        report.errors
    );
}

pub(crate) fn state(daemon: &Daemon, entry: &str) -> Option<FiberState> {
    daemon
        .entry_fiber(entry)
        .and_then(|fiber| daemon.fiber_state(fiber))
}

pub(crate) async fn until_state(daemon: &Daemon, entry: &str, want: FiberState) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        daemon.sync_transitions();
        if state(daemon, entry) == Some(want) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{entry} reaches {want:?} (rests {:?})",
            state(daemon, entry)
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub(crate) async fn until_loaded(daemon: &Daemon, entry: &str, want: usize) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let loaded = loads(&events(daemon).await, entry);
        if loaded >= want {
            assert_eq!(loaded, want, "{entry} loaded exactly {want} times");
            return;
        }
        assert!(Instant::now() < deadline, "{entry} loads {want} times");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub(crate) async fn witness_gate(daemon: &Daemon, provider: &str, consumer: &str) -> usize {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut observed = 0;
    loop {
        let records = events(daemon).await;
        match state(daemon, provider) {
            Some(FiberState::Active) => return observed,
            Some(FiberState::Loading) if provided(&records, provider) => {
                observed += 1;
                assert_eq!(state(daemon, consumer), Some(FiberState::Pending));
                assert_eq!(loads(&records, consumer), 0);
            }
            _ => {}
        }
        assert!(Instant::now() < deadline, "{provider} reaches Active");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub(crate) async fn settle(daemon: &Daemon) {
    tokio::time::sleep(Duration::from_millis(600)).await;
    daemon.sync_transitions();
}

pub(crate) async fn wait_json(path: &std::path::Path) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(bytes) = std::fs::read(path)
            && !bytes.is_empty()
        {
            return serde_json::from_slice(&bytes)
                .unwrap_or_else(|error| panic!("json {error}: {:?}", bytes));
        }
        assert!(Instant::now() < deadline, "{} lands", path.display());
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
