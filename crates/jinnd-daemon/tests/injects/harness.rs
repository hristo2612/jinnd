//! The daemon home the M2-K24 cases drive: a profile of fixture entries
//! (grants, `injects`, mode), a booted daemon over it, and the reads the
//! cases make of the ledger and the fibers. Split from the cases
//! themselves (R10 file hygiene).

#[path = "../support/mod.rs"]
mod support;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use jinnd_api::FiberState;
use jinnd_daemon::{Daemon, DaemonPaths};

use crate::ledger::{COUNTER, events, loads, provided};

pub(crate) struct Home(pub(crate) PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(crate) fn home(name: &str) -> Home {
    let root = std::env::temp_dir().join(format!("jinnd-injects-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
    Home(root)
}

/// One fixture entry: `grants` and `injects` beside the mode, exactly the
/// seat config the card's delta describes.
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

/// The `provider` mode providing the counter contract.
pub(crate) fn provider(id: &str) -> serde_json::Value {
    entry(
        id,
        serde_json::json!([COUNTER]),
        serde_json::json!([]),
        "provider",
    )
}

/// The `provider-slow` mode: provision lands, then activation dawdles
/// ~700 ms before returning — the provided-but-`Loading` window.
pub(crate) fn slow_provider(id: &str) -> serde_json::Value {
    entry(
        id,
        serde_json::json!([COUNTER, "jinn:clock"]),
        serde_json::json!([]),
        "provider-slow",
    )
}

/// A consumer that DECLARES the counter contract.
pub(crate) fn declared(id: &str, mode: &str) -> serde_json::Value {
    entry(
        id,
        serde_json::json!([COUNTER]),
        serde_json::json!([COUNTER]),
        mode,
    )
}

/// A consumer with the grant and no declaration — today's shape.
pub(crate) fn undeclared(id: &str, mode: &str) -> serde_json::Value {
    entry(
        id,
        serde_json::json!([COUNTER]),
        serde_json::json!([]),
        mode,
    )
}

/// An unrelated sibling with no grants and no declaration.
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
        serde_json::to_string_pretty(&profile).unwrap_or_else(|error| panic!("{error}")),
    )
    .unwrap_or_else(|error| panic!("{error}"));
}

pub(crate) fn paths(home: &Home, entries: &[serde_json::Value]) -> (DaemonPaths, String) {
    let (bytes, hash) = support::pinned_fixture();
    std::fs::write(home.0.join("artifacts/counter-plugin.wasm"), &bytes)
        .unwrap_or_else(|error| panic!("{error}"));
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

/// Waits until `entry` rests in `want`, syncing transitions as it goes.
/// The deadline is well beyond the guest deadline: a run that needs it has
/// already failed the packet's claim.
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

/// Waits until `entry` has been loaded `want` times.
pub(crate) async fn until_loaded(daemon: &Daemon, entry: &str, want: usize) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let loaded = loads(&events(daemon).await, entry);
        if loaded >= want {
            assert_eq!(loaded, want, "{entry} loaded exactly {want} times");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{entry} loads {want} times (loaded {loaded})"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// THE witness for (a): polls the provided-but-`Loading` window of a
/// `provider-slow` entry and, on every observation inside it, requires
/// that `consumer` has not loaded — provision alone is not readiness.
/// Returns how many observations fell inside the window; the caller
/// requires at least one, so the window was actually exercised.
pub(crate) async fn witness_gate(daemon: &Daemon, provider: &str, consumer: &str) -> usize {
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut observed = 0;
    loop {
        let records = events(daemon).await;
        match state(daemon, provider) {
            Some(FiberState::Active) => return observed,
            Some(FiberState::Loading) if provided(&records, provider) => {
                observed += 1;
                assert!(
                    matches!(state(daemon, consumer), None | Some(FiberState::Pending)),
                    "{consumer} waits while {provider} is provided but Loading (rests {:?})",
                    state(daemon, consumer)
                );
                assert_eq!(
                    loads(&records, consumer),
                    0,
                    "{consumer} did not load on provision"
                );
            }
            _ => {}
        }
        assert!(Instant::now() < deadline, "{provider} reaches Active");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A quiet interval for the NEGATIVE claims — that nothing moved.
pub(crate) async fn settle(daemon: &Daemon) {
    tokio::time::sleep(Duration::from_millis(600)).await;
    daemon.sync_transitions();
}

/// Waits for a file a guest writes once, then reads it as JSON.
pub(crate) async fn wait_json(path: &std::path::Path) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(bytes) = std::fs::read(path)
            && !bytes.is_empty()
        {
            return serde_json::from_slice(&bytes).unwrap_or_else(|error| {
                panic!("json: {error} in {:?}", String::from_utf8_lossy(&bytes))
            });
        }
        assert!(Instant::now() < deadline, "{} lands", path.display());
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
