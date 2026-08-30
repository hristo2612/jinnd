//! The daemon home this suite drives its two acceptance cases through:
//! a profile of fixture entries, a booted daemon over it, and the reads
//! the cases make of what the guests left behind. Split from the cases
//! themselves (R10 file hygiene).

#[path = "../support/mod.rs"]
mod support;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use jinnd_api::LedgerRecord;
use jinnd_daemon::{Daemon, DaemonPaths};

pub(crate) struct Home(pub(crate) PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(crate) fn home(name: &str) -> Home {
    let root = std::env::temp_dir().join(format!("jinnd-cycle-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
    Home(root)
}

pub(crate) fn entry(id: &str, grants: serde_json::Value, mode: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "package": "demo/counter-plugin",
        "version": "0.0.1",
        "hash": "",
        "config": { "grants": grants, "data": mode },
    })
}

pub(crate) fn paths(home: &Home, entries: Vec<serde_json::Value>) -> DaemonPaths {
    let (bytes, hash) = support::pinned_fixture();
    std::fs::write(home.0.join("artifacts/counter-plugin.wasm"), &bytes)
        .unwrap_or_else(|error| panic!("{error}"));
    let entries: Vec<serde_json::Value> = entries
        .into_iter()
        .map(|mut entry| {
            entry["hash"] = serde_json::Value::String(hash.clone());
            entry
        })
        .collect();
    let profile = serde_json::json!({ "entries": entries });
    std::fs::write(
        home.0.join("profile.json"),
        serde_json::to_string_pretty(&profile).unwrap_or_else(|error| panic!("{error}")),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    DaemonPaths {
        profile: home.0.join("profile.json"),
        ledger: home.0.join("ledger.sqlite"),
        artifacts: home.0.join("artifacts"),
        data: home.0.join("data"),
    }
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

pub(crate) async fn events(daemon: &Daemon) -> Vec<LedgerRecord> {
    daemon
        .ledger_events()
        .await
        .unwrap_or_else(|error| panic!("ledger read: {error:?}"))
}

/// Waits for a file the guests write once the shape has run. The deadline
/// is well beyond the kernel's own guest deadline: a run that needs it has
/// already failed the packet's claim.
pub(crate) async fn wait_for(path: &std::path::Path) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(bytes) = std::fs::read(path)
            && !bytes.is_empty()
        {
            return bytes;
        }
        assert!(
            Instant::now() < deadline,
            "{} lands (nothing parked forever)",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// The typed record a `cycle-*` guest wrote back: tag 7 then the record.
pub(crate) fn cycle(bytes: &[u8]) -> serde_json::Value {
    assert_eq!(
        bytes.first(),
        Some(&7),
        "the typed wait-cycle refusal (tag 7), got {:?}: {}",
        bytes.first(),
        String::from_utf8_lossy(&bytes[1.min(bytes.len())..])
    );
    json(&bytes[1..])
}

pub(crate) fn json(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|error| panic!("json: {error} in {:?}", String::from_utf8_lossy(bytes)))
}
