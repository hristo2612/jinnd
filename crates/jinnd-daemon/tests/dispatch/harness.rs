//! The daemon home this suite drives its acceptance case through: a
//! profile of fixture entries, a booted daemon over it, and the reads the
//! case makes of what the guests left behind. Split from the case itself
//! (R10 file hygiene): the shape of the harness is not the shape of the
//! claim, and the claim is easier to check when nothing else shares its
//! file.

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
    let root = std::env::temp_dir().join(format!("jinnd-dispatch-{name}-{}", std::process::id()));
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

pub(crate) async fn wait_for(path: &std::path::Path, ready: impl Fn(&[u8]) -> bool) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(bytes) = std::fs::read(path)
            && ready(&bytes)
        {
            return bytes;
        }
        assert!(
            Instant::now() < deadline,
            "{} lands; attempts so far: {:?}",
            path.display(),
            (
                std::fs::read(path.with_file_name("notify.log")).unwrap_or_default(),
                String::from_utf8_lossy(
                    &std::fs::read(path.with_file_name("notify.err")).unwrap_or_default()
                )
                .into_owned(),
            )
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub(crate) fn json(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).unwrap_or_else(|error| panic!("json: {error}"))
}
