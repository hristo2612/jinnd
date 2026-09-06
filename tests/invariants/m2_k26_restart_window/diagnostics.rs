//! Preserve the actual K26 guest state before a failing home is removed.

use super::dispatch;
use jinnd_daemon::{Daemon, DaemonPaths};
use std::path::Path;
use std::time::{Duration, Instant};

fn files(root: &Path, directory: &Path, output: &mut serde_json::Map<String, serde_json::Value>) {
    for entry in std::fs::read_dir(directory).into_iter().flatten().flatten() {
        let path = entry.path();
        if path.is_dir() {
            files(root, &path, output);
        } else if let Ok(bytes) = std::fs::read(&path) {
            let name = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .display()
                .to_string();
            output.insert(name, serde_json::json!(bytes));
        }
    }
}

pub(super) async fn snapshot(daemon: &Daemon, paths: &DaemonPaths, phase: &str) {
    daemon.sync_transitions();
    let states: Vec<_> = ["provider", "consumer", "trigger"]
        .into_iter()
        .map(|entry| {
            let fiber = daemon.entry_fiber(entry);
            serde_json::json!({"entry": entry, "fiber": fiber,
            "state": fiber.and_then(|id| daemon.fiber_state(id))})
        })
        .collect();
    let mut guest_files = serde_json::Map::new();
    files(&paths.data, &paths.data, &mut guest_files);
    let records = dispatch::events(daemon).await;
    println!(
        "K26-EVIDENCE {}",
        serde_json::json!({
            "phase": phase, "states": states, "files": guest_files, "ledger": records,
            "profile": std::fs::read(&paths.profile).unwrap_or_default()
        })
    );
}

pub(super) async fn outcome(daemon: &Daemon, paths: &DaemonPaths) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(bytes) = std::fs::read(paths.data.join("notify.out"))
            && !bytes.is_empty()
        {
            return bytes;
        }
        if Instant::now() >= deadline {
            snapshot(daemon, paths, "notify.out timeout").await;
            panic!("notify.out did not land within the unchanged 30s bound; full evidence above");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
