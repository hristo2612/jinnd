//! M2-K14 acceptance, through the real daemon assembly: a fixture plugin
//! makes real outbound calls to a loopback HTTP target it was granted, and
//! every other reading is a DIFFERENT answer — an off-allowlist authority
//! denied without a dial, a malformed URL invalid, an off-allowlist
//! redirect denied inside the call. The Law-2 record carries the call's
//! shape and none of its credentials, and an entry cannot widen its own
//! allowlist: those pins live in `authority`. Law 3 — a revert unit
//! containing the call REJECTED WHOLE, and still rejected after a daemon
//! reopen — lives in `revert` (split by seam; test-file cap soft).

#[path = "../support/mod.rs"]
mod support;

mod authority;
mod revert;

use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use jinnd_api::{LedgerEventKind, LedgerRecord};
use jinnd_daemon::{Daemon, DaemonPaths};

/// The secret the fixture carries in an `authorization` header AND in a
/// query string: the test greps the whole ledger file for these bytes.
const SECRET: &str = "sk-live-0xDEADBEEF-fixture-secret";

struct Home(PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn home(name: &str) -> Home {
    let root =
        std::env::temp_dir().join(format!("jinnd-outbound-test-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
    Home(root)
}

fn paths(home: &Home, grants: serde_json::Value, mode: &str) -> DaemonPaths {
    let (bytes, hash) = support::pinned_fixture();
    std::fs::write(home.0.join("artifacts/counter-plugin.wasm"), &bytes)
        .unwrap_or_else(|error| panic!("{error}"));
    let profile = serde_json::json!({ "entries": [{
        "id": "worker",
        "package": "demo/counter-plugin",
        "version": "0.0.1",
        "hash": hash,
        "config": { "grants": grants, "data": mode },
    }]});
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

async fn booted(paths: DaemonPaths) -> Daemon {
    let daemon = Daemon::open(paths).unwrap_or_else(|error| panic!("open: {error:?}"));
    let report = daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot: {error:?}"));
    assert!(report.errors.is_empty(), "clean boot: {:?}", report.errors);
    daemon
}

async fn events(daemon: &Daemon) -> Vec<LedgerRecord> {
    daemon
        .ledger_events()
        .await
        .unwrap_or_else(|error| panic!("ledger read: {error:?}"))
}

/// A loopback HTTP target: `/probe` answers 200 `pong`, `/redirect`
/// answers a 302 pointing at `elsewhere`. `hits` counts every accepted
/// connection, so a test can prove a target was never dialled.
struct Target {
    port: u16,
    hits: Arc<AtomicUsize>,
}

fn target(elsewhere: Option<u16>) -> Target {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").unwrap_or_else(|error| panic!("bind: {error}"));
    let port = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("addr: {error}"))
        .port();
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&hits);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            counter.fetch_add(1, Ordering::SeqCst);
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                match stream.read(&mut byte) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => head.push(byte[0]),
                }
            }
            let line = String::from_utf8_lossy(&head).into_owned();
            let answer = if line.starts_with("GET /redirect") {
                format!(
                    "HTTP/1.1 302 Found\r\nlocation: http://127.0.0.1:{}/taken\r\ncontent-length: 0\r\n\r\n",
                    elsewhere.unwrap_or(0)
                )
            } else {
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 4\r\n\r\npong"
                    .to_owned()
            };
            let _ = stream.write_all(answer.as_bytes());
        }
    });
    Target { port, hits }
}

fn requests(records: &[LedgerRecord]) -> Vec<&LedgerEventKind> {
    records
        .iter()
        .map(|record| &record.kind)
        .filter(|kind| matches!(kind, LedgerEventKind::NetRequested { .. }))
        .collect()
}
