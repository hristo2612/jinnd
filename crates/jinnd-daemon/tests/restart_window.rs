//! The restart window on the record (M2-K26; harness FINDINGS #47): a
//! config restart's suspension leaves the listener's subscription in
//! place as a tombstone; the old subscription ends at the REPLACEMENT'S
//! COMMIT (never at the suspension, never absent in between), and a
//! replacement that fails has its tombstones withdrawn AFTER the fiber
//! rests `Failed` — each ending an `EffectWithdrawn` row at the moment it
//! actually happened (Law 2, I4).

mod support;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use jinnd_api::{FiberId, FiberState, LedgerEventKind, LedgerRecord};
use jinnd_daemon::{Daemon, DaemonPaths};

const TOPIC: &str = "jinn:test/settings-changed";

struct Home(PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn home(name: &str) -> Home {
    let root = std::env::temp_dir().join(format!(
        "jinnd-restart-window-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
    Home(root)
}

fn consumer(hash: &str, config: serde_json::Value) -> serde_json::Value {
    let mut config = config;
    config["grants"] = serde_json::json!([TOPIC, "jinn:fs", "jinn:clock"]);
    serde_json::json!({
        "id": "consumer",
        "package": "demo/counter-plugin",
        "version": "0.0.1",
        "hash": hash,
        "config": config,
    })
}

fn write_profile(home: &Home, entries: serde_json::Value) {
    let profile = serde_json::json!({ "entries": entries });
    std::fs::write(
        home.0.join("profile.json"),
        serde_json::to_string_pretty(&profile).unwrap_or_else(|error| panic!("{error}")),
    )
    .unwrap_or_else(|error| panic!("{error}"));
}

fn paths(home: &Home) -> (DaemonPaths, String) {
    let (bytes, hash) = support::pinned_fixture();
    std::fs::write(home.0.join("artifacts/counter-plugin.wasm"), &bytes)
        .unwrap_or_else(|error| panic!("{error}"));
    write_profile(
        home,
        serde_json::json!([consumer(&hash, serde_json::json!({ "data": "notify-consumer" }))]),
    );
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

/// Indexes (ledger order) of the consumer fiber's `listen` withdrawals,
/// `listen` registrations, suspensions, and transitions INTO `to`.
struct Trail {
    withdrawn: Vec<usize>,
    registered: Vec<usize>,
    suspended: Vec<usize>,
    rested: Vec<usize>,
}

fn trail(records: &[LedgerRecord], fiber: FiberId, to: FiberState) -> Trail {
    let label = format!("listen {TOPIC}");
    let mut out = Trail {
        withdrawn: Vec::new(),
        registered: Vec::new(),
        suspended: Vec::new(),
        rested: Vec::new(),
    };
    for (index, record) in records.iter().enumerate() {
        if record.fiber != Some(fiber) {
            continue;
        }
        match &record.kind {
            LedgerEventKind::EffectWithdrawn { label: seen, .. } if *seen == label => {
                out.withdrawn.push(index);
            }
            LedgerEventKind::EffectRegistered { label: seen } if *seen == label => {
                out.registered.push(index);
            }
            LedgerEventKind::FiberSuspended { .. } => out.suspended.push(index),
            LedgerEventKind::FiberTransition(transition) if transition.to == to => {
                out.rested.push(index);
            }
            _ => {}
        }
    }
    out
}

async fn wait_until(daemon: &Daemon, fiber: FiberId, state: FiberState) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let rested = events(daemon)
            .await
            .iter()
            .any(|record| {
                record.fiber == Some(fiber)
                    && matches!(&record.kind, LedgerEventKind::FiberTransition(t) if t.to == state)
            });
        if rested {
            return;
        }
        assert!(Instant::now() < deadline, "the consumer never rested {state:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// M2-K26 (a)+(b): the suspension entombs the subscription and the
/// replacement's commit replaces it — so the ONE withdrawal row of the
/// old listen lands after the suspension AND after the successor's own
/// registration, never before either. At the pin this row precedes the
/// suspension (harness FINDINGS #47, rows 386–387).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replacement_ends_the_old_subscription_at_its_commit_not_at_the_suspension() {
    let home = home("commit");
    let (paths, hash) = paths(&home);
    let daemon = booted(paths).await;
    let fiber = daemon
        .entry_fiber("consumer")
        .unwrap_or_else(|| panic!("the consumer has a fiber"));

    write_profile(
        &home,
        serde_json::json!([consumer(
            &hash,
            serde_json::json!({ "data": "notify-consumer", "revision": 2 })
        )]),
    );
    let report = daemon
        .reload()
        .await
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    assert_eq!(report.restarted.len(), 1, "exactly the edited entry restarted");
    wait_until(&daemon, fiber, FiberState::Active).await;
    // The restart's own Active: the second one on the record.
    let records = events(&daemon).await;
    let seen = trail(&records, fiber, FiberState::Active);
    assert_eq!(seen.registered.len(), 2, "two incarnations listened: {records:?}");
    assert_eq!(seen.suspended.len(), 1, "one suspension: {records:?}");
    assert_eq!(
        seen.withdrawn.len(),
        1,
        "the old subscription ended exactly once: {records:?}"
    );
    let (withdrawn, suspended, successor) = (seen.withdrawn[0], seen.suspended[0], seen.registered[1]);
    assert!(
        withdrawn > suspended,
        "the subscription outlived its suspension (row {withdrawn} vs {suspended}): {records:?}"
    );
    assert!(
        withdrawn > successor,
        "it ended at the successor's commit, never before its registration \
         (row {withdrawn} vs {successor}): {records:?}"
    );
}

/// M2-K26 (c): a replacement that fails activation rests `Failed`, and
/// ONLY THEN are its tombstones withdrawn — the record shows when the
/// subscription actually ended, not when the old instance died. At the
/// pin the withdrawal precedes even the suspension.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_replacement_withdraws_its_tombstones_after_it_rests_failed() {
    let home = home("failed");
    let (paths, hash) = paths(&home);
    let daemon = booted(paths).await;
    let fiber = daemon
        .entry_fiber("consumer")
        .unwrap_or_else(|| panic!("the consumer has a fiber"));

    write_profile(
        &home,
        serde_json::json!([consumer(&hash, serde_json::json!({ "data": "trap" }))]),
    );
    let _ = daemon.reload().await;
    wait_until(&daemon, fiber, FiberState::Failed).await;
    let records = events(&daemon).await;
    let seen = trail(&records, fiber, FiberState::Failed);
    assert_eq!(seen.registered.len(), 1, "the trap never listened: {records:?}");
    assert_eq!(seen.withdrawn.len(), 1, "withdrawn exactly once: {records:?}");
    assert_eq!(seen.rested.len(), 1, "rested Failed once: {records:?}");
    assert!(
        seen.withdrawn[0] > seen.rested[0],
        "the tombstone was withdrawn after the fiber rested Failed (row {} vs {}): {records:?}",
        seen.withdrawn[0],
        seen.rested[0]
    );
}
