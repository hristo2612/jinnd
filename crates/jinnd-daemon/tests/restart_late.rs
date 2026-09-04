//! A replacement seat is INSTALLED at its commit (M2-K26 amendment 2;
//! harness FINDINGS #53): a registration the successor makes AFTER its
//! activation — an `alarm-at` armed from a wake handler — routes exactly
//! as it does on a first activation, and its wake lands on the record. At
//! `138fdce` the replacement stays a staging seat for the rest of its
//! life: the request is answered `Ok(0)`, nothing is armed, no row lands
//! (R9: no silent replacement; Law 2).

mod support;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use jinnd_api::{FiberId, FiberState, LedgerEventKind, LedgerRecord, TransitionCause};
use jinnd_daemon::{Daemon, DaemonPaths};

struct Home(PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn home(name: &str) -> Home {
    let root =
        std::env::temp_dir().join(format!("jinnd-restart-late-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
    Home(root)
}

/// One entry in `clock-chain`: a periodic alarm requested at activation
/// whose first tick arms a one-shot FROM THE HANDLER (a late registration).
fn write_profile(home: &Home, hash: &str, revision: u64) {
    let profile = serde_json::json!({
        "entries": [{
            "id": "waker",
            "package": "demo/counter-plugin",
            "version": "0.0.1",
            "hash": hash,
            "config": { "grants": ["jinn:clock"], "data": "clock-chain", "revision": revision },
        }]
    });
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
    write_profile(home, &hash, 1);
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

async fn events(daemon: &Daemon) -> Vec<LedgerRecord> {
    daemon
        .ledger_events()
        .await
        .unwrap_or_else(|error| panic!("ledger read: {error:?}"))
}

/// The distinct alarms that woke `fiber` in the rows at or after `from`.
fn alarms_woken(records: &[LedgerRecord], fiber: FiberId, from: usize) -> BTreeSet<u64> {
    records
        .iter()
        .skip(from)
        .filter(|record| record.fiber == Some(fiber))
        .filter_map(|record| match &record.kind {
            LedgerEventKind::AlarmWake { alarm } => Some(*alarm),
            _ => None,
        })
        .collect()
}

/// Waits until two DISTINCT alarms have woken the fiber since `from`: the
/// periodic one and the one-shot its handler armed.
async fn wait_for_chain(daemon: &Daemon, fiber: FiberId, from: usize, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let records = events(daemon).await;
        let woken = alarms_woken(&records, fiber, from);
        if woken.len() >= 2 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: the one-shot armed from the wake handler never fired — \
             alarms woken since row {from}: {woken:?}; records: {records:#?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The index of the fiber's `ConfigChanged` transition INTO `Active`.
async fn restarted_active(daemon: &Daemon, fiber: FiberId) -> usize {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let records = events(daemon).await;
        let landed = records.iter().position(|record| {
            record.fiber == Some(fiber)
                && matches!(
                    &record.kind,
                    LedgerEventKind::FiberTransition(transition)
                        if transition.to == FiberState::Active
                            && transition.cause == TransitionCause::ConfigChanged
                )
        });
        if let Some(index) = landed {
            return index;
        }
        assert!(
            Instant::now() < deadline,
            "the replacement never rested Active: {records:#?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// FINDINGS #53: after a `ConfigChanged` restart, the replacement's
/// post-activation `alarm-at` is armed and wakes it — the seat committed
/// INSTALLED, not staging. The first incarnation is the control: the same
/// chain fires there at every pin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replacement_seat_routes_a_registration_made_after_its_activation() {
    let home = home("chain");
    let (paths, hash) = paths(&home);
    let daemon = Daemon::open(paths).unwrap_or_else(|error| panic!("open: {error:?}"));
    let report = daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot: {error:?}"));
    assert!(report.errors.is_empty(), "clean boot: {:?}", report.errors);
    let fiber = daemon
        .entry_fiber("waker")
        .unwrap_or_else(|| panic!("the entry has a fiber"));
    wait_for_chain(&daemon, fiber, 0, "first activation (control)").await;

    write_profile(&home, &hash, 2);
    let report = daemon
        .reload()
        .await
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    assert_eq!(
        report.restarted.len(),
        1,
        "exactly the edited entry restarted"
    );
    let active = restarted_active(&daemon, fiber).await;
    wait_for_chain(&daemon, fiber, active, "after the config restart").await;

    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}
