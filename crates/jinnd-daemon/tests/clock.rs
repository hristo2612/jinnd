//! M2-K2 acceptance, through the real daemon assembly: a fixture plugin
//! requests a periodic alarm from its profile entry and receives typed
//! wakes; every wake is a ledger event with fiber attribution; the daemon's
//! shutdown — the seat's teardown, the effect's undo — cancels the alarm;
//! and a bus emit through the daemon path lands exactly one DispatchTrace.
//! Restart honesty is contractual: alarms are host memory, so a new daemon
//! holds none until plugins re-request on activate (contracts/jinn-clock).

mod support;

use std::path::PathBuf;

use jinnd_api::{DispatchMode, LedgerEventKind, LedgerRecord};
use jinnd_daemon::{Daemon, DaemonPaths};

/// A scratch home for one test daemon; removed on drop.
struct Home(PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn home(name: &str) -> Home {
    let root = std::env::temp_dir().join(format!("jinnd-clock-test-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
    Home(root)
}

/// Boots a daemon over one profile entry running the counter fixture in
/// `mode`, with the given grants.
fn paths(home: &Home, grants: &[&str], mode: &str) -> DaemonPaths {
    let (bytes, hash) = support::pinned_fixture();
    std::fs::write(home.0.join("artifacts/counter-plugin.wasm"), &bytes)
        .unwrap_or_else(|error| panic!("{error}"));
    let profile = serde_json::json!({
        "entries": [{
            "id": "waker",
            "package": "demo/counter-plugin",
            "version": "0.0.1",
            "hash": hash,
            "config": { "grants": grants, "data": mode },
        }]
    });
    let profile_path = home.0.join("profile.json");
    std::fs::write(
        &profile_path,
        serde_json::to_string_pretty(&profile).unwrap_or_else(|error| panic!("{error}")),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    DaemonPaths {
        profile: profile_path,
        ledger: home.0.join("ledger.sqlite"),
        artifacts: home.0.join("artifacts"),
        data: home.0.join("data"),
    }
}

async fn events(daemon: &Daemon) -> Vec<LedgerRecord> {
    daemon
        .ledger_events()
        .await
        .unwrap_or_else(|error| panic!("ledger read: {error:?}"))
}

fn wake_count(records: &[LedgerRecord]) -> usize {
    records
        .iter()
        .filter(|record| matches!(record.kind, LedgerEventKind::AlarmWake { .. }))
        .count()
}

#[tokio::test]
async fn a_profile_entry_holds_a_periodic_alarm_ledgered_wake_by_wake() {
    let home = home("alarm");
    let daemon = Daemon::open(paths(
        &home,
        &["jinn:clock", "jinn:test/counter"],
        "clock-alarm",
    ))
    .unwrap_or_else(|error| panic!("open: {error:?}"));
    let report = daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot: {error:?}"));
    assert!(report.errors.is_empty(), "clean boot: {:?}", report.errors);
    let fiber = daemon
        .entry_fiber("waker")
        .unwrap_or_else(|| panic!("the entry has a fiber"));

    // The alarm request is an effect, ledgered under its label (Law 2, R5).
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let records = events(&daemon).await;
        if wake_count(&records) >= 2 {
            assert!(
                records.iter().any(|record| matches!(
                    &record.kind,
                    LedgerEventKind::EffectRegistered { label } if label == "alarm every 250ms"
                )),
                "the request itself is a ledger event (guest trail)"
            );
            assert!(
                records
                    .iter()
                    .filter(|record| matches!(record.kind, LedgerEventKind::AlarmWake { .. }))
                    .all(|record| record.fiber == Some(fiber)),
                "every wake carries the requesting fiber's attribution"
            );
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "two ledgered wakes should arrive well within the deadline; got {records:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Shutdown disposes the fiber: the seat's LIFO teardown runs the alarm
    // effect's undo — cancellation, ledgered — and no wake lands after.
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
    let records = events(&daemon).await;
    assert!(
        records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::EffectWithdrawn { label, clean: true }
                if label == "alarm every 250ms"
        )),
        "the undo cancelled the alarm, ledgered under the request's label"
    );
    let settled = wake_count(&records);
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;
    assert_eq!(
        wake_count(&events(&daemon).await),
        settled,
        "after the undo returned, no wake is ever appended again"
    );
}

#[tokio::test]
async fn a_bus_emit_through_the_daemon_path_lands_exactly_one_dispatch_trace() {
    let home = home("trace");
    let daemon = Daemon::open(paths(&home, &[], "emitter"))
        .unwrap_or_else(|error| panic!("open: {error:?}"));
    let report = daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot: {error:?}"));
    assert!(report.errors.is_empty(), "clean boot: {:?}", report.errors);
    let fiber = daemon.entry_fiber("waker");

    let records = events(&daemon).await;
    let traces: Vec<&LedgerRecord> = records
        .iter()
        .filter(|record| matches!(record.kind, LedgerEventKind::DispatchTrace { .. }))
        .collect();
    assert_eq!(traces.len(), 1, "exactly one trace per emit: {records:?}");
    match &traces[0].kind {
        LedgerEventKind::DispatchTrace {
            topic,
            mode,
            listeners,
            failures,
            ..
        } => {
            assert_eq!(topic, "jinn:test/topic");
            assert_eq!(*mode, DispatchMode::Emit);
            assert_eq!(
                (*listeners, *failures),
                (0, 0),
                "no listener registered, none failed — the audit statement is exact"
            );
        }
        other => panic!("not a trace: {other:?}"),
    }
    assert_eq!(traces[0].fiber, fiber, "attributed to the emitting fiber");

    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}
