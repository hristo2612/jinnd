//! M2-K3 acceptance, through the real daemon assembly: a fixture plugin
//! exercises the full `jinn:fs` bundle (write/append/meta/list/remove and
//! the typed not-found) under its profile grant; every op is ledgered with
//! attribution; the daemon's keyed revert undoes append (truncate to the
//! prior length) and remove (restore prior content) from the retention
//! store and reclaims each consumed inverse; and every new op refuses
//! without a grant, on the record.

mod support;

use std::path::PathBuf;

use jinnd_api::{LedgerEventKind, LedgerRecord, RevertResolution};
use jinnd_daemon::{Daemon, DaemonPaths};

struct Home(PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn home(name: &str) -> Home {
    let root = std::env::temp_dir().join(format!("jinnd-fs-test-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
    Home(root)
}

fn paths(home: &Home, grants: serde_json::Value, mode: &str) -> DaemonPaths {
    let (bytes, hash) = support::pinned_fixture();
    std::fs::write(home.0.join("artifacts/counter-plugin.wasm"), &bytes)
        .unwrap_or_else(|error| panic!("{error}"));
    let profile = serde_json::json!({
        "entries": [{
            "id": "scribe",
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

fn inverse_files(paths: &DaemonPaths) -> usize {
    std::fs::read_dir(paths.inverses())
        .map(|entries| {
            entries
                .flatten()
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "inverse"))
                .count()
        })
        .unwrap_or(0)
}

#[tokio::test]
async fn the_fs_bundle_round_trips_with_revertible_inverses_from_the_spill() {
    let home = home("bundle");
    let paths = paths(&home, serde_json::json!(["jinn:fs"]), "fs-bundle");
    let daemon = Daemon::open(paths.clone()).unwrap_or_else(|error| panic!("open: {error:?}"));
    let report = daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot: {error:?}"));
    assert!(report.errors.is_empty(), "clean boot: {:?}", report.errors);
    let fiber = daemon
        .entry_fiber("scribe")
        .unwrap_or_else(|| panic!("the entry has a fiber"));
    let data = paths.data.clone();
    assert_eq!(
        std::fs::read(data.join("log/a.txt")).ok(),
        Some(b"one\ntwo\n".to_vec()),
        "write then append"
    );
    assert!(!data.join("log/b.txt").exists(), "removed");

    // Every op crossed the broker with the entry's attribution (Law 2, R4).
    let records = events(&daemon).await;
    for op in ["write", "append", "meta", "list", "remove", "read"] {
        assert!(
            records.iter().any(|record| matches!(
                &record.kind,
                LedgerEventKind::ContractCall { contract, operation }
                    if contract == "jinn:fs" && operation == op
            ) && record.fiber == Some(fiber)),
            "the {op} crossing is ledgered with attribution: {records:?}"
        );
    }
    let effects = daemon.fs_effects();
    let labels: Vec<&str> = effects.iter().map(|(_, label)| label.as_str()).collect();
    assert_eq!(
        labels,
        vec!["log/a.txt", "log/a.txt", "log/b.txt", "log/b.txt"]
    );
    assert_eq!(inverse_files(&paths), 4, "one durable inverse per effect");
    for (effect, _) in &effects {
        assert!(
            records.iter().any(|record| matches!(
                &record.kind,
                LedgerEventKind::EffectRegistered { label }
                    if label.ends_with(&format!("[effect {}]", effect.0))
            ) && record.fiber == Some(fiber)),
            "effect {} registered with attribution",
            effect.0
        );
    }

    // Undo of remove restores the prior content; undo of append truncates
    // to the prior length; each consumed inverse is reclaimed.
    let (removed, _) = effects[3];
    let resolution = daemon
        .revert(removed, "k3-remove")
        .await
        .unwrap_or_else(|error| panic!("revert: {error:?}"));
    assert_eq!(resolution, RevertResolution::Reverted);
    assert_eq!(
        std::fs::read(data.join("log/b.txt")).ok(),
        Some(b"bee".to_vec())
    );
    let (appended, _) = effects[1];
    let resolution = daemon
        .revert(appended, "k3-append")
        .await
        .unwrap_or_else(|error| panic!("revert: {error:?}"));
    assert_eq!(resolution, RevertResolution::Reverted);
    assert_eq!(
        std::fs::read(data.join("log/a.txt")).ok(),
        Some(b"one\n".to_vec())
    );
    assert_eq!(inverse_files(&paths), 2, "consumed inverses are reclaimed");
    assert_eq!(daemon.fs_effects().len(), 2);
    // A same-key replay answers from the record — the reclaimed inverse is
    // never needed again (constitution 03).
    assert_eq!(
        daemon
            .revert(appended, "k3-append")
            .await
            .unwrap_or_else(|error| panic!("replay: {error:?}")),
        RevertResolution::Reverted
    );
    assert!(
        daemon.revert(appended, "other-key").await.is_err(),
        "a distinct key is refused"
    );

    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// Red-first (M2-K3): each new op — list, meta, append, remove — refuses
/// without a grant at the broker choke point, every refusal a ledger event
/// with the entry's attribution; the entry itself activates cleanly.
#[tokio::test]
async fn each_new_fs_op_refuses_without_a_grant_on_the_record() {
    let home = home("denied");
    let paths = paths(&home, serde_json::json!([]), "fs-bundle-denied");
    let daemon = Daemon::open(paths).unwrap_or_else(|error| panic!("open: {error:?}"));
    let report = daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot: {error:?}"));
    assert!(
        report.errors.is_empty(),
        "the guest observed every refusal: {:?}",
        report.errors
    );
    let fiber = daemon
        .entry_fiber("scribe")
        .unwrap_or_else(|| panic!("the entry has a fiber"));
    let refusals = events(&daemon)
        .await
        .iter()
        .filter(|record| {
            matches!(&record.kind, LedgerEventKind::GrantRefused { contract } if contract == "jinn:fs")
                && record.fiber == Some(fiber)
        })
        .count();
    assert_eq!(
        refusals, 4,
        "list, meta, append, remove — each refused on the record"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}
