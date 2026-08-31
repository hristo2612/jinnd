//! M2-K8 acceptance, through the real daemon assembly (harness findings
//! 5-remainder / 24): a fixture drives the `jinn:keystore` bundle under a
//! prefix grant — put/get/list/delete, a key beside the prefix refused on
//! the record, the typed not-found — and the ledger holds key names and
//! digests only: the value bytes appear NOWHERE on disk (ledger file,
//! sealed store, inverses); the daemon's keyed revert restores a deleted
//! value from the sealed spill; dispose withdraws the entry's keystore
//! trail LIFO. The grant-attenuation pins live in `authority`, the
//! journal-identity pins in `journal` (split by seam; test-file cap soft).

#[path = "../support/mod.rs"]
mod support;

mod authority;
mod journal;

use std::path::PathBuf;

use jinnd_api::{FiberState, LedgerEventKind, LedgerRecord, RefusalReason, RevertResolution};
use jinnd_daemon::{Daemon, DaemonPaths, MasterKeySource};

/// The fixture's secret (fixtures/counter-plugin `SECRET`).
const SECRET: &[u8] = b"sk-live-0xDEADBEEF-fixture-secret";
const KEPT: &[u8] = b"kept-0xCAFEBABE-value";
/// The operator passphrase the tests supply OUTSIDE the data root (round-2
/// vault ruling): the daemon derives the master key from it; nothing under
/// the home may ever hold it.
const PASSPHRASE: &[u8] = b"operator-passphrase-0xFEEDFACE";

fn passphrase() -> MasterKeySource {
    MasterKeySource::Passphrase(PASSPHRASE.to_vec())
}

struct Home(PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn home(name: &str) -> Home {
    let root =
        std::env::temp_dir().join(format!("jinnd-keystore-test-{name}-{}", std::process::id()));
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
            "id": "holder",
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

async fn booted(paths: DaemonPaths) -> Daemon {
    let daemon =
        Daemon::open_with(paths, passphrase()).unwrap_or_else(|error| panic!("open: {error:?}"));
    let report = daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot: {error:?}"));
    println!("K12 boot report: {report:?}");
    assert!(report.errors.is_empty(), "clean boot: {:?}", report.errors);
    daemon
}

async fn events(daemon: &Daemon) -> Vec<LedgerRecord> {
    daemon
        .ledger_events()
        .await
        .unwrap_or_else(|error| panic!("ledger read: {error:?}"))
}

fn scope_refusals(records: &[LedgerRecord], contract: &str) -> Vec<String> {
    records
        .iter()
        .filter_map(|record| match &record.kind {
            LedgerEventKind::GrantRefused {
                contract: refused,
                reason: RefusalReason::ScopeMismatch,
                detail: Some(detail),
            } if refused == contract
                && record
                    .entry
                    .as_ref()
                    .is_some_and(|entry| entry.0 == "holder") =>
            {
                Some(detail.clone())
            }
            _ => None,
        })
        .collect()
}

/// Every byte under `root`, recursively.
fn bytes_under(root: &std::path::Path) -> Vec<u8> {
    let mut all = Vec::new();
    for entry in std::fs::read_dir(root).into_iter().flatten().flatten() {
        let path = entry.path();
        if path.is_dir() {
            all.extend(bytes_under(&path));
        } else {
            all.extend(std::fs::read(&path).unwrap_or_default());
        }
    }
    all
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

fn active(daemon: &Daemon) -> bool {
    daemon
        .entry_fiber("holder")
        .and_then(|fiber| daemon.fiber_state(fiber))
        == Some(FiberState::Active)
}

#[tokio::test]
async fn the_keystore_bundle_round_trips_and_the_value_is_nowhere_on_disk() {
    let home = home("bundle");
    let paths = paths(
        &home,
        serde_json::json!([{ "contract": "jinn:keystore", "scope": ["engines/"] }, "jinn:fs"]),
        "keystore",
    );
    let daemon = booted(paths.clone()).await;
    assert!(active(&daemon), "the bundle round-tripped in activate");
    assert_eq!(
        std::fs::read(paths.data.join("keystore.out")).ok(),
        Some(b"engines/openai".to_vec()),
        "list answers names only, under the prefix"
    );
    let records = events(&daemon).await;
    for op in ["put", "get", "list", "delete"] {
        assert!(
            records.iter().any(|record| matches!(&record.kind, LedgerEventKind::ContractCall { contract, operation } if contract == "jinn:keystore" && operation == op)
                && record.entry.as_ref().is_some_and(|entry| entry.0 == "holder")),
            "the {op} crossing is ledgered with entry attribution"
        );
    }
    let accessed: Vec<(String, String, bool)> = records
        .iter()
        .filter_map(|record| match &record.kind {
            LedgerEventKind::KeystoreAccessed {
                operation,
                key,
                digest,
            } => Some((operation.clone(), key.clone(), digest.is_some())),
            _ => None,
        })
        .collect();
    assert!(
        accessed.contains(&("put".to_owned(), "engines/openai".to_owned(), true))
            && accessed.contains(&("delete".to_owned(), "engines/openai".to_owned(), false)),
        "name and digest per crossing: {accessed:?}"
    );
    let refused = scope_refusals(&records, "jinn:keystore");
    assert_eq!(refused.len(), 1, "{refused:?}");
    assert!(refused[0].contains("smtp/password") && !contains(refused[0].as_bytes(), b"beside"));
    let effects = daemon.keystore_effects();
    let keys: Vec<&str> = effects.iter().map(|(_, key)| key.as_str()).collect();
    assert_eq!(
        keys,
        vec!["engines/openai", "engines/kept", "engines/openai"]
    );

    // The daemon's keyed revert restores the deleted value from the sealed
    // spill; a replay answers from the record.
    let (deleted, _) = effects[2];
    assert_eq!(
        daemon
            .revert_keystore(deleted, "k8-delete")
            .await
            .unwrap_or_else(|error| panic!("revert: {error:?}")),
        RevertResolution::Reverted
    );
    assert_eq!(daemon.keystore_effects().len(), 2);
    assert_eq!(
        daemon
            .revert_keystore(deleted, "k8-delete")
            .await
            .unwrap_or_else(|error| panic!("replay: {error:?}")),
        RevertResolution::Reverted
    );

    // Dispose — the entry leaving the profile — withdraws the keystore
    // trail LIFO: the consumed delete is a clean no-op, both puts restore
    // prior absence; every withdrawal is ledgered in reverse order.
    std::fs::write(home.0.join("profile.json"), r#"{ "entries": [] }"#)
        .unwrap_or_else(|error| panic!("{error}"));
    daemon
        .reload()
        .await
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    let order: Vec<String> = events(&daemon)
        .await
        .into_iter()
        .filter_map(|record| match record.kind {
            LedgerEventKind::EffectWithdrawn { label, clean: true }
                if label.starts_with("keystore ") =>
            {
                Some(
                    label
                        .split(" [effect")
                        .next()
                        .unwrap_or_default()
                        .to_owned(),
                )
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        order,
        vec![
            "keystore delete engines/openai",
            "keystore put engines/kept",
            "keystore put engines/openai",
        ],
        "strictly LIFO, each on the record: {:?}",
        events(&daemon)
            .await
            .iter()
            .filter(|r| matches!(
                r.kind,
                LedgerEventKind::EffectWithdrawn { .. } | LedgerEventKind::ErrorRecorded { .. }
            ))
            .map(|r| format!("{:?}", r.kind))
            .collect::<Vec<_>>()
    );
    assert!(
        daemon.keystore_effects().is_empty(),
        "dispose reclaims the trail: {:?} / {:?}",
        daemon.keystore_effects(),
        events(&daemon)
            .await
            .iter()
            .filter(|r| matches!(
                r.kind,
                LedgerEventKind::EffectWithdrawn { .. } | LedgerEventKind::ErrorRecorded { .. }
            ))
            .map(|r| format!("{:?}", r.kind))
            .collect::<Vec<_>>()
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));

    // The verifier's grep: the value bytes appear nowhere the daemon
    // wrote — not in the ledger file (and its WAL), the sealed store, the
    // master key, an inverse, or the data root. (The fixture ARTIFACT
    // embeds the literal, so the walk names the daemon's outputs, not
    // the artifacts directory.)
    let mut disk = bytes_under(&paths.keystore());
    disk.extend(bytes_under(&paths.inverses()));
    disk.extend(bytes_under(&paths.data));
    for ledger in std::fs::read_dir(&home.0).into_iter().flatten().flatten() {
        if ledger
            .file_name()
            .to_string_lossy()
            .starts_with("ledger.sqlite")
        {
            disk.extend(std::fs::read(ledger.path()).unwrap_or_default());
        }
    }
    assert!(!contains(&disk, SECRET), "the secret is sealed at rest");
    assert!(!contains(&disk, KEPT), "the kept value is sealed at rest");
    assert!(
        contains(&disk, b"engines/openai"),
        "the key NAME is on the record"
    );
}
