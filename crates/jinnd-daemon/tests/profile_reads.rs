//! M2-K8 acceptance for `jinn:profile` 0.2.0, through the real daemon
//! (harness findings 25/26): a settings provider patches its OWNER from
//! inside a handler while the owner resolves the provider from `activate`
//! — the two-hop shape — and the patch answers `accepted(seq)` without
//! the nested-dispatch deadlock: the owner restarts, Active, its second
//! activation served by the same provider instance; and a read-only
//! viewer reads `entry`/`document` (authority fields) while its
//! `patch-entry` is refused by the operation class.

mod support;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use jinnd_api::{EntryId, FiberState, LedgerEventKind, LedgerRecord, TransitionCause};
use jinnd_daemon::{Daemon, DaemonPaths};

struct Home(PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn home(name: &str) -> Home {
    let root =
        std::env::temp_dir().join(format!("jinnd-profile-reads-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
    Home(root)
}

fn entry(id: &str, hash: &str, grants: serde_json::Value, mode: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "package": "demo/counter-plugin",
        "version": "0.0.1",
        "hash": hash,
        "config": { "grants": grants, "data": mode },
    })
}

fn paths(home: &Home, entries: Vec<serde_json::Value>) -> DaemonPaths {
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

async fn wait_for(path: &std::path::Path, ready: impl Fn(&[u8]) -> bool) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(bytes) = std::fs::read(path)
            && ready(&bytes)
        {
            return bytes;
        }
        assert!(Instant::now() < deadline, "{} lands", path.display());
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn json(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).unwrap_or_else(|error| panic!("json: {error}"))
}

/// #26, red-first: with a patch that awaits the restart, the owner's
/// second `activate` calls a provider instance that is mid-`patch`
/// waiting for that very restart — held to the guest deadline, the owner
/// fails. With the deferred amendment the trigger sees `accepted(seq)`,
/// the owner's log shows both activations served, the owner is Active
/// on its new config, and no owner transition ever reached `Failed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_provider_patches_its_owner_from_a_handler_without_the_two_hop_deadlock() {
    let home = home("two-hop");
    let paths = paths(
        &home,
        vec![
            entry(
                "settings",
                "",
                serde_json::json!([
                    "jinn:test/settings",
                    { "contract": "jinn:profile", "scope": ["owner"] }
                ]),
                "settings-provider",
            ),
            entry(
                "owner",
                "",
                serde_json::json!(["jinn:test/settings", "jinn:fs", "jinn:clock"]),
                "settings-owner",
            ),
            entry(
                "trigger",
                "",
                serde_json::json!(["jinn:test/settings", "jinn:fs", "jinn:clock"]),
                "settings-trigger:owner",
            ),
        ],
    );
    let daemon = booted(paths.clone()).await;
    let owner = daemon
        .entry_fiber("owner")
        .unwrap_or_else(|| panic!("owner live"));
    let answer = wait_for(&paths.data.join("trigger.out"), |_| true).await;
    assert_eq!(answer.first(), Some(&2), "accepted: {answer:?}");
    let mut seq = [0u8; 8];
    seq.copy_from_slice(&answer[1..9]);
    let seq = u64::from_le_bytes(seq);
    let log = wait_for(&paths.data.join("owner.log"), |bytes| bytes == b"v1\nv1\n").await;
    assert_eq!(log, b"v1\nv1\n", "both activations were served");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        daemon.sync_transitions();
        if daemon.fiber_state(owner) == Some(FiberState::Active)
            && events(&daemon).await.iter().any(|record| matches!(&record.kind, LedgerEventKind::FiberTransition(transition) if transition.fiber == owner && transition.cause == TransitionCause::ConfigChanged && transition.to == FiberState::Active))
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the owner's scheduled restart lands Active"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let records = events(&daemon).await;
    assert!(
        !records.iter().any(|record| matches!(&record.kind, LedgerEventKind::FiberTransition(transition) if transition.fiber == owner && transition.to == FiberState::Failed)),
        "the owner never failed: {records:?}"
    );
    let patched = records
        .iter()
        .find(|record| matches!(&record.kind, LedgerEventKind::ProfilePatched { entry, by } if entry.0 == "owner" && by == "settings"))
        .unwrap_or_else(|| panic!("ProfilePatched lands: {records:?}"));
    assert_eq!(patched.sequence, seq, "the answered seq IS the record's");
    assert!(
        records.iter().any(|record| matches!(&record.kind, LedgerEventKind::FiberTransition(transition) if transition.fiber == owner && transition.cause == TransitionCause::ConfigChanged) && record.sequence > seq),
        "the restart's transitions land after the receipt"
    );
    let document = json(&std::fs::read(&paths.profile).unwrap_or_else(|error| panic!("{error}")));
    let owner_data = document["entries"]
        .as_array()
        .and_then(|entries| entries.iter().find(|entry| entry["id"] == "owner"))
        .map(|entry| entry["config"]["data"].clone());
    assert_eq!(owner_data, Some(serde_json::json!("settings-owner:v2")));
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// #25: a viewer holding `{ scope: ["*"], ops: ["entry", "document"] }`
/// reads its sibling's authority fields (package, version, hash, grants
/// as written, config) and the whole document, each a ledgered contract
/// call under its entry; its `patch-entry` is refused by the operation
/// class, on the record, and the document is untouched.
#[tokio::test]
async fn a_read_only_viewer_reads_authority_fields_and_cannot_patch() {
    let home = home("viewer");
    let paths = paths(
        &home,
        vec![
            entry("worker", "", serde_json::json!(["jinn:fs"]), "plain"),
            entry(
                "viewer",
                "",
                serde_json::json!([
                    "jinn:fs",
                    { "contract": "jinn:profile", "scope": ["*"], "ops": ["entry", "document"] }
                ]),
                "profile-read:worker",
            ),
        ],
    );
    let daemon = booted(paths.clone()).await;
    let entry_view = json(&wait_for(&paths.data.join("profile-entry.json"), |_| true).await);
    assert_eq!(entry_view["id"], "worker");
    assert_eq!(entry_view["package"], "demo/counter-plugin");
    assert_eq!(entry_view["version"], "0.0.1");
    assert!(
        entry_view["hash"]
            .as_str()
            .is_some_and(|hash| hash.len() == 64)
    );
    assert_eq!(entry_view["grants"], serde_json::json!(["jinn:fs"]));
    assert_eq!(entry_view["config"]["data"], "plain");
    assert_eq!(entry_view["disabled"], false);
    let document = json(&wait_for(&paths.data.join("profile-document.json"), |_| true).await);
    let ids: Vec<&str> = document["entries"]
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry["id"].as_str())
                .collect()
        })
        .unwrap_or_default();
    assert_eq!(ids, vec!["worker", "viewer"]);
    wait_for(&paths.data.join("profile-read-denied"), |_| true).await;
    let records = events(&daemon).await;
    for op in ["entry", "document"] {
        assert!(
            records.iter().any(|record| matches!(&record.kind, LedgerEventKind::ContractCall { contract, operation } if contract == "jinn:profile" && operation == op)
                && record.entry == Some(EntryId("viewer".to_owned()))),
            "the {op} read is a ledgered contract call under the viewer"
        );
    }
    assert!(
        records.iter().any(|record| matches!(&record.kind, LedgerEventKind::GrantRefused { contract, detail: Some(detail), .. } if contract == "jinn:profile" && detail.contains("patch-entry"))
            && record.entry == Some(EntryId("viewer".to_owned()))),
        "the patch is refused by the operation class, on the record: {records:?}"
    );
    assert!(
        !records
            .iter()
            .any(|record| matches!(record.kind, LedgerEventKind::ProfilePatched { .. })),
        "nothing was patched"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}
