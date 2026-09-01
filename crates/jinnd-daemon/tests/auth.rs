//! M2-K21 acceptance through the REAL guest path (a wasm component over
//! `services.resolve("jinn:auth")` + `services.call`): one entry presents
//! the operator's credential and is granted `operator`; a sibling presents
//! a wrong one and is `unauthenticated` on the contract's own wire; both
//! decisions land as `AuthDecided` rows attributed to the calling ENTRY,
//! carrying the name and the digest and never the credential; an entry
//! without the grant cannot resolve the contract at all (Law 1, Law 2).
//! The provider-level matrix (rotation, file preconditions, no-effect) is
//! `src/daemon/auth_cap/tests.rs`; this file proves the composition.

mod support;

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use jinnd_api::{EntryId, FiberState, LedgerEventKind};
use jinnd_daemon::{Daemon, DaemonPaths, MasterKeySource};

const TOKEN: &str = "operator-token-0xFEEDFACE-guest-side";

struct Home(PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn entry(id: &str, grants: serde_json::Value, mode: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "package": "demo/counter-plugin",
        "version": "0.0.1",
        "hash": "",
        "config": { "grants": grants, "data": mode },
    })
}

fn rig(entries: serde_json::Value) -> (Home, DaemonPaths) {
    let root = std::env::temp_dir().join(format!("jinnd-auth-guest-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
    let (bytes, hash) = support::pinned_fixture();
    std::fs::write(root.join("artifacts/counter-plugin.wasm"), &bytes)
        .unwrap_or_else(|error| panic!("{error}"));
    let entries: Vec<serde_json::Value> = entries
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .map(|entry| {
                    let mut entry = entry.clone();
                    entry["hash"] = serde_json::Value::String(hash.clone());
                    entry
                })
                .collect()
        })
        .unwrap_or_default();
    std::fs::write(
        root.join("profile.json"),
        serde_json::to_string_pretty(&serde_json::json!({ "entries": entries }))
            .unwrap_or_else(|error| panic!("{error}")),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    let paths = DaemonPaths {
        profile: root.join("profile.json"),
        ledger: root.join("ledger.sqlite"),
        artifacts: root.join("artifacts"),
        data: root.join("data"),
    };
    // The launcher's half: the credential, mode 0600, beside the data root.
    let credential = paths.credential();
    std::fs::write(&credential, TOKEN).unwrap_or_else(|error| panic!("{error}"));
    std::fs::set_permissions(&credential, std::fs::Permissions::from_mode(0o600))
        .unwrap_or_else(|error| panic!("{error}"));
    (Home(root), paths)
}

async fn wait_for_file(path: &std::path::Path) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(bytes) = std::fs::read(path) {
            return bytes;
        }
        assert!(Instant::now() < deadline, "{} lands", path.display());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_guest_is_granted_or_unauthenticated_on_the_record_under_its_own_entry() {
    let (_home, paths) = rig(serde_json::json!([
        entry(
            "right",
            serde_json::json!(["jinn:auth", "jinn:fs"]),
            &format!("auth:right:{TOKEN}")
        ),
        entry(
            "wrong",
            serde_json::json!(["jinn:auth", "jinn:fs"]),
            "auth:wrong:operator-token-0xBADC0FFEE-guest-wrong"
        ),
        entry(
            "ungranted",
            serde_json::json!(["jinn:fs"]),
            &format!("auth:ungranted:{TOKEN}")
        ),
    ]));
    let daemon = Daemon::open_with(paths.clone(), MasterKeySource::Absent)
        .unwrap_or_else(|error| panic!("open: {error:?}"));
    daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot: {error:?}"));
    let right = wait_for_file(&paths.data.join("auth-answer-right")).await;
    assert_eq!(right, b"\x00operator".to_vec(), "granted as operator");
    let wrong = wait_for_file(&paths.data.join("auth-answer-wrong")).await;
    assert_eq!(
        wrong.first(),
        Some(&1),
        "unauthenticated on the wire: {wrong:?}"
    );
    assert!(
        !String::from_utf8_lossy(&wrong).contains(TOKEN),
        "the reason carries no credential"
    );
    for (id, state) in [
        ("right", FiberState::Active),
        ("wrong", FiberState::Active),
        ("ungranted", FiberState::Failed),
    ] {
        let fiber = daemon
            .entry_fiber(id)
            .unwrap_or_else(|| panic!("{id} has a fiber"));
        assert_eq!(daemon.fiber_state(fiber), Some(state), "{id}");
    }
    let records = daemon
        .ledger_events()
        .await
        .unwrap_or_else(|error| panic!("ledger read: {error:?}"));
    let decisions: Vec<(Option<String>, Option<String>, bool)> = records
        .iter()
        .filter_map(|record| match &record.kind {
            LedgerEventKind::AuthDecided { name, granted, .. } => Some((
                record.entry.as_ref().map(|entry| entry.0.clone()),
                name.clone(),
                *granted,
            )),
            _ => None,
        })
        .collect();
    assert!(
        decisions.contains(&(Some("right".to_owned()), Some("operator".to_owned()), true)),
        "the grant is attributed to its ENTRY: {decisions:?}"
    );
    assert!(
        decisions.contains(&(Some("wrong".to_owned()), None, false)),
        "the refusal is attributed to its ENTRY: {decisions:?}"
    );
    assert_eq!(
        decisions.len(),
        2,
        "the ungranted entry never reached the decision point"
    );
    // The ungranted entry could not even RESOLVE the contract: the broker
    // refused it on the record under its entry, and its activation failed
    // contained to itself (R11) — the sibling that presented the same
    // bytes with a grant was untouched.
    assert!(
        records.iter().any(|record| matches!(&record.kind, LedgerEventKind::GrantRefused { contract, .. } if contract == "jinn:auth")
            && record.entry == Some(EntryId("ungranted".to_owned()))),
        "no grant, no handle, on the record: {records:?}"
    );
    let rendered = serde_json::to_string(&records).unwrap_or_else(|error| panic!("{error}"));
    assert!(
        !rendered.contains(TOKEN),
        "the credential's bytes are in no ledger row"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}
