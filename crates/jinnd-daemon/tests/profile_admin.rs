//! M2-K23 acceptance for `jinn:profile-admin` 0.1.0 through the real
//! daemon (harness finding 37): an ADMIN guest holding the grant `["*"]`
//! runs the five writes in turn — each ONE `ProfileAdministered` row under
//! its entry with `before` ≠ `after`, the digests chained, the last
//! `after` equal to the on-disk file's SHA-256 read independently, `prior`
//! the entry as read before the write; an add is reversed by its recorded
//! remove ACROSS A DAEMON REOPEN, the digest returning to the first row's
//! `before`; a viewer without the grant, a scoped admin outside its scope,
//! a self-write, a malformed record and a removal with children refuse
//! typed with nothing written; and (d): a `patch-entry` carrying `grants`
//! is refused with the entry NOT restarted.

mod support;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use jinnd_api::{
    EntryId, FiberState, LedgerEventKind, LedgerRecord, ProfileWrite, RefusalReason,
    TransitionCause,
};
use jinnd_daemon::{Daemon, DaemonPaths};

struct Home(PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn home(name: &str) -> Home {
    let root =
        std::env::temp_dir().join(format!("jinnd-profile-admin-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
    std::fs::create_dir_all(root.join("data")).unwrap_or_else(|error| panic!("{error}"));
    Home(root)
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

/// Writes the profile with every `demo/counter-plugin` entry pinned to the
/// fixture's true hash, and a second package `demo/counter-copy` — the
/// same component under another name, so a swap between them is a real
/// `Replace` on a lane that exists.
fn paths(home: &Home, entries: Vec<serde_json::Value>) -> (DaemonPaths, String) {
    let (bytes, hash) = support::pinned_fixture();
    for name in ["counter-plugin", "counter-copy"] {
        std::fs::write(home.0.join(format!("artifacts/{name}.wasm")), &bytes)
            .unwrap_or_else(|error| panic!("{error}"));
    }
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

fn script(paths: &DaemonPaths, name: &str, lines: &[&str]) {
    std::fs::write(
        paths.data.join(format!("admin-{name}-script.txt")),
        lines.join("\n"),
    )
    .unwrap_or_else(|error| panic!("{error}"));
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

async fn wait_for(path: &std::path::Path) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(bytes) = std::fs::read(path) {
            return bytes;
        }
        assert!(Instant::now() < deadline, "{} lands", path.display());
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// The on-disk document's SHA-256, read with nothing but the file.
fn disk_digest(paths: &DaemonPaths) -> String {
    jinnd_wasm::hex_digest(&std::fs::read(&paths.profile).unwrap_or_else(|error| panic!("{error}")))
}

fn administered(records: &[LedgerRecord]) -> Vec<&LedgerRecord> {
    records
        .iter()
        .filter(|record| matches!(record.kind, LedgerEventKind::ProfileAdministered { .. }))
        .collect()
}

fn document(paths: &DaemonPaths) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(&paths.profile).unwrap_or_else(|error| panic!("{error}")))
        .unwrap_or_else(|error| panic!("json: {error}"))
}

fn ids(paths: &DaemonPaths) -> Vec<String> {
    document(paths)["entries"]
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry["id"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

/// Case 2 shape: the five writes in turn, one row each, digests chained
/// and the last equal to the file's; `prior` as read before the write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_of_the_five_writes_lands_one_row_with_the_caller_and_both_digests() {
    let home = home("five");
    let (paths, hash) = paths(
        &home,
        vec![
            entry(
                "admin",
                serde_json::json!([
                    "jinn:fs",
                    "jinn:clock",
                    { "contract": "jinn:profile-admin", "scope": ["*"] }
                ]),
                "admin:a",
            ),
            entry("target", serde_json::json!(["jinn:fs"]), "plain"),
            serde_json::json!({
                "id": "anchor", "package": "demo/counter-copy", "version": "0.0.1",
                "hash": "", "config": { "grants": ["jinn:fs"], "data": "plain" }
            }),
        ],
    );
    let added = format!(
        r#"{{"id":"added","package":"demo/counter-plugin","version":"0.0.1","hash":"{hash}","grants":["jinn:fs"],"config":{{"data":"plain"}}}}"#
    );
    let swap = format!("swap-plugin\ttarget\tdemo/counter-copy\t0.0.1\t{hash}");
    script(
        &paths,
        "a",
        &[
            &format!("add-entry\t{added}"),
            "set-disabled\ttarget\ttrue",
            "set-disabled\ttarget\tfalse",
            "set-grants\ttarget\t[\"jinn:fs\",\"jinn:clock\"]",
            &swap,
            "remove-entry\tadded",
        ],
    );
    let daemon = booted(paths.clone()).await;
    // The boot reconcile's own write-back is the baseline every digest
    // chains from (the loader renders; the fixture's pretty-print is not
    // the document of record's bytes).
    let boot_digest = disk_digest(&paths);
    for index in 0..6 {
        let answer = wait_for(&paths.data.join(format!("admin-a-{index}.out"))).await;
        assert_eq!(
            answer.first(),
            Some(&2),
            "write {index} accepted: {answer:?}"
        );
    }
    let records = events(&daemon).await;
    let rows = administered(&records);
    assert_eq!(rows.len(), 6, "one row per write: {rows:?}");
    let expected = [
        ("added", ProfileWrite::Add),
        ("target", ProfileWrite::SetDisabled),
        ("target", ProfileWrite::SetDisabled),
        ("target", ProfileWrite::SetGrants),
        ("target", ProfileWrite::SwapPlugin),
        ("added", ProfileWrite::Remove),
    ];
    let mut previous_after = boot_digest;
    for (row, (id, write)) in rows.iter().zip(expected) {
        assert_eq!(
            row.entry,
            Some(EntryId("admin".to_owned())),
            "under the caller"
        );
        let LedgerEventKind::ProfileAdministered {
            entry,
            by,
            write: recorded,
            before,
            after,
            prior,
        } = &row.kind
        else {
            unreachable!()
        };
        assert_eq!(entry.0, id);
        assert_eq!(by, "admin");
        assert_eq!(*recorded, write);
        assert_ne!(before, after, "{write:?} changed the document");
        assert_eq!(
            *before, previous_after,
            "{write:?}'s before is the last after"
        );
        previous_after = after.clone();
        match write {
            ProfileWrite::Add => assert!(prior.is_none(), "no prior on add"),
            _ => {
                let prior: serde_json::Value =
                    serde_json::from_str(prior.as_deref().unwrap_or("null"))
                        .unwrap_or_else(|error| panic!("prior parses: {error}"));
                assert_eq!(prior["id"], id, "prior is the entry's record");
                assert!(prior["hash"].as_str().is_some_and(|hash| hash.len() == 64));
            }
        }
    }
    assert_eq!(
        previous_after,
        disk_digest(&paths),
        "after == the file's digest"
    );
    // The enable's prior shows the disabled record; the swap's prior the
    // old package; the grants prior the old grants.
    let priors: Vec<serde_json::Value> = rows
        .iter()
        .filter_map(|row| match &row.kind {
            LedgerEventKind::ProfileAdministered {
                prior: Some(prior), ..
            } => serde_json::from_str(prior).ok(),
            _ => None,
        })
        .collect();
    assert_eq!(priors[1]["disabled"], true, "enable's prior is disabled");
    assert_eq!(priors[2]["grants"], serde_json::json!(["jinn:fs"]));
    assert_eq!(priors[3]["package"], "demo/counter-plugin");
    let target = document(&paths)["entries"]
        .as_array()
        .and_then(|entries| {
            entries
                .iter()
                .find(|entry| entry["id"] == "target")
                .cloned()
        })
        .unwrap_or_default();
    assert_eq!(
        target["package"], "demo/counter-copy",
        "the swap landed on disk"
    );
    assert_ne!(
        target["disabled"], true,
        "enabled again (false is rendered by omission)"
    );
    assert_eq!(
        target["config"]["grants"],
        serde_json::json!(["jinn:fs", "jinn:clock"])
    );
    assert_eq!(
        ids(&paths),
        vec!["admin", "target", "anchor"],
        "added is gone"
    );
    // The disable was a disposal and the enable a fresh incarnation, on
    // the record; the grants change and the swap each restarted.
    let target_disposed = records.iter().any(|record| matches!(&record.kind, LedgerEventKind::FiberTransition(transition) if transition.to == FiberState::Disposed) && record.entry == Some(EntryId("target".to_owned())));
    assert!(target_disposed, "disable disposed the target: {records:?}");
    let restarted = records.iter().filter(|record| matches!(&record.kind, LedgerEventKind::FiberTransition(transition) if transition.cause == TransitionCause::ConfigChanged && transition.to == FiberState::Active) && record.entry == Some(EntryId("target".to_owned()))).count();
    assert!(restarted >= 1, "the grants change landed through a restart");
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// Case 3 shape: an add is reversed by its recorded remove across a
/// daemon reopen; the digest returns to the first row's `before`, which
/// is the file's digest before anything was written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_add_is_reversed_by_its_recorded_remove_across_a_daemon_reopen() {
    let home = home("reopen");
    let (paths, hash) = paths(
        &home,
        vec![
            entry(
                "admin",
                serde_json::json!([
                    "jinn:fs",
                    "jinn:clock",
                    { "contract": "jinn:profile-admin", "scope": ["*"] }
                ]),
                "admin:r",
            ),
            entry("target", serde_json::json!(["jinn:fs"]), "plain"),
        ],
    );
    let add = format!(
        r#"add-entry	{{"id":"added","package":"demo/counter-plugin","version":"0.0.1","hash":"{hash}","grants":["jinn:fs"],"config":{{"data":"plain"}}}}"#
    );
    script(&paths, "r", &[&add]);
    let daemon = booted(paths.clone()).await;
    let original = disk_digest(&paths);
    let answer = wait_for(&paths.data.join("admin-r-0.out")).await;
    assert_eq!(answer.first(), Some(&2), "add accepted: {answer:?}");
    assert_eq!(ids(&paths), vec!["admin", "target", "added"]);
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));

    // Reopen; the row's recorded inverse is `remove-entry(added)`.
    script(&paths, "r", &[&add, "remove-entry\tadded"]);
    let daemon = booted(paths.clone()).await;
    let answer = wait_for(&paths.data.join("admin-r-1.out")).await;
    assert_eq!(answer.first(), Some(&2), "remove accepted: {answer:?}");
    let records = events(&daemon).await;
    let rows = administered(&records);
    assert_eq!(
        rows.len(),
        2,
        "add then remove survive the reopen: {rows:?}"
    );
    let (
        LedgerEventKind::ProfileAdministered { before: first, .. },
        LedgerEventKind::ProfileAdministered {
            after: last, write, ..
        },
    ) = (&rows[0].kind, &rows[1].kind)
    else {
        unreachable!()
    };
    assert_eq!(*write, ProfileWrite::Remove);
    assert_eq!(*first, original, "the first before is the original file");
    assert_eq!(
        last, first,
        "the remove returns the digest to the add's before"
    );
    assert_eq!(
        disk_digest(&paths),
        original,
        "the file is byte-identical to the original"
    );
    assert_eq!(ids(&paths), vec!["admin", "target"]);
    assert_eq!(
        daemon.fiber_state(
            daemon
                .entry_fiber("target")
                .unwrap_or_else(|| panic!("target live"))
        ),
        Some(FiberState::Active)
    );
    assert!(daemon.entry_fiber("added").is_none(), "added has no fiber");
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// Cases 1/10/11/12 shape: refusals are typed on the wire and on the
/// record with NOTHING written — the digest is unchanged, zero rows, the
/// target never restarted — and (d) closes the `patch-entry` door.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refusals_write_nothing_and_a_grants_patch_through_patch_entry_is_refused() {
    let home = home("refused");
    let (paths, _) = paths(
        &home,
        vec![
            entry(
                "scoped",
                serde_json::json!([
                    "jinn:fs",
                    "jinn:clock",
                    { "contract": "jinn:profile-admin", "scope": ["target", "leaf", "scoped"] }
                ]),
                "admin:s",
            ),
            entry("target", serde_json::json!(["jinn:fs"]), "plain"),
            serde_json::json!({
                "id": "leaf", "package": "demo/counter-plugin", "version": "0.0.1",
                "hash": "", "parent": "target",
                "config": { "grants": ["jinn:fs"], "data": "plain" }
            }),
            entry("other", serde_json::json!(["jinn:fs"]), "plain"),
            entry(
                "editor",
                serde_json::json!([
                    "jinn:fs",
                    "jinn:clock",
                    { "contract": "jinn:profile", "scope": ["other"] }
                ]),
                "profile-patch-grants:other",
            ),
        ],
    );
    script(
        &paths,
        "s",
        &[
            "remove-entry\tother",     // outside the scope: unauthorized
            "remove-entry\tscoped",    // itself, inside its scope: unauthorized
            "set-grants\ttarget\t[7]", // grants that refuse: malformed
            "remove-entry\ttarget",    // has a child: irreversible
            "remove-entry\tleaf",      // in scope, a leaf: accepted last
        ],
    );
    let daemon = booted(paths.clone()).await;
    let original = disk_digest(&paths);
    let target = daemon
        .entry_fiber("target")
        .unwrap_or_else(|| panic!("target live"));
    let mut classes = Vec::new();
    for index in 0..4 {
        let answer = wait_for(&paths.data.join(format!("admin-s-{index}.out"))).await;
        assert_eq!(
            answer.first(),
            Some(&1),
            "write {index} refused: {answer:?}"
        );
        classes.push(answer[1]);
    }
    assert_eq!(
        classes,
        vec![1, 1, 2, 4],
        "unauthorized, unauthorized, malformed, irreversible"
    );
    let patch = wait_for(&paths.data.join("patch.out")).await;
    assert_eq!(
        patch.first(),
        Some(&1),
        "(d): the grants patch is refused: {patch:?}"
    );
    assert!(
        String::from_utf8_lossy(&patch[1..]).contains("jinn:profile-admin"),
        "the refusal names whose the grants are: {patch:?}"
    );
    assert_eq!(
        disk_digest(&paths),
        original,
        "nothing was written by any refusal"
    );
    let records = events(&daemon).await;
    assert!(
        records.iter().any(|record| matches!(&record.kind, LedgerEventKind::GrantRefused { contract, reason: RefusalReason::ScopeMismatch, .. } if contract == "jinn:profile-admin") && record.entry == Some(EntryId("scoped".to_owned()))),
        "the scope refusal is the broker's GrantRefused: {records:?}"
    );
    let refused = records.iter().filter(|record| matches!(&record.kind, LedgerEventKind::AmendmentRefused { detail } if detail.contains("refused (")) && record.entry == Some(EntryId("scoped".to_owned()))).count();
    assert_eq!(
        refused, 3,
        "self, malformed and irreversible are each a row"
    );
    assert!(
        records.iter().any(|record| matches!(&record.kind, LedgerEventKind::AmendmentRefused { detail } if detail.contains("jinn:profile-admin")) && record.entry == Some(EntryId("editor".to_owned()))),
        "(d) is a row under the editor"
    );
    assert!(
        !records.iter().any(|record| matches!(&record.kind, LedgerEventKind::FiberTransition(transition) if transition.fiber == target && transition.cause == TransitionCause::ConfigChanged)),
        "the target was never restarted by a refusal"
    );
    assert!(
        !records.iter().any(|record| matches!(&record.kind, LedgerEventKind::FiberTransition(transition) if transition.cause == TransitionCause::ConfigChanged) && record.entry == Some(EntryId("other".to_owned()))),
        "(d): other was never restarted"
    );
    // The engagement is not left held: the in-scope leaf removal lands.
    let answer = wait_for(&paths.data.join("admin-s-4.out")).await;
    assert_eq!(
        answer.first(),
        Some(&2),
        "a following write succeeds: {answer:?}"
    );
    assert_eq!(administered(&events(&daemon).await).len(), 1);
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}
