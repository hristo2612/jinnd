//! Round-2 ruling 3 (R5, I1), red-first: effect identity in the retained
//! journal is (contract, effect id) — the fs and keystore stores each mint
//! from their own epoch, so a seat's first fs effect and first keystore
//! effect share a bare id. Suspension (a reconcile restart) retains BOTH,
//! and the entry's removal withdraws both.

use jinnd_api::LedgerEventKind;

use super::{booted, events, home, paths, support};

fn write_profile(home: &super::Home, hash: &str, entries: serde_json::Value) {
    let profile = serde_json::json!({ "entries": entries });
    let _ = hash;
    std::fs::write(
        home.0.join("profile.json"),
        serde_json::to_string_pretty(&profile).unwrap_or_else(|error| panic!("{error}")),
    )
    .unwrap_or_else(|error| panic!("{error}"));
}

fn holder(hash: &str, revision: u64) -> serde_json::Value {
    serde_json::json!({
        "id": "holder",
        "package": "demo/counter-plugin",
        "version": "0.0.1",
        "hash": hash,
        "config": {
            "grants": ["jinn:fs", { "contract": "jinn:keystore", "scope": ["engines/"] }],
            "data": "keystore",
            "revision": revision,
        },
    })
}

#[tokio::test]
async fn an_fs_effect_and_a_keystore_effect_sharing_an_id_both_survive_suspension() {
    let home = home("journal-identity");
    let paths = paths(
        &home,
        serde_json::json!(["jinn:fs", { "contract": "jinn:keystore", "scope": ["engines/"] }]),
        "keystore",
    );
    let (_, hash) = support::pinned_fixture();
    let daemon = booted(paths.clone()).await;
    let fiber = daemon
        .entry_fiber("holder")
        .unwrap_or_else(|| panic!("the entry has a fiber"));
    assert!(paths.data.join("keystore.out").exists());
    // The mode's first keystore effect and its one fs effect: fresh stores
    // mint the same first id.
    assert_eq!(daemon.fs_effects().len(), 1);

    // A config restate: the incarnation suspends and its successor inherits
    // the entry's journal — three keystore effects AND the fs write.
    write_profile(&home, &hash, serde_json::json!([holder(&hash, 2)]));
    let report = daemon
        .reload()
        .await
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    assert_eq!(report.restarted.len(), 1);
    let records = events(&daemon).await;
    assert!(
        records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::FiberSuspended { retained: 4 }
        ) && record.fiber == Some(fiber)),
        "suspension retained all four world effects: {:?}",
        records
            .iter()
            .filter(|record| matches!(&record.kind, LedgerEventKind::FiberSuspended { .. }))
            .collect::<Vec<_>>()
    );

    // Removal withdraws the whole trail of both incarnations: the fs
    // document is gone and no fs effect stays live.
    write_profile(&home, &hash, serde_json::json!([]));
    let report = daemon
        .reload()
        .await
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    assert_eq!(report.disposed.len(), 1);
    assert!(
        !paths.data.join("keystore.out").exists(),
        "the first incarnation's fs write withdrew"
    );
    assert!(daemon.fs_effects().is_empty(), "no orphaned fs effect");
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}
