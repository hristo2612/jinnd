use jinnd_api::FiberState;

use crate::harness::{booted, entry, home, paths, reload, settle, state, wait_json};
use crate::ledger::{COUNTER, events, failed, loads};

const SETTINGS: &str = "jinn:test/settings";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutually_declared_entries_rest_pending_and_say_so() {
    let home = home("mutual");
    let mutual = [
        entry(
            "alpha",
            serde_json::json!([SETTINGS, COUNTER]),
            serde_json::json!([COUNTER]),
            "settings-provider",
        ),
        entry(
            "beta",
            serde_json::json!([COUNTER, SETTINGS]),
            serde_json::json!([SETTINGS]),
            "provider",
        ),
    ];
    let (paths, hash) = paths(&home, &mutual);
    let data = paths.data.clone();
    let daemon = booted(paths).await;
    settle(&daemon).await;
    let records = events(&daemon).await;
    for parked in ["alpha", "beta"] {
        assert_eq!(state(&daemon, parked), Some(FiberState::Pending));
        assert_eq!(loads(&records, parked), 0);
        assert!(!failed(&records, parked));
    }

    let mut with_viewer = mutual.to_vec();
    with_viewer.push(entry(
        "viewer",
        serde_json::json!(["jinn:introspect", "jinn:fs", "jinn:clock"]),
        serde_json::json!([]),
        "introspect",
    ));
    reload(&daemon, &home, &with_viewer, &hash).await;
    let entries = wait_json(&data.join("introspect-entries.json")).await;
    let read = |id: &str| {
        entries
            .as_array()
            .and_then(|entries| entries.iter().find(|entry| entry["id"] == id))
            .cloned()
            .unwrap_or_else(|| panic!("{id} is listed: {entries}"))
    };
    let alpha = read("alpha");
    assert_eq!(alpha["state"], "pending");
    assert_eq!(alpha["injects"], serde_json::json!([COUNTER]));
    assert_eq!(alpha["unmet"], serde_json::json!([COUNTER]));
    let beta = read("beta");
    assert_eq!(beta["state"], "pending");
    assert_eq!(beta["injects"], serde_json::json!([SETTINGS]));
    assert_eq!(beta["unmet"], serde_json::json!([SETTINGS]));
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}
