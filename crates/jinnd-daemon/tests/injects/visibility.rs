//! Law 2 for the gate: an entry resting `Pending` on a declaration says
//! WHICH declared contracts are unmet on `jinn:introspect` (0.6.0), and
//! the contract text says so by PARSING, never by `contains` (M2-K16).

use jinnd_api::FiberState;

use crate::harness::{
    COUNTER, booted, declared, entry, events, failed, home, loads, paths, provider, reload, settle,
    state, wait_json,
};

const SETTINGS: &str = "jinn:test/settings";

/// Two entries that declare each other rest `Pending` forever — neither
/// `Failed`, neither attempted — and both are visible on `jinn:introspect`
/// naming the unmet contract (the recorded limit, I3: cleanly inactive). A
/// satisfied pair beside them shows its declaration with nothing unmet.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutually_declared_entries_rest_pending_and_say_so() {
    let home = home("mutual");
    let mutual = [
        // Provides the counter, but injects the settings first.
        entry(
            "alpha",
            serde_json::json!([COUNTER, SETTINGS]),
            serde_json::json!([SETTINGS]),
            "provider",
        ),
        // Provides the settings, but injects the counter first.
        entry(
            "beta",
            serde_json::json!([SETTINGS, COUNTER]),
            serde_json::json!([COUNTER]),
            "settings-provider",
        ),
        provider("provider"),
        declared("consumer", "inject-counter"),
    ];
    let (paths, hash) = paths(&home, &mutual);
    let data = paths.data.clone();
    let daemon = booted(paths).await;
    settle(&daemon).await;
    let records = events(&daemon).await;
    for parked in ["alpha", "beta"] {
        assert_eq!(
            state(&daemon, parked),
            Some(FiberState::Pending),
            "{parked} rests Pending"
        );
        assert_eq!(loads(&records, parked), 0, "{parked} was never attempted");
        assert!(!failed(&records, parked));
    }
    assert_eq!(state(&daemon, "consumer"), Some(FiberState::Active));
    // The viewer arrives AFTER the composition settled, so its one read
    // reports the rest state, not a moment mid-activation.
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
    assert_eq!(alpha["injects"], serde_json::json!([SETTINGS]));
    assert_eq!(alpha["unmet"], serde_json::json!([SETTINGS]));
    let beta = read("beta");
    assert_eq!(beta["state"], "pending");
    assert_eq!(beta["injects"], serde_json::json!([COUNTER]));
    assert_eq!(beta["unmet"], serde_json::json!([COUNTER]));
    let consumer = read("consumer");
    assert_eq!(consumer["state"], "active");
    assert_eq!(consumer["injects"], serde_json::json!([COUNTER]));
    assert_eq!(consumer["unmet"], serde_json::json!([]));
    let viewer = read("viewer");
    assert_eq!(viewer["injects"], serde_json::json!([]));
    assert_eq!(viewer["unmet"], serde_json::json!([]));
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
}

/// The contract delta, asserted by parsing (R12): `jinn:introspect` is
/// 0.6.0 in every identity copy, and `entry` carries `injects` and `unmet`
/// as lists of contract names.
#[test]
fn the_introspect_contract_declares_the_declaration_fields_at_0_6_0() {
    let bundle = jinnd_contract_lens::bundle("jinn-introspect");
    let wit = bundle.wit().wit();
    assert_eq!(wit.package_id(), "jinn:introspect@0.6.0");
    assert_eq!(bundle.metadata().metadata().version(), "0.6.0");
    let fields = wit.interface("composition").record_fields("entry");
    for field in ["injects: list<string>", "unmet: list<string>"] {
        assert!(
            fields.iter().any(|declared| declared == field),
            "entry declares `{field}`: {fields:?}"
        );
    }
}
