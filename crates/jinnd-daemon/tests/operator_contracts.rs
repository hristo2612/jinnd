//! M2-K7 acceptance, through the real daemon assembly (harness findings
//! 19/20/21/23): a fixture reads the composition it is part of through
//! `jinn:introspect` (its own entry, state, incarnation, registrations,
//! readiness); pages the ledger through `jinn:ledger` with consumption
//! receipts, its own receipts excluded; patches a sibling through
//! `jinn:profile` — exactly the patched fiber restarts (same fiber, new
//! incarnation), the document changes on disk with no fs inverse and no
//! journal entry, and DISPOSING THE EDITOR LEAVES THE DOCUMENT UNCHANGED;
//! a patch outside the scope and a patch failing validation refuse on the
//! record with nothing written; and a listener serves a real TCP peer from
//! the readiness wake alone — zero alarms, wakes bounded under a flood
//! (Law 1, Law 2, R1, R9, R11).

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use jinnd_api::{
    EntryId, FiberState, LedgerEventKind, LedgerRecord, RefusalReason, TransitionCause,
};
use jinnd_daemon::{Daemon, DaemonPaths};

struct Home(PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn home(name: &str) -> Home {
    let root = std::env::temp_dir().join(format!(
        "jinnd-operator-contracts-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
    Home(root)
}

fn write_profile(home: &Home, entries: &serde_json::Value) {
    let profile = serde_json::json!({ "entries": entries });
    std::fs::write(
        home.0.join("profile.json"),
        serde_json::to_string_pretty(&profile).unwrap_or_else(|error| panic!("{error}")),
    )
    .unwrap_or_else(|error| panic!("{error}"));
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

fn paths(home: &Home, entries: serde_json::Value) -> (DaemonPaths, String) {
    let (bytes, hash) = support::pinned_fixture();
    std::fs::write(home.0.join("artifacts/counter-plugin.wasm"), &bytes)
        .unwrap_or_else(|error| panic!("{error}"));
    let entries = entries
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .map(|entry| {
                    let mut entry = entry.clone();
                    entry["hash"] = serde_json::Value::String(hash.clone());
                    entry
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    write_profile(home, &serde_json::Value::Array(entries));
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

fn json_file(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).unwrap_or_else(|error| panic!("json: {error}"))
}

fn count(records: &[LedgerRecord], select: impl Fn(&LedgerEventKind) -> bool) -> usize {
    records.iter().filter(|record| select(&record.kind)).count()
}

/// #19: the fixture lists its own entry with fiber, state, incarnation,
/// provisions, and registrations, and sees the boot readiness; the read is
/// a ledgered contract call attributed to its ENTRY; every attributable
/// event names its entry.
#[tokio::test]
async fn introspect_lists_the_composition_and_readiness_on_the_record() {
    let home = home("introspect");
    let (paths, _) = paths(
        &home,
        serde_json::json!([
            entry(
                "provider",
                "",
                serde_json::json!(["jinn:test/counter", "jinn:clock"]),
                "clock-alarm"
            ),
            entry(
                "viewer",
                "",
                serde_json::json!(["jinn:introspect", "jinn:fs", "jinn:clock"]),
                "introspect"
            ),
        ]),
    );
    let daemon = booted(paths.clone()).await;
    let entries = json_file(&wait_for_file(&paths.data.join("introspect-entries.json")).await);
    let provider = entries
        .as_array()
        .and_then(|entries| entries.iter().find(|entry| entry["id"] == "provider"))
        .unwrap_or_else(|| panic!("the provider entry is listed: {entries}"));
    assert_eq!(
        provider["fiber"].as_u64(),
        daemon.entry_fiber("provider").map(|fiber| fiber.0)
    );
    assert_eq!(provider["state"], "active");
    assert!(provider["incarnation"].is_u64());
    assert_eq!(
        provider["provisions"],
        serde_json::json!(["jinn:test/counter"])
    );
    assert_eq!(provider["registrations"]["alarms"], 1);
    assert_eq!(provider["registrations"]["sockets"], 0);
    let viewer = entries
        .as_array()
        .and_then(|entries| entries.iter().find(|entry| entry["id"] == "viewer"))
        .unwrap_or_else(|| panic!("the viewer sees itself: {entries}"));
    assert_eq!(viewer["state"], "active", "the reader sees itself settled");
    assert_eq!(viewer["registrations"]["alarms"], 1);
    let readiness = json_file(&wait_for_file(&paths.data.join("introspect-readiness.json")).await);
    // Read after the boot reconcile landed; no shell armed a watcher here.
    assert_eq!(
        readiness,
        serde_json::json!({ "boot-reconciled": true, "watcher-armed": false })
    );
    let records = events(&daemon).await;
    let read = records
        .iter()
        .find(|record| matches!(&record.kind, LedgerEventKind::ContractCall { contract, operation } if contract == "jinn:introspect" && operation == "entries"))
        .unwrap_or_else(|| panic!("the read is a ledgered contract call: {records:?}"));
    assert_eq!(
        read.entry,
        Some(EntryId("viewer".to_owned())),
        "attributed to the ENTRY"
    );
    assert!(
        records
            .iter()
            .filter(|record| record.fiber.is_some())
            .all(|record| record.entry.is_some()),
        "every fiber-attributed event names its entry: {records:?}"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// #20: `read-range` pages from 1 with a `next-from`, every delivered event
/// is a typed record with a sensitivity tag, a consumption receipt lands
/// under the reader's attribution, the reader's own receipt is excluded
/// from its second page while `last-seq` counts it.
#[tokio::test]
async fn ledger_reads_page_with_receipts_and_exclude_the_readers_own() {
    let home = home("ledger");
    let (paths, _) = paths(
        &home,
        serde_json::json!([entry(
            "reader",
            "",
            serde_json::json!(["jinn:ledger", "jinn:fs"]),
            "ledger-read"
        )]),
    );
    let daemon = booted(paths.clone()).await;
    let first = json_file(&wait_for_file(&paths.data.join("ledger-page1.json")).await);
    let second = json_file(&wait_for_file(&paths.data.join("ledger-page2.json")).await);
    let last = wait_for_file(&paths.data.join("ledger-last")).await;
    let first_events = first["events"]
        .as_array()
        .unwrap_or_else(|| panic!("{first}"));
    assert!(!first_events.is_empty());
    assert_eq!(first_events[0]["id"], 1);
    assert!(
        first_events.iter().all(|event| event["kind"].is_string()),
        "kind is the canonical name the bundle declares: {first}"
    );
    assert!(
        first_events
            .iter()
            .all(|event| { matches!(event["sensitivity"].as_str(), Some("public" | "personal")) })
    );
    let loaded = first_events
        .iter()
        .find(|event| event["kind"] == "ArtifactLoaded")
        .unwrap_or_else(|| panic!("the pin admission is on the page: {first}"));
    let payload: serde_json::Value = serde_json::from_str(
        loaded["payload"]
            .as_str()
            .unwrap_or_else(|| panic!("payload is JSON text: {loaded}")),
    )
    .unwrap_or_else(|error| panic!("payload decodes: {error}"));
    assert!(
        payload["hash"].is_string(),
        "ArtifactLoaded decodes to its declared fields: {payload}"
    );
    let next = first["next-from"]
        .as_u64()
        .unwrap_or_else(|| panic!("{first}"));
    assert_eq!(
        next,
        first_events
            .last()
            .map_or(0, |event| event["id"].as_u64().unwrap_or(0) + 1)
    );
    let second_events = second["events"]
        .as_array()
        .unwrap_or_else(|| panic!("{second}"));
    assert!(
        second_events.len() > first_events.len(),
        "the second read sees more"
    );
    assert!(
        !second_events
            .iter()
            .any(|event| event["kind"] == "LedgerConsumed"),
        "the reader's own receipt is not in its feed: {second}"
    );
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&last);
    let last = u64::from_le_bytes(bytes);
    let records = events(&daemon).await;
    let receipts: Vec<&LedgerRecord> = records
        .iter()
        .filter(|record| matches!(record.kind, LedgerEventKind::LedgerConsumed { .. }))
        .collect();
    assert_eq!(
        receipts.len(),
        3,
        "one receipt per read, last-seq included: {records:?}"
    );
    assert!(
        receipts
            .iter()
            .all(|receipt| receipt.entry == Some(EntryId("reader".to_owned())))
    );
    if let LedgerEventKind::LedgerConsumed {
        first,
        last: end,
        count,
    } = &receipts[0].kind
    {
        assert_eq!(*first, 1);
        assert_eq!(*count as usize, first_events.len());
        assert_eq!(*end + 1, next);
    }
    assert_eq!(
        receipts[2].kind,
        LedgerEventKind::LedgerConsumed {
            first: last,
            last,
            count: 0
        },
        "last-seq's receipt is the consulted high-water mark, zero events delivered"
    );
    assert!(last >= receipts[0].sequence, "last-seq counts the receipt");
    assert!(last <= records.last().map_or(0, |record| record.sequence));
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// The ledger once `fiber`'s scheduled restart has landed (M2-K8 #26):
/// polls the transition sync until a `ConfigChanged` transition shows.
async fn restarted_records(daemon: &Daemon, fiber: jinnd_api::FiberId) -> Vec<LedgerRecord> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        daemon.sync_transitions();
        let records = events(daemon).await;
        if records.iter().any(|record| matches!(&record.kind, LedgerEventKind::FiberTransition(transition) if transition.fiber == fiber && transition.cause == TransitionCause::ConfigChanged)) {
            return records;
        }
        assert!(Instant::now() < deadline, "the scheduled restart lands");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn worker_config(paths: &DaemonPaths) -> serde_json::Value {
    let document: serde_json::Value =
        json_file(&std::fs::read(&paths.profile).unwrap_or_else(|error| panic!("{error}")));
    document["entries"]
        .as_array()
        .and_then(|entries| entries.iter().find(|entry| entry["id"] == "worker"))
        .map(|entry| entry["config"].clone())
        .unwrap_or_else(|| panic!("worker persisted: {document}"))
}

/// #21: the editor patches the worker — exactly the worker restarts (same
/// fiber, `ConfigChanged`, new incarnation), the document on disk carries
/// the patch, `ProfilePatched` is ledgered under the editor with NO effect
/// registered for it; then the operator removes the editor and the document
/// STAYS patched (the harness transcript's reversal flips red).
#[tokio::test]
async fn a_profile_patch_is_operator_intent_that_survives_the_editors_dispose() {
    let home = home("patch");
    let (paths, hash) = paths(
        &home,
        serde_json::json!([
            entry("worker", "", serde_json::json!(["jinn:fs"]), "plain"),
            entry(
                "editor",
                "",
                serde_json::json!([
                    "jinn:fs",
                    "jinn:clock",
                    { "contract": "jinn:profile", "scope": ["worker"] }
                ]),
                "profile-patch:worker"
            ),
        ]),
    );
    let daemon = booted(paths.clone()).await;
    let worker = daemon
        .entry_fiber("worker")
        .unwrap_or_else(|| panic!("worker live"));
    let editor = daemon
        .entry_fiber("editor")
        .unwrap_or_else(|| panic!("editor live"));
    let answer = wait_for_file(&paths.data.join("patch.out")).await;
    // 0.2.0 (M2-K8 #26): `accepted` + the ProfilePatched sequence; the
    // restart is scheduled, its transitions land after that sequence.
    assert_eq!(answer.first(), Some(&2), "accepted: {answer:?}");
    assert_eq!(answer.len(), 9);
    assert_eq!(
        worker_config(&paths)["data"],
        "noop",
        "the document carries the patch"
    );
    assert_eq!(daemon.entry_fiber("worker"), Some(worker), "same fiber");
    let records = restarted_records(&daemon, worker).await;
    let restarted = |fiber| {
        count(
            &records,
            |kind| matches!(kind, LedgerEventKind::FiberTransition(transition) if transition.fiber == fiber && transition.cause == TransitionCause::ConfigChanged),
        )
    };
    assert!(
        restarted(worker) > 0,
        "the worker restarted on the new config: {records:?}"
    );
    assert_eq!(restarted(editor), 0, "the editor never restarted");
    let patched = records
        .iter()
        .find(|record| matches!(&record.kind, LedgerEventKind::ProfilePatched { entry, by } if entry.0 == "worker" && by == "editor"))
        .unwrap_or_else(|| panic!("ProfilePatched lands: {records:?}"));
    assert_eq!(patched.entry, Some(EntryId("editor".to_owned())));
    assert_eq!(
        count(
            &records,
            |kind| matches!(kind, LedgerEventKind::EffectRegistered { label } if label.contains("profile"))
        ),
        0,
        "no fiber effect for the patch"
    );

    // The operator retires the editor: its dispose withdraws ITS trail —
    // and the document stays exactly as patched.
    let before = std::fs::read(&paths.profile).unwrap_or_else(|error| panic!("{error}"));
    write_profile(
        &home,
        &serde_json::json!([entry(
            "worker",
            &hash,
            serde_json::json!(["jinn:fs"]),
            "noop"
        )]),
    );
    let report = daemon
        .reload()
        .await
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    assert_eq!(report.disposed, vec![EntryId("editor".to_owned())]);
    assert_eq!(
        report.unchanged,
        vec![EntryId("worker".to_owned())],
        "the patch already applied"
    );
    assert_eq!(
        worker_config(&paths)["data"],
        "noop",
        "disposing the editor leaves the document unchanged"
    );
    assert!(daemon.entry_fiber("editor").is_none());
    let after = std::fs::read(&paths.profile).unwrap_or_else(|error| panic!("{error}"));
    assert_ne!(before, after, "the editor left the document");
    let records = events(&daemon).await;
    assert_eq!(
        count(
            &records,
            |kind| matches!(kind, LedgerEventKind::EffectWithdrawn { label, .. } if label.contains("profile"))
        ),
        0,
        "nothing of the document was withdrawn: {records:?}"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// Hostile probes: a patch outside the grant scope is a ledgered
/// `GrantRefused` with its reason and the guest sees the typed refusal; a
/// patch failing schema validation refuses with NOTHING written — the
/// document bytes are identical before and after.
#[tokio::test]
async fn patches_outside_the_scope_or_failing_validation_refuse_without_writing() {
    let home = home("refusals");
    let (paths, _) = paths(
        &home,
        serde_json::json!([
            entry("worker", "", serde_json::json!(["jinn:fs"]), "plain"),
            entry(
                "narrow",
                "",
                serde_json::json!([{ "contract": "jinn:profile", "scope": ["other"] }]),
                "profile-patch-denied:worker"
            ),
            entry(
                "clumsy",
                "",
                serde_json::json!([
                    "jinn:fs",
                    "jinn:clock",
                    { "contract": "jinn:profile", "scope": ["*"] }
                ]),
                "profile-patch-bad:worker"
            ),
        ]),
    );
    // The boot write-back re-renders the document (LAW §3); the CONTENT
    // is what a refused patch must leave untouched.
    let before =
        json_file(&std::fs::read(&paths.profile).unwrap_or_else(|error| panic!("{error}")));
    let daemon = booted(paths.clone()).await;
    let answer = wait_for_file(&paths.data.join("patch.out")).await;
    assert_eq!(answer.first(), Some(&1), "refused");
    let reason = String::from_utf8_lossy(&answer[1..]);
    assert!(
        reason.contains("refused"),
        "validation names the refused grant: {reason}"
    );
    assert_eq!(
        json_file(&std::fs::read(&paths.profile).unwrap_or_else(|error| panic!("{error}"))),
        before,
        "nothing written"
    );
    assert_eq!(worker_config(&paths)["data"], "plain");
    let records = events(&daemon).await;
    let refused = records
        .iter()
        .find(|record| matches!(&record.kind, LedgerEventKind::GrantRefused { contract, reason: RefusalReason::ScopeMismatch, detail: Some(detail) } if contract == "jinn:profile" && detail.contains("worker")))
        .unwrap_or_else(|| panic!("the scope refusal is on the record, typed, with its detail: {records:?}"));
    assert_eq!(refused.entry, Some(EntryId("narrow".to_owned())));
    assert!(
        records.iter().any(|record| matches!(&record.kind, LedgerEventKind::FiberTransition(transition) if transition.to == FiberState::Active) && record.entry == Some(EntryId("narrow".to_owned()))),
        "the editor saw `refused` on the wire, not a fault: {records:?}"
    );
    assert!(
        records.iter().any(|record| matches!(&record.kind, LedgerEventKind::AmendmentRefused { detail } if detail.contains("worker")) && record.entry == Some(EntryId("clumsy".to_owned()))),
        "the validation refusal is on the record: {records:?}"
    );
    assert_eq!(
        count(&records, |kind| matches!(
            kind,
            LedgerEventKind::ProfilePatched { .. }
        )),
        0
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .unwrap_or_else(|error| panic!("free port: {error}"))
}

fn connect(port: u16) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
            return stream;
        }
        assert!(Instant::now() < deadline, "the listener is up");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// #23: a listener with NO clock grant serves a real TCP peer from the
/// readiness wake — connect and data each wake once, `NetReadable` is
/// ledgered under the entry, zero `AlarmWake`s exist, and a flood of 200
/// writes lands a bounded number of wakes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_listener_serves_from_readiness_wakes_with_zero_alarms() {
    let home = home("wake");
    let port = free_port();
    let (paths, _) = paths(
        &home,
        serde_json::json!([entry(
            "server",
            "",
            serde_json::json!([{ "contract": "jinn:net", "scope": { "bind": [port, port] } }]),
            &format!("net-wake:{port}")
        )]),
    );
    let daemon = booted(paths.clone()).await;
    let mut client = connect(port);
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap_or_else(|error| panic!("{error}"));
    client
        .write_all(b"ping")
        .unwrap_or_else(|error| panic!("{error}"));
    let mut echoed = [0u8; 4];
    client
        .read_exact(&mut echoed)
        .unwrap_or_else(|error| panic!("echo: {error}"));
    assert_eq!(&echoed, b"ping");
    for _ in 0..200 {
        client
            .write_all(b"x")
            .unwrap_or_else(|error| panic!("{error}"));
    }
    let mut flood = vec![0u8; 200];
    client
        .read_exact(&mut flood)
        .unwrap_or_else(|error| panic!("flood echo: {error}"));
    assert!(flood.iter().all(|byte| *byte == b'x'));
    let records = events(&daemon).await;
    assert_eq!(
        count(&records, |kind| matches!(
            kind,
            LedgerEventKind::AlarmWake { .. }
        )),
        0,
        "no alarm"
    );
    let wakes = count(&records, |kind| {
        matches!(kind, LedgerEventKind::NetReadable { .. })
    });
    let reads = count(
        &records,
        |kind| matches!(kind, LedgerEventKind::ContractCall { contract, operation } if contract == "jinn:net" && (operation == "read" || operation == "accept")),
    );
    assert!(wakes >= 2, "connect and data each woke: {records:?}");
    assert!(
        wakes <= reads + 2,
        "wakes ({wakes}) bounded by guest reads ({reads}) under the flood"
    );
    assert!(
        records
            .iter()
            .filter(|record| matches!(record.kind, LedgerEventKind::NetReadable { .. }))
            .all(|record| record.entry == Some(EntryId("server".to_owned()))),
        "wakes are attributed to the entry"
    );
    drop(client);
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
    let records = events(&daemon).await;
    assert!(
        records
            .iter()
            .any(|record| matches!(record.kind, LedgerEventKind::NetClosed { .. })),
        "suspend released the listener"
    );
}
