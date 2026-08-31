//! M2-K13 acceptance, through the real daemon assembly (harness findings
//! #40 and #41): the kernel PUBLISHES every fiber transition it commits on
//! a reserved topic, so a plugin emits what it WITNESSED instead of what it
//! inferred from two snapshots.
//!
//! What the round proves here: a granted listener receives the transient
//! states no poller at this pin can reach; no delivery ever precedes its
//! ledger row; an ungranted entry cannot subscribe and a granted one still
//! cannot forge a publish; a slow listener never stalls the kernel, never
//! reorders a transition, and its losses are COUNTED — in the ledger and as
//! a gap in the listener's own ordinals (Law 1, Law 2, R1, R9, R11).

mod support;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use jinnd_api::{LedgerEventKind, LedgerRecord};
use jinnd_daemon::{Daemon, DaemonPaths};

/// The reserved topic, written out rather than imported: the test states
/// the wire name a plugin author reads in the contract, and checks the
/// bundle still says it.
const TOPIC: &str = "jinn:introspect/transitions";
const BUNDLE: &str = include_str!("../../../contracts/jinn-introspect/contract.wit");

struct Home(PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn home(name: &str) -> Home {
    let root = std::env::temp_dir().join(format!("jinnd-lifecycle-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
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

fn write_profile(home: &Home, entries: &[serde_json::Value], hash: &str) {
    let entries: Vec<serde_json::Value> = entries
        .iter()
        .map(|entry| {
            let mut entry = entry.clone();
            entry["hash"] = serde_json::Value::String(hash.to_owned());
            entry
        })
        .collect();
    let profile = serde_json::json!({ "entries": entries });
    std::fs::write(
        home.0.join("profile.json"),
        serde_json::to_string_pretty(&profile).unwrap_or_else(|error| panic!("{error}")),
    )
    .unwrap_or_else(|error| panic!("{error}"));
}

fn paths(home: &Home, entries: &[serde_json::Value]) -> (DaemonPaths, String) {
    let (bytes, hash) = support::pinned_fixture();
    std::fs::write(home.0.join("artifacts/counter-plugin.wasm"), &bytes)
        .unwrap_or_else(|error| panic!("{error}"));
    write_profile(home, entries, &hash);
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

/// The listener grant set: the composition read the delivery is bounded by,
/// plus the ledger read the ordering probe makes and the fs it writes with.
fn listener_grants() -> serde_json::Value {
    serde_json::json!(["jinn:fs", "jinn:introspect", "jinn:ledger"])
}

/// One delivered line: the event JSON and the ledger high-water mark the
/// guest read from INSIDE that delivery.
struct Delivered {
    event: serde_json::Value,
    high_water: u64,
}

fn deliveries(data: &std::path::Path) -> Vec<Delivered> {
    let bytes = std::fs::read(data.join("transitions.log")).unwrap_or_default();
    String::from_utf8_lossy(&bytes)
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| {
            let (event, high) = line
                .split_once('\t')
                .unwrap_or_else(|| panic!("a delivered line carries its probe: {line}"));
            Delivered {
                event: serde_json::from_str(event)
                    .unwrap_or_else(|error| panic!("delivery {event}: {error}")),
                high_water: high.parse().unwrap_or_else(|error| panic!("{error}")),
            }
        })
        .collect()
}

/// Reloads until the listener has written at least `want` deliveries, or
/// the deadline passes; answers what landed.
async fn until(data: &std::path::Path, want: usize) -> Vec<Delivered> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let landed = deliveries(data);
        if landed.len() >= want || Instant::now() >= deadline {
            return landed;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn records(daemon: &Daemon) -> Vec<LedgerRecord> {
    daemon
        .ledger_events()
        .await
        .unwrap_or_else(|error| panic!("ledger read: {error:?}"))
}

/// #40 + #41: the kernel publishes what it commits, the listener sees the
/// TRANSIENT states a poller at this pin cannot reach, and no delivery ever
/// precedes its own ledger row.
#[tokio::test]
async fn a_granted_listener_witnesses_the_transitions_the_kernel_commits() {
    let home = home("witness");
    let entries = [
        entry("watcher", listener_grants(), "lifecycle-listener"),
        entry("worker", serde_json::json!(["jinn:fs"]), "plain"),
    ];
    assert!(
        BUNDLE.contains(TOPIC),
        "the bundle declares the wire name this test subscribes on"
    );
    let (paths, hash) = paths(&home, &entries);
    let daemon = booted(paths.clone()).await;
    // One restart of the worker: the kernel commits
    // active → unloading → pending → loading → active.
    let restarted = [
        entry("watcher", listener_grants(), "lifecycle-listener"),
        entry("worker", serde_json::json!(["jinn:fs"]), "noop"),
    ];
    write_profile(&home, &restarted, &hash);
    daemon
        .reload()
        .await
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    let landed = until(&paths.data, 5).await;
    assert!(
        landed.len() >= 5,
        "the restart's transitions were published: {}",
        landed.len()
    );
    let seen: Vec<String> = landed
        .iter()
        .filter(|line| line.event["entry"] == "worker")
        .map(|line| format!("{}->{}", line.event["from"], line.event["to"]))
        .collect();
    for transient in ["unloading", "pending", "loading"] {
        assert!(
            seen.iter().any(|step| step.contains(transient)),
            "the listener reached the transient reading {transient}: {seen:?}"
        );
    }
    // The delivery's SHAPE is exactly what a `jinn:introspect` pull already
    // admits: entry, fiber, incarnation and the `state` vocabulary. No
    // `cause` — the authority demonstration failed for that one field, so
    // it is not delivered here (packet report, M2-K13).
    let mut keys: Vec<&str> = landed[0]
        .event
        .as_object()
        .unwrap_or_else(|| panic!("an object: {}", landed[0].event))
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "committed-by",
            "entry",
            "fiber",
            "from",
            "incarnation",
            "ordinal",
            "to"
        ],
        "the delivered fields are the introspect-admitted ones"
    );
    // Ordering, both halves. (a) the row is committed at or before the
    // `committed-by` the kernel published; (b) the ledger's high-water mark
    // read from INSIDE the delivery had already reached it.
    let ledger = records(&daemon).await;
    let transitions: Vec<u64> = ledger
        .iter()
        .filter(|record| matches!(record.kind, LedgerEventKind::FiberTransition(_)))
        .map(|record| record.sequence)
        .collect();
    for line in &landed {
        let committed_by = line.event["committed-by"]
            .as_u64()
            .unwrap_or_else(|| panic!("committed-by: {}", line.event));
        assert!(
            line.high_water >= committed_by,
            "the ledger had already committed through {committed_by} when the \
             delivery landed (it was at {})",
            line.high_water
        );
        assert!(
            transitions.iter().any(|sequence| *sequence <= committed_by),
            "a transition row sits at or before {committed_by}"
        );
    }
    // Ordinals are the kernel's own publish count: strictly increasing,
    // never reordered.
    let ordinals: Vec<u64> = landed
        .iter()
        .map(|line| line.event["ordinal"].as_u64().unwrap_or_default())
        .collect();
    assert!(
        ordinals.windows(2).all(|pair| pair[0] < pair[1]),
        "ordinals never repeat or go backwards: {ordinals:?}"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// The reserved topic is FAIL-CLOSED both ways: an entry without
/// `jinn:introspect` cannot subscribe, and one WITH it still cannot emit —
/// only the kernel publishes there, so a witnessed transition can never be
/// a forged one.
#[tokio::test]
async fn the_reserved_topic_refuses_an_ungranted_listener_and_every_guest_emit() {
    let home = home("reserved");
    let entries = [
        entry(
            "eavesdropper",
            serde_json::json!(["jinn:fs"]),
            "lifecycle-eavesdrop",
        ),
        entry("forger", listener_grants(), "lifecycle-forge"),
    ];
    let (paths, _) = paths(&home, &entries);
    let daemon = booted(paths.clone()).await;
    // Each fixture WRITES DOWN the refusal it got: a test that only
    // checked the activations did not fault would pass with no gate at all.
    let ungranted = std::fs::read(paths.data.join("eavesdrop.out"))
        .unwrap_or_else(|error| panic!("the ungranted subscribe was refused: {error}"));
    assert!(
        !ungranted.is_empty(),
        "an entry without jinn:introspect cannot subscribe"
    );
    let refusal = std::fs::read(paths.data.join("forge.out"))
        .unwrap_or_else(|error| panic!("the forge refusal landed: {error}"));
    let refusal = String::from_utf8_lossy(&refusal);
    assert!(
        !refusal.is_empty(),
        "the guest emit was refused typed: {refusal}"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// Back-pressure: a listener that dawdles inside every delivery cannot hold
/// up the kernel's own reconciles, cannot reorder a transition, and cannot
/// lose one silently — every loss is a `PublishDropped` row and the gap it
/// leaves in the listener's ordinals.
#[tokio::test]
async fn a_slow_listener_never_stalls_reorders_or_silently_drops() {
    let home = home("slow");
    let mut entries = vec![
        entry("watcher", listener_grants(), "lifecycle-slow"),
        entry("worker", serde_json::json!(["jinn:fs"]), "plain"),
    ];
    let (paths, hash) = paths(&home, &entries);
    let daemon = booted(paths.clone()).await;
    let started = Instant::now();
    for round in 0..6u32 {
        entries[1] = entry(
            "worker",
            serde_json::json!(["jinn:fs"]),
            if round % 2 == 0 { "noop" } else { "plain" },
        );
        write_profile(&home, &entries, &hash);
        daemon
            .reload()
            .await
            .unwrap_or_else(|error| panic!("reload: {error:?}"));
    }
    // Six reconciles against a listener that dawdles 400 ms per delivery:
    // the kernel never waited on it.
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "the reconciles did not wait on the slow listener: {:?}",
        started.elapsed()
    );
    let landed = until(&paths.data, 3).await;
    assert!(
        landed.len() >= 3,
        "the slow listener still RECEIVES — back-pressure is not silence: {}",
        landed.len()
    );
    let ordinals: Vec<u64> = landed
        .iter()
        .map(|line| line.event["ordinal"].as_u64().unwrap_or_default())
        .collect();
    assert!(
        ordinals.windows(2).all(|pair| pair[0] < pair[1]),
        "back-pressure never reorders: {ordinals:?}"
    );
    let dropped: u64 = records(&daemon)
        .await
        .iter()
        .filter_map(|record| match &record.kind {
            LedgerEventKind::PublishDropped { dropped, .. } => Some(*dropped),
            _ => None,
        })
        .sum();
    let gaps = ordinals
        .last()
        .zip(ordinals.first())
        .map_or(0, |(last, first)| {
            (last - first + 1) - ordinals.len() as u64
        });
    assert!(
        gaps <= dropped,
        "every gap in the listener's ordinals is a COUNTED drop: {gaps} gaps, \
         {dropped} counted"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}
