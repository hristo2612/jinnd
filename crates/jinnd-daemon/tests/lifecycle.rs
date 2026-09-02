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

use jinnd_api::{FiberState, LedgerEventKind, LedgerRecord};
use jinnd_daemon::{Daemon, DaemonPaths};

/// The reserved topic, written out rather than imported: the test states
/// the wire name a plugin author reads in the contract, and checks the
/// bundle still says it.
const TOPIC: &str = "jinn:introspect/transitions";

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

/// The wire spelling of a kernel state, exactly as the delivered record
/// spells it — one vocabulary, so a ledger row and a delivery are compared
/// on the same words.
fn state(state: FiberState) -> String {
    format!("{state:?}").to_lowercase()
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
        jinnd_contract_lens::bundle("jinn-introspect")
            .wit()
            .wit()
            .interface("composition")
            .type_docs("transition")
            .states(TOPIC),
        "the transition record declares the wire name this test subscribes on"
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
    // ORDERING, against each delivery's OWN row — never against merely
    // some earlier one, which boot already supplies and which would make
    // this pass for a weaker reason than its name promises.
    //
    // The binding is exact and checkable: the kernel offers one transition
    // per row it commits, in commit order, so the delivery carrying
    // `ordinal` N is the Nth `FiberTransition` on the stream. The test
    // asserts that identity (fiber, from, to, entry) before it uses it,
    // so an index that drifted would be caught rather than believed.
    let ledger = records(&daemon).await;
    let transitions: Vec<&LedgerRecord> = ledger
        .iter()
        .filter(|record| matches!(record.kind, LedgerEventKind::FiberTransition(_)))
        .collect();
    assert!(
        !landed.is_empty() && !transitions.is_empty(),
        "PRECONDITION: there is a delivery and a row to bind it to"
    );
    for line in &landed {
        let ordinal = line.event["ordinal"]
            .as_u64()
            .unwrap_or_else(|| panic!("ordinal: {}", line.event));
        let index = usize::try_from(ordinal - 1).unwrap_or_else(|error| panic!("{error}"));
        let own = transitions.get(index).unwrap_or_else(|| {
            panic!(
                "PRECONDITION: ordinal {ordinal} has no row to bind to — only \
                 {} transitions are on the stream, so this test cannot run",
                transitions.len()
            )
        });
        let LedgerEventKind::FiberTransition(committed) = &own.kind else {
            unreachable!("filtered above")
        };
        // The row at that index IS the delivered transition, not merely a
        // row that happens to sit there.
        assert_eq!(
            (
                committed.fiber.0,
                state(committed.from),
                state(committed.to),
                own.entry.as_ref().map(|entry| entry.0.clone())
            ),
            (
                line.event["fiber"].as_u64().unwrap_or_default(),
                line.event["from"].as_str().unwrap_or_default().to_owned(),
                line.event["to"].as_str().unwrap_or_default().to_owned(),
                line.event["entry"].as_str().map(str::to_owned)
            ),
            "ordinal {ordinal} names the {index}th committed transition"
        );
        // (a) the kernel published a mark at or past its own row, and
        // (b) the ledger read from INSIDE the delivery had already passed
        // that row: the delivery could not precede its own commit.
        let committed_by = line.event["committed-by"]
            .as_u64()
            .unwrap_or_else(|| panic!("committed-by: {}", line.event));
        assert!(
            own.sequence <= committed_by,
            "the delivery's OWN row (sequence {}) was committed at or before the \
             published mark {committed_by}",
            own.sequence
        );
        assert!(
            line.high_water >= own.sequence,
            "the delivery's own row (sequence {}) was already on the ledger when \
             the delivery landed (the guest read {})",
            own.sequence,
            line.high_water
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
    // PRECONDITION, stated rather than assumed: six reconciles offer far
    // fewer transitions than the publisher's bound, so this fixture is the
    // LOSSLESS half of back-pressure and asserts exactly that — nothing
    // dropped, no gap. Round 1 asserted `gaps <= dropped` here and passed
    // as `0 <= 0`, which is true of a kernel that publishes nothing at all.
    // The OVERFLOW half cannot be established through six reloads, so it is
    // proven where it can be, deterministically:
    // `jinnd_daemon::daemon::lifecycle::tests::
    // a_real_overflow_is_ledgered_and_shows_as_a_gap_in_the_ordinals`.
    let dropped: u64 = records(&daemon)
        .await
        .iter()
        .filter_map(|record| match &record.kind {
            LedgerEventKind::PublishDropped { dropped, .. } => Some(*dropped),
            _ => None,
        })
        .sum();
    assert_eq!(
        dropped, 0,
        "PRECONDITION: this fixture stays under the bound, so it is the lossless case"
    );
    let span = ordinals
        .last()
        .zip(ordinals.first())
        .map_or(0, |(last, first)| last - first + 1);
    assert_eq!(
        span,
        ordinals.len() as u64,
        "under the bound a slow listener loses NOTHING: its ordinals are \
         contiguous — {ordinals:?}"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// LATE JOIN: there is no replay, and the kernel says so in a number. A
/// listener that mounts after the kernel has already published receives
/// nothing that happened before it — and its FIRST `ordinal` names exactly
/// how many transitions it missed, so "it just starts receiving" is never
/// what a consumer has to guess at (packet card, design question 3).
#[tokio::test]
async fn a_late_joining_listener_gets_no_replay_and_its_first_ordinal_names_the_miss() {
    let home = home("late");
    let worker = |mode: &str| entry("worker", serde_json::json!(["jinn:fs"]), mode);
    let (paths, hash) = paths(&home, &[worker("plain")]);
    let daemon = booted(paths.clone()).await;
    // Life happens with NOBODY listening: two restarts of the worker, each
    // committing — and publishing to zero listeners — its transitions.
    for mode in ["noop", "plain"] {
        write_profile(&home, &[worker(mode)], &hash);
        daemon
            .reload()
            .await
            .unwrap_or_else(|error| panic!("reload: {error:?}"));
    }
    // PRECONDITION, asserted: the kernel really did publish before anyone
    // could listen. Without it, "the first ordinal is above 1" would be a
    // claim about nothing at all.
    let missed = records(&daemon)
        .await
        .iter()
        .filter(|record| matches!(record.kind, LedgerEventKind::FiberTransition(_)))
        .count() as u64;
    assert!(
        missed >= 2,
        "PRECONDITION: transitions were committed and published while no \
         listener existed — only {missed} did, so this is not a late join"
    );
    assert!(
        deliveries(&paths.data).is_empty(),
        "PRECONDITION: nothing has been delivered to anyone yet"
    );
    // NOW the listener mounts, and the worker restarts once more under it.
    let composed = [
        worker("plain"),
        entry("watcher", listener_grants(), "lifecycle-listener"),
    ];
    write_profile(&home, &composed, &hash);
    daemon
        .reload()
        .await
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    let restarted = [
        worker("noop"),
        entry("watcher", listener_grants(), "lifecycle-listener"),
    ];
    write_profile(&home, &restarted, &hash);
    daemon
        .reload()
        .await
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    let landed = until(&paths.data, 1).await;
    assert!(
        !landed.is_empty(),
        "a late listener receives from the moment it mounted"
    );
    let ordinals: Vec<u64> = landed
        .iter()
        .map(|line| line.event["ordinal"].as_u64().unwrap_or_default())
        .collect();
    let first = ordinals[0];
    // NO REPLAY: not one of the transitions published before the mount is
    // handed over afterwards.
    assert!(
        ordinals.iter().all(|ordinal| *ordinal > missed),
        "nothing published before the listener existed is replayed to it: \
         {ordinals:?} against {missed} earlier transitions"
    );
    // AND THE MISS IS NAMED: the first ordinal is the kernel's own count,
    // so `first - 1` is exactly how many the listener was not there for.
    assert!(
        first > missed,
        "the first delivered ordinal ({first}) is above the {missed} \
         transitions published before the mount"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}
