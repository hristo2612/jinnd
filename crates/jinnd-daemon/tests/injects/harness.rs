//! The daemon home the M2-K24 cases drive: a profile of fixture entries
//! (grants, `injects`, mode), a booted daemon over it, and the reads the
//! cases make of the ledger and the fibers. Split from the cases
//! themselves (R10 file hygiene).

#[path = "../support/mod.rs"]
mod support;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use jinnd_api::{FiberState, LedgerEventKind, LedgerRecord, Transition};
use jinnd_daemon::{Daemon, DaemonPaths};

/// The sibling contract the fixture's `provider` mode provides and the
/// `inject-counter` modes inject at activation.
pub(crate) const COUNTER: &str = "jinn:test/counter";

pub(crate) struct Home(pub(crate) PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(crate) fn home(name: &str) -> Home {
    let root = std::env::temp_dir().join(format!("jinnd-injects-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
    Home(root)
}

/// One fixture entry: `grants` and `injects` beside the mode, exactly the
/// seat config the card's delta describes.
pub(crate) fn entry(
    id: &str,
    grants: serde_json::Value,
    injects: serde_json::Value,
    mode: &str,
) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "package": "demo/counter-plugin",
        "version": "0.0.1",
        "hash": "",
        "config": { "grants": grants, "injects": injects, "data": mode },
    })
}

/// The `provider` mode providing the counter contract.
pub(crate) fn provider(id: &str) -> serde_json::Value {
    entry(
        id,
        serde_json::json!([COUNTER]),
        serde_json::json!([]),
        "provider",
    )
}

/// A consumer that DECLARES the counter contract.
pub(crate) fn declared(id: &str, mode: &str) -> serde_json::Value {
    entry(
        id,
        serde_json::json!([COUNTER]),
        serde_json::json!([COUNTER]),
        mode,
    )
}

/// A consumer with the grant and no declaration — today's shape.
pub(crate) fn undeclared(id: &str, mode: &str) -> serde_json::Value {
    entry(
        id,
        serde_json::json!([COUNTER]),
        serde_json::json!([]),
        mode,
    )
}

/// An unrelated sibling with no grants and no declaration.
pub(crate) fn bystander(id: &str, mode: &str) -> serde_json::Value {
    entry(id, serde_json::json!([]), serde_json::json!([]), mode)
}

pub(crate) fn write_profile(home: &Home, entries: &[serde_json::Value], hash: &str) {
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

pub(crate) fn paths(home: &Home, entries: &[serde_json::Value]) -> (DaemonPaths, String) {
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

pub(crate) async fn booted(paths: DaemonPaths) -> Daemon {
    let daemon = Daemon::open(paths).unwrap_or_else(|error| panic!("open: {error:?}"));
    let report = daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot: {error:?}"));
    assert!(report.errors.is_empty(), "clean boot: {:?}", report.errors);
    daemon
}

pub(crate) async fn reload(
    daemon: &Daemon,
    home: &Home,
    entries: &[serde_json::Value],
    hash: &str,
) {
    write_profile(home, entries, hash);
    let report = daemon
        .reload()
        .await
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    assert!(
        report.errors.is_empty(),
        "clean reload: {:?}",
        report.errors
    );
}

/// The ledger after the daemon has committed every landed transition.
pub(crate) async fn events(daemon: &Daemon) -> Vec<LedgerRecord> {
    daemon.sync_transitions();
    daemon
        .ledger_events()
        .await
        .unwrap_or_else(|error| panic!("ledger read: {error:?}"))
}

pub(crate) fn state(daemon: &Daemon, entry: &str) -> Option<FiberState> {
    daemon
        .entry_fiber(entry)
        .and_then(|fiber| daemon.fiber_state(fiber))
}

/// Waits until `entry` rests in `want`, syncing transitions as it goes.
/// The deadline is well beyond the guest deadline: a run that needs it has
/// already failed the packet's claim.
pub(crate) async fn until_state(daemon: &Daemon, entry: &str, want: FiberState) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        daemon.sync_transitions();
        if state(daemon, entry) == Some(want) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{entry} reaches {want:?} (rests {:?})",
            state(daemon, entry)
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Waits until `entry` has been loaded `want` times.
pub(crate) async fn until_loaded(daemon: &Daemon, entry: &str, want: usize) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let loaded = loads(&events(daemon).await, entry);
        if loaded >= want {
            assert_eq!(loaded, want, "{entry} loaded exactly {want} times");
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{entry} loads {want} times (loaded {loaded})"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// A quiet interval for the NEGATIVE claims — that nothing moved.
pub(crate) async fn settle(daemon: &Daemon) {
    tokio::time::sleep(Duration::from_millis(600)).await;
    daemon.sync_transitions();
}

/// `entry`'s committed transitions, in ledger order.
pub(crate) fn transitions<'a>(records: &'a [LedgerRecord], entry: &str) -> Vec<&'a Transition> {
    records
        .iter()
        .filter(|record| record.entry.as_ref().is_some_and(|id| id.0 == entry))
        .filter_map(|record| match &record.kind {
            LedgerEventKind::FiberTransition(transition) => Some(transition),
            _ => None,
        })
        .collect()
}

/// How many times `entry` entered `Loading` — one per activation.
pub(crate) fn loads(records: &[LedgerRecord], entry: &str) -> usize {
    transitions(records, entry)
        .iter()
        .filter(|transition| transition.to == FiberState::Loading)
        .count()
}

/// Whether `entry` ever rested `Failed`.
pub(crate) fn failed(records: &[LedgerRecord], entry: &str) -> bool {
    transitions(records, entry)
        .iter()
        .any(|transition| transition.to == FiberState::Failed)
}

/// The ledger sequence of `entry`'s `n`th arrival in `state` (from 1).
pub(crate) fn arrival(records: &[LedgerRecord], entry: &str, state: FiberState, n: usize) -> u64 {
    records
        .iter()
        .filter(|record| record.entry.as_ref().is_some_and(|id| id.0 == entry))
        .filter(|record| {
            matches!(&record.kind, LedgerEventKind::FiberTransition(transition) if transition.to == state)
        })
        .nth(n - 1)
        .map(|record| record.sequence)
        .unwrap_or_else(|| panic!("{entry} arrived in {state:?} {n} time(s)"))
}

/// `entry`'s contract-call crossings of `operation` on the counter.
pub(crate) fn calls(records: &[LedgerRecord], entry: &str, operation: &str) -> usize {
    records
        .iter()
        .filter(|record| record.entry.as_ref().is_some_and(|id| id.0 == entry))
        .filter(|record| {
            matches!(&record.kind, LedgerEventKind::ContractCall { contract, operation: op } if contract == COUNTER && op == operation)
        })
        .count()
}

/// `entry`'s recorded errors, as their messages.
pub(crate) fn errors(records: &[LedgerRecord], entry: &str) -> Vec<String> {
    records
        .iter()
        .filter(|record| record.entry.as_ref().is_some_and(|id| id.0 == entry))
        .filter_map(|record| match &record.kind {
            LedgerEventKind::ErrorRecorded { error } => Some(error.message.clone()),
            _ => None,
        })
        .collect()
}

/// Waits for a file a guest writes once, then reads it as JSON.
pub(crate) async fn wait_json(path: &std::path::Path) -> serde_json::Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(bytes) = std::fs::read(path)
            && !bytes.is_empty()
        {
            return serde_json::from_slice(&bytes).unwrap_or_else(|error| {
                panic!("json: {error} in {:?}", String::from_utf8_lossy(&bytes))
            });
        }
        assert!(Instant::now() < deadline, "{} lands", path.display());
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
