//! M2-K4 acceptance, through the real daemon assembly: effects are
//! ENTRY-scoped — suspend ≠ dispose (decision log 2026-08-28; harness
//! FINDINGS #14/#15). A clean shutdown SUSPENDS every fiber: guest fs state
//! is preserved byte-for-byte, quiescence and the ledger flush are reached,
//! and a typed suspension event lands per fiber; SIGINT and SIGKILL agree on
//! the disk outcome. An incarnation replacement (reconcile restart) hands
//! the successor the entry's live journal: persisted documents survive, an
//! inherited effect keyed-reverts, and a true dispose withdraws the WHOLE
//! trail LIFO across incarnations — in one process and across a process
//! restart. And a dispose during a mid-tick handler seals the journal: a
//! late registration refuses on the record, the trail is never torn (#15).

mod support;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use jinnd_api::{
    FiberId, FiberState, LedgerEventKind, LedgerRecord, RevertResolution, TransitionCause,
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
        std::env::temp_dir().join(format!("jinnd-suspend-test-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
    Home(root)
}

fn write_profile(home: &Home, entries: serde_json::Value) {
    let profile = serde_json::json!({ "entries": entries });
    std::fs::write(
        home.0.join("profile.json"),
        serde_json::to_string_pretty(&profile).unwrap_or_else(|error| panic!("{error}")),
    )
    .unwrap_or_else(|error| panic!("{error}"));
}

fn scribe(hash: &str, grants: serde_json::Value, config: serde_json::Value) -> serde_json::Value {
    let mut config = config;
    config["grants"] = grants;
    serde_json::json!({
        "id": "scribe",
        "package": "demo/counter-plugin",
        "version": "0.0.1",
        "hash": hash,
        "config": config,
    })
}

/// A home with the fixture artifact and one `scribe` entry in `mode`.
fn paths(home: &Home, grants: serde_json::Value, mode: &str) -> (DaemonPaths, String) {
    let (bytes, hash) = support::pinned_fixture();
    std::fs::write(home.0.join("artifacts/counter-plugin.wasm"), &bytes)
        .unwrap_or_else(|error| panic!("{error}"));
    write_profile(
        home,
        serde_json::json!([scribe(&hash, grants, serde_json::json!({ "data": mode }))]),
    );
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

fn inverse_files(paths: &DaemonPaths) -> usize {
    std::fs::read_dir(paths.inverses())
        .map(|entries| {
            entries
                .flatten()
                .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "inverse"))
                .count()
        })
        .unwrap_or(0)
}

/// Every file under `dir`, relative path → bytes (the disk outcome).
fn snapshot(dir: &std::path::Path) -> Vec<(String, Vec<u8>)> {
    let mut files = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        for entry in std::fs::read_dir(&next).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                let relative = path
                    .strip_prefix(dir)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .into_owned();
                files.push((relative, std::fs::read(&path).unwrap_or_default()));
            }
        }
    }
    files.sort();
    files
}

/// The fs withdrawals ledgered for `fiber`, in order, labels stripped of
/// their effect ids.
fn fs_withdrawals(records: &[LedgerRecord], fiber: FiberId) -> Vec<String> {
    records
        .iter()
        .filter_map(|record| match &record.kind {
            LedgerEventKind::EffectWithdrawn { label, clean: true }
                if label.starts_with("fs ") && record.fiber == Some(fiber) =>
            {
                Some(label.split(" [effect").next().unwrap_or(label).to_owned())
            }
            _ => None,
        })
        .collect()
}

const BUNDLE_STATE: &[(&str, &[u8])] = &[("log/a.txt", b"one\ntwo\n")];

fn assert_bundle_state(data: &std::path::Path) {
    let expected: Vec<(String, Vec<u8>)> = BUNDLE_STATE
        .iter()
        .map(|(path, bytes)| ((*path).to_owned(), bytes.to_vec()))
        .collect();
    assert_eq!(
        snapshot(data),
        expected,
        "the guest's fs state, byte for byte"
    );
}

/// Ruling 2: shutdown = suspend. The fs-bundle entry's state survives a
/// clean shutdown byte-for-byte, its inverses stay retained for the entry,
/// the fiber lands `Disposed` under the `Suspend` cause, and the ledger
/// carries the typed suspension event with the retained count.
#[tokio::test]
async fn a_clean_shutdown_suspends_preserving_guest_fs_state_on_the_record() {
    let home = home("shutdown");
    let (paths, _) = paths(&home, serde_json::json!(["jinn:fs"]), "fs-bundle");
    let daemon = booted(paths.clone()).await;
    let fiber = daemon
        .entry_fiber("scribe")
        .unwrap_or_else(|| panic!("the entry has a fiber"));
    assert_bundle_state(&paths.data);
    assert_eq!(inverse_files(&paths), 4);

    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));

    assert_bundle_state(&paths.data);
    assert_eq!(inverse_files(&paths), 4, "suspension withdraws nothing");
    assert_eq!(daemon.fs_effects().len(), 4);
    assert_eq!(daemon.fiber_state(fiber), Some(FiberState::Disposed));
    let records = events(&daemon).await;
    assert!(
        records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::FiberSuspended { retained: 4 }
        ) && record.fiber == Some(fiber)),
        "the typed suspension event, attributed: {records:?}"
    );
    assert!(
        records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::FiberTransition(transition)
                if transition.fiber == fiber
                    && transition.to == FiberState::Disposed
                    && transition.cause == TransitionCause::Suspend
        )),
        "the terminal transition is a suspension: {records:?}"
    );
    assert!(
        fs_withdrawals(&records, fiber).is_empty(),
        "no fs withdrawal on shutdown"
    );
}

/// Sends `signal` to the child (unix), by name.
fn signal(child: &std::process::Child, name: &str) {
    let status = std::process::Command::new("kill")
        .args([format!("-{name}"), child.id().to_string()])
        .status()
        .unwrap_or_else(|error| panic!("kill: {error}"));
    assert!(status.success(), "kill -{name} delivered");
}

/// Spawns the real `jinnd` binary over `home` and waits for the fs-bundle
/// entry's state to land on disk.
fn spawn_daemon(paths: &DaemonPaths) -> std::process::Child {
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_jinnd"))
        .args(["--profile"])
        .arg(&paths.profile)
        .arg("--ledger")
        .arg(&paths.ledger)
        .arg("--artifacts")
        .arg(&paths.artifacts)
        .arg("--data")
        .arg(&paths.data)
        .stdin(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap_or_else(|error| panic!("spawn jinnd: {error}"));
    let deadline = Instant::now() + Duration::from_secs(30);
    while std::fs::read(paths.data.join("log/a.txt")).ok().as_deref() != Some(b"one\ntwo\n")
        || paths.data.join("log/b.txt").exists()
    {
        assert!(Instant::now() < deadline, "the entry activates in time");
        std::thread::sleep(Duration::from_millis(50));
    }
    child
}

/// Ruling 2, composition-shaped: the real daemon under SIGINT preserves the
/// guest's state exactly as SIGKILL does — crash and clean shutdown agree
/// on disk — and the clean path additionally exits 0 having flushed a
/// ledger that carries the suspension.
#[cfg(unix)]
#[test]
fn sigint_and_sigkill_agree_on_disk_and_sigint_flushes_the_suspension() {
    let clean = home("sigint");
    let (clean_paths, _) = paths(&clean, serde_json::json!(["jinn:fs"]), "fs-bundle");
    let mut child = spawn_daemon(&clean_paths);
    signal(&child, "INT");
    let status = child.wait().unwrap_or_else(|error| panic!("wait: {error}"));
    assert_eq!(status.code(), Some(0), "clean shutdown exits 0");
    assert_bundle_state(&clean_paths.data);
    assert_eq!(inverse_files(&clean_paths), 4);

    let crashed = home("sigkill");
    let (crash_paths, _) = paths(&crashed, serde_json::json!(["jinn:fs"]), "fs-bundle");
    let mut child = spawn_daemon(&crash_paths);
    signal(&child, "KILL");
    let _ = child.wait();
    assert_eq!(
        snapshot(&crash_paths.data),
        snapshot(&clean_paths.data),
        "SIGINT and SIGKILL agree on the disk outcome"
    );
    assert_eq!(inverse_files(&crash_paths), inverse_files(&clean_paths));

    let runtime = tokio::runtime::Runtime::new().unwrap_or_else(|error| panic!("{error}"));
    let records = runtime.block_on(async {
        let reopened =
            Daemon::open(clean_paths.clone()).unwrap_or_else(|error| panic!("reopen: {error:?}"));
        events(&reopened).await
    });
    assert!(
        records
            .iter()
            .any(|record| matches!(record.kind, LedgerEventKind::FiberSuspended { retained: 4 })),
        "the flushed ledger carries the suspension: {records:?}"
    );
}

/// Ruling 3: a config-edit restart replaces the incarnation, not the entry.
/// The persisted documents survive; the successor's keyed writes — same
/// fiber, same keys — are answered from the record (03 §Act: exactly
/// once, nothing mutates twice); the successor keyed-reverts an INHERITED
/// effect; and profile removal then withdraws the whole inherited trail,
/// strictly LIFO, leaving no orphaned inverse.
#[tokio::test]
async fn a_reconcile_restart_hands_the_successor_the_entrys_journal() {
    let home = home("restart");
    let (paths, hash) = paths(&home, serde_json::json!(["jinn:fs"]), "fs-bundle");
    let daemon = booted(paths.clone()).await;
    let fiber = daemon
        .entry_fiber("scribe")
        .unwrap_or_else(|| panic!("the entry has a fiber"));
    let inherited = daemon.fs_effects();
    assert_eq!(inherited.len(), 4);

    // The edit keeps the mode and changes the config document: a restate.
    write_profile(
        &home,
        serde_json::json!([scribe(
            &hash,
            serde_json::json!(["jinn:fs"]),
            serde_json::json!({ "data": "fs-bundle", "revision": 2 })
        )]),
    );
    let report = daemon
        .reload()
        .await
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    assert_eq!(
        report.restarted.len(),
        1,
        "exactly the edited entry restarted"
    );
    assert_eq!(daemon.entry_fiber("scribe"), Some(fiber), "same cell");
    assert_bundle_state(&paths.data);
    assert_eq!(
        inverse_files(&paths),
        4,
        "the successor's keyed replays registered nothing new (03 §Act)"
    );
    let records = events(&daemon).await;
    assert!(
        records
            .iter()
            .filter(|record| matches!(
                &record.kind,
                LedgerEventKind::ContractCall { contract, operation }
                    if contract == "jinn:fs" && operation == "write"
            ))
            .count()
            >= 4,
        "the successor's writes crossed the broker again: {records:?}"
    );
    assert!(
        records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::FiberSuspended { retained: 4 }
        ) && record.fiber == Some(fiber)),
        "the restart suspended the first incarnation: {records:?}"
    );

    // The successor reverts an inherited effect by key: the first
    // incarnation's `remove b.txt` restores "bee".
    let (removed, _) = inherited[3];
    let resolution = daemon
        .revert(removed, "k4-inherited")
        .await
        .unwrap_or_else(|error| panic!("revert: {error:?}"));
    assert_eq!(resolution, RevertResolution::Reverted);
    assert_eq!(
        std::fs::read(paths.data.join("log/b.txt")).ok(),
        Some(b"bee".to_vec())
    );
    assert_eq!(inverse_files(&paths), 3);

    // Profile removal: the entry leaves the composition — its WHOLE trail
    // withdraws LIFO, the successor's effects first, then the inherited.
    write_profile(&home, serde_json::json!([]));
    let report = daemon
        .reload()
        .await
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    assert_eq!(report.disposed.len(), 1);
    assert!(!paths.data.join("log/a.txt").exists());
    assert!(!paths.data.join("log/b.txt").exists());
    assert_eq!(inverse_files(&paths), 0, "no orphaned inverse");
    assert!(daemon.fs_effects().is_empty());
    assert_eq!(
        fs_withdrawals(&events(&daemon).await, fiber),
        vec![
            "fs remove log/b.txt",
            "fs write log/b.txt",
            "fs append log/a.txt",
            "fs write log/a.txt",
        ],
        "the inherited trail withdraws strictly LIFO (the reverted effect a clean no-op)"
    );
}

/// Ruling 3 across a process restart: the retention store carries the
/// entry's journal to the next daemon; removal there withdraws the trail
/// of both processes' incarnations.
#[tokio::test]
async fn an_entrys_journal_survives_a_process_restart_until_its_removal() {
    let home = home("reboot");
    let (paths, _) = paths(&home, serde_json::json!(["jinn:fs"]), "fs-bundle");
    let first = booted(paths.clone()).await;
    first
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
    drop(first);
    assert_eq!(inverse_files(&paths), 4);

    let second = booted(paths.clone()).await;
    assert_eq!(
        inverse_files(&paths),
        8,
        "the successor incarnation's effects join"
    );
    assert_eq!(second.fs_effects().len(), 8);
    write_profile(&home, serde_json::json!([]));
    second
        .reload()
        .await
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    assert!(!paths.data.join("log").exists() || snapshot(&paths.data).is_empty());
    assert_eq!(
        inverse_files(&paths),
        0,
        "both processes' inverses withdrawn"
    );
    assert!(second.fs_effects().is_empty());
}

/// An entry removed from the profile while the daemon was down left the
/// composition: its retained journal withdraws at the next boot (I4 — a
/// fresh boot of the final configuration shows no trace of it).
#[tokio::test]
async fn a_journal_of_an_entry_removed_while_down_withdraws_at_boot() {
    let home = home("orphan");
    let (paths, _) = paths(&home, serde_json::json!(["jinn:fs"]), "fs-bundle");
    let first = booted(paths.clone()).await;
    first
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
    drop(first);
    write_profile(&home, serde_json::json!([]));

    let second = booted(paths.clone()).await;
    assert!(
        snapshot(&paths.data).is_empty(),
        "no trace of the removed entry"
    );
    assert_eq!(inverse_files(&paths), 0);
    let withdrawn = events(&second)
        .await
        .into_iter()
        .filter(|record| {
            matches!(&record.kind, LedgerEventKind::EffectWithdrawn { label, clean: true } if label.starts_with("fs "))
                && record.entry.as_ref().is_some_and(|entry| entry.0 == "scribe")
        })
        .count();
    assert_eq!(
        withdrawn, 4,
        "the withdrawal is on the record under the entry"
    );
}

/// FINDINGS #15 shape, ruled by #16 (M2-K5): the wake handler appends,
/// dawdles, and appends again; a dispose landing mid-tick DRAINS the
/// in-flight handler under the guest deadline before sealing, so BOTH
/// appends land in the journal (never a torn prefix, never a refusal on the
/// record for a sub-deadline handler), and then the whole trail withdraws
/// LIFO — both appends undone, no orphaned inverse (I1, R5).
#[tokio::test]
async fn a_dispose_during_a_mid_tick_handler_drains_then_seals() {
    let home = home("drain-dispose");
    let (paths, _) = paths(
        &home,
        serde_json::json!(["jinn:fs", "jinn:clock"]),
        "fs-on-wake-busy",
    );
    let daemon = booted(paths.clone()).await;
    let fiber = daemon
        .entry_fiber("scribe")
        .unwrap_or_else(|| panic!("the entry has a fiber"));
    await_first_append(&paths).await;

    write_profile(&home, serde_json::json!([]));
    let report = daemon
        .reload()
        .await
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    assert_eq!(report.disposed.len(), 1);

    assert!(
        !paths.data.join("wakes.log").exists(),
        "the trail withdrew whole: {:?}",
        std::fs::read(paths.data.join("wakes.log"))
    );
    assert_eq!(inverse_files(&paths), 0, "no orphaned inverse");
    let records = events(&daemon).await;
    assert!(
        !records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::ErrorRecorded { error } if error.message.contains("sealed")
        ) && record.fiber == Some(fiber)),
        "a drained handler is never refused: {records:?}"
    );
    assert_eq!(appends(&records), 2, "both appends landed before the seal");
    assert_eq!(
        fs_withdrawals(&records, fiber),
        vec!["fs append wakes.log", "fs append wakes.log"],
        "both withdrew, LIFO"
    );
}

/// FINDINGS #16 (M2-K5), the planned-stop shape: a clean shutdown landing
/// mid-tick suspends AFTER the in-flight handler finishes — every related
/// effect the handler makes lands on disk and in the journal, the
/// suspension retains them all, and nothing is refused on the record.
#[tokio::test]
async fn a_suspend_during_a_mid_tick_handler_lands_every_effect() {
    let home = home("drain-suspend");
    let (paths, _) = paths(
        &home,
        serde_json::json!(["jinn:fs", "jinn:clock"]),
        "fs-on-wake-busy",
    );
    let daemon = booted(paths.clone()).await;
    let fiber = daemon
        .entry_fiber("scribe")
        .unwrap_or_else(|| panic!("the entry has a fiber"));
    await_first_append(&paths).await;

    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));

    assert_eq!(
        std::fs::read(paths.data.join("wakes.log")).ok().as_deref(),
        Some(b"tick\ntock\n".as_slice()),
        "the whole tick landed, never a prefix"
    );
    assert_eq!(inverse_files(&paths), 2, "both inverses retained");
    let records = events(&daemon).await;
    assert!(
        !records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::ErrorRecorded { error } if error.message.contains("sealed")
        )),
        "nothing refused on a planned stop: {records:?}"
    );
    assert_eq!(appends(&records), 2);
    assert!(
        records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::FiberSuspended { retained: 2 }
        ) && record.fiber == Some(fiber)),
        "the suspension retained both: {records:?}"
    );
}

async fn await_first_append(paths: &DaemonPaths) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::fs::read(paths.data.join("wakes.log")).ok().as_deref() != Some(b"tick\n") {
        assert!(Instant::now() < deadline, "the first append lands");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn appends(records: &[LedgerRecord]) -> usize {
    records
        .iter()
        .filter(|record| {
            matches!(
                &record.kind,
                LedgerEventKind::EffectRegistered { label } if label.starts_with("fs append")
            )
        })
        .count()
}
