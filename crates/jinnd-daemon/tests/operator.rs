//! M2-K5 acceptance, through the real daemon: the operator lane (stop,
//! edit, start) never loses work or lies in the log. An external edit that
//! lands DURING a slow reconcile is applied, never swallowed as the daemon's
//! own write-back echo (FINDINGS #17); the daemon recognizes its echo by the
//! bytes it wrote, so an identical operator rewrite still reconciles
//! `unchanged`. And the `jinnd` binary arms its file watcher BEFORE the
//! boot reconcile writes any evidence, resolves a relative `--profile`
//! against the working directory, and emits one machine-readable readiness
//! line only once the watcher is armed and the boot reconcile is done
//! (FINDINGS #18, #12 minimum).

mod support;

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use jinnd_api::EntryId;
use jinnd_daemon::{Daemon, DaemonPaths};

struct Home(PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn home(name: &str) -> Home {
    let root =
        std::env::temp_dir().join(format!("jinnd-operator-test-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
    Home(root)
}

fn entry(id: &str, hash: &str, mode: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "package": "demo/counter-plugin",
        "version": "0.0.1",
        "hash": hash,
        "config": { "grants": ["jinn:fs", "jinn:clock"], "data": mode },
    })
}

/// The OPERATOR's rendering of the profile — deliberately not the daemon's
/// write-back form, exactly as a launcher or a human writes it.
fn write_profile(home: &Home, entries: serde_json::Value) {
    let text = serde_json::to_string_pretty(&serde_json::json!({ "entries": entries }))
        .unwrap_or_else(|error| panic!("{error}"));
    let temp = home.0.join("profile.json.tmp");
    std::fs::write(&temp, text).unwrap_or_else(|error| panic!("{error}"));
    std::fs::rename(&temp, home.0.join("profile.json")).unwrap_or_else(|error| panic!("{error}"));
}

fn paths(home: &Home) -> (DaemonPaths, String) {
    let (bytes, hash) = support::pinned_fixture();
    std::fs::write(home.0.join("artifacts/counter-plugin.wasm"), &bytes)
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

/// FINDINGS #17 transcript (`grants-6778`): a reconcile that restarts the
/// busy fixture mid-tick is slow (the dispose drains the 600 ms handler,
/// M2-K5 #16); an operator edit landing inside that window MUST be applied
/// by the watcher's next delivery — never swallowed as the daemon's own
/// echo under an all-empty success line (Law 2).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_external_edit_landing_during_a_slow_reconcile_is_applied() {
    let home = home("edit-during-reconcile");
    let (paths, hash) = paths(&home);
    write_profile(
        &home,
        serde_json::json!([entry("scribe", &hash, "fs-on-wake-busy")]),
    );
    let daemon = Arc::new(Daemon::open(paths.clone()).unwrap_or_else(|error| panic!("{error:?}")));
    let report = daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot: {error:?}"));
    assert!(report.errors.is_empty(), "clean boot: {:?}", report.errors);
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::fs::read(paths.data.join("wakes.log")).ok().as_deref() != Some(b"tick\n") {
        assert!(Instant::now() < deadline, "the first append lands");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Edit 1 restarts scribe (new data) — its dispose drains the busy tick.
    write_profile(
        &home,
        serde_json::json!([entry("scribe", &hash, "fs-on-wake")]),
    );
    let slow = {
        let daemon = Arc::clone(&daemon);
        tokio::spawn(async move { daemon.deliver().await })
    };
    tokio::time::sleep(Duration::from_millis(150)).await;
    // Edit 2 lands while edit 1's reconcile is still applying.
    write_profile(
        &home,
        serde_json::json!([
            entry("scribe", &hash, "fs-on-wake"),
            entry("second", &hash, "noop")
        ]),
    );
    let first = slow
        .await
        .unwrap_or_else(|error| panic!("join: {error:?}"))
        .unwrap_or_else(|error| panic!("deliver 1: {error:?}"))
        .unwrap_or_else(|| panic!("edit 1 is not an echo"));
    assert_eq!(first.restarted, vec![EntryId("scribe".to_owned())]);

    let second = daemon
        .deliver()
        .await
        .unwrap_or_else(|error| panic!("deliver 2: {error:?}"))
        .unwrap_or_else(|| panic!("edit 2 was swallowed as the daemon's own echo"));
    assert_eq!(
        second.created,
        vec![EntryId("second".to_owned())],
        "edit 2 applied"
    );
    assert!(daemon.entry_fiber("second").is_some());
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// The echo is recognized by the bytes the daemon WROTE: its own write-back
/// delivery is `None` (no lying `reconciled` line), while an identical
/// operator rewrite — the same operator bytes again — reconciles
/// `unchanged`, never skipped (the harness's byte-varying mitigation can
/// retire).
#[tokio::test]
async fn own_write_back_is_an_echo_but_an_identical_operator_rewrite_reconciles() {
    let home = home("echo");
    let (paths, hash) = paths(&home);
    write_profile(&home, serde_json::json!([entry("scribe", &hash, "noop")]));
    let daemon = Daemon::open(paths.clone()).unwrap_or_else(|error| panic!("{error:?}"));
    daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot: {error:?}"));

    // The file now holds the daemon's own write-back.
    let echo = daemon
        .deliver()
        .await
        .unwrap_or_else(|error| panic!("deliver: {error:?}"));
    assert!(
        echo.is_none(),
        "the daemon's own write-back is an echo: {echo:?}"
    );

    write_profile(&home, serde_json::json!([entry("scribe", &hash, "noop")]));
    let rewrite = daemon
        .deliver()
        .await
        .unwrap_or_else(|error| panic!("deliver: {error:?}"))
        .unwrap_or_else(|| panic!("an operator rewrite is never an echo"));
    assert_eq!(rewrite.unchanged, vec![EntryId("scribe".to_owned())]);
    // ...and the write-back of THAT reconcile is again an echo.
    let echo = daemon
        .deliver()
        .await
        .unwrap_or_else(|error| panic!("deliver: {error:?}"));
    assert!(echo.is_none(), "{echo:?}");
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// Round-2 blocker (the verifier's probe): own-write recognition is
/// ONE-SHOT. The remembered bytes identify exactly one delivery — the
/// watcher's echo of the daemon's own save — and are consumed on that
/// match; an operator's atomic rewrite of the BYTE-IDENTICAL text read
/// back from the daemon is a later delivery of the same bytes, and it
/// reconciles `unchanged` — never skipped as an echo.
#[tokio::test]
async fn a_byte_identical_operator_rewrite_reconciles_unchanged() {
    let home = home("byte-identical");
    let (paths, hash) = paths(&home);
    write_profile(&home, serde_json::json!([entry("scribe", &hash, "noop")]));
    let daemon = Daemon::open(paths.clone()).unwrap_or_else(|error| panic!("{error:?}"));
    daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot: {error:?}"));
    let echo = daemon
        .deliver()
        .await
        .unwrap_or_else(|error| panic!("deliver: {error:?}"));
    assert!(echo.is_none(), "first delivery is the echo: {echo:?}");

    // The operator rewrites EXACTLY the bytes the daemon wrote back.
    let read_back = std::fs::read(&paths.profile).unwrap_or_else(|error| panic!("{error}"));
    let temp = home.0.join("profile.json.tmp");
    std::fs::write(&temp, &read_back).unwrap_or_else(|error| panic!("{error}"));
    std::fs::rename(&temp, &paths.profile).unwrap_or_else(|error| panic!("{error}"));
    let rewrite = daemon
        .deliver()
        .await
        .unwrap_or_else(|error| panic!("deliver: {error:?}"))
        .unwrap_or_else(|| panic!("identical_operator_rewrite_reconciled=false"));
    assert_eq!(rewrite.unchanged, vec![EntryId("scribe".to_owned())]);
    assert!(rewrite.created.is_empty() && rewrite.restarted.is_empty());
    assert!(rewrite.disposed.is_empty() && rewrite.errors.is_empty());
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// The two-echo hazard: two consecutive daemon write-backs each consume
/// their own echo, and an operator edit landing between a write-back and
/// its echo delivery is applied — the stale signature is superseded by the
/// save that edit causes, never left lying in wait for a later identical
/// rewrite.
#[tokio::test]
async fn consecutive_write_backs_each_consume_their_own_echo() {
    let home = home("two-echo");
    let (paths, hash) = paths(&home);
    write_profile(&home, serde_json::json!([entry("scribe", &hash, "noop")]));
    let daemon = Daemon::open(paths.clone()).unwrap_or_else(|error| panic!("{error:?}"));
    daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot: {error:?}"));
    // Write-back 1 is pending; the operator edits BEFORE its echo delivers.
    write_profile(
        &home,
        serde_json::json!([
            entry("scribe", &hash, "noop"),
            entry("second", &hash, "noop")
        ]),
    );
    let applied = daemon
        .deliver()
        .await
        .unwrap_or_else(|error| panic!("deliver: {error:?}"))
        .unwrap_or_else(|| panic!("the operator edit was swallowed as write-back 1's echo"));
    assert_eq!(applied.created, vec![EntryId("second".to_owned())]);
    // Write-back 2 (of that reconcile) echoes: consumed, no reconcile.
    let echo = daemon
        .deliver()
        .await
        .unwrap_or_else(|error| panic!("deliver: {error:?}"));
    assert!(echo.is_none(), "write-back 2's echo: {echo:?}");
    // An explicit reload is write-back 3; its echo is consumed exactly once.
    daemon
        .reload()
        .await
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    let echo = daemon
        .deliver()
        .await
        .unwrap_or_else(|error| panic!("deliver: {error:?}"));
    assert!(echo.is_none(), "write-back 3's echo: {echo:?}");
    let read_back = std::fs::read(&paths.profile).unwrap_or_else(|error| panic!("{error}"));
    let temp = home.0.join("profile.json.tmp");
    std::fs::write(&temp, &read_back).unwrap_or_else(|error| panic!("{error}"));
    std::fs::rename(&temp, &paths.profile).unwrap_or_else(|error| panic!("{error}"));
    let rewrite = daemon
        .deliver()
        .await
        .unwrap_or_else(|error| panic!("deliver: {error:?}"))
        .unwrap_or_else(|| panic!("a consumed echo signature cannot swallow a rewrite"));
    assert_eq!(rewrite.unchanged.len(), 2, "{rewrite:?}");
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// The real `jinnd` binary, run from `cwd` with `args`; its stderr is
/// drained on a thread for the whole run (a full pipe must never stall the
/// daemon), so every line is available after the fact.
struct Jinnd {
    child: std::process::Child,
    lines: Arc<std::sync::Mutex<Vec<String>>>,
    drained: Option<std::thread::JoinHandle<()>>,
}

impl Jinnd {
    fn spawn(cwd: &std::path::Path, args: &[&str]) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_jinnd"))
            .args(args)
            .current_dir(cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|error| panic!("spawn jinnd: {error}"));
        let stderr = child
            .stderr
            .take()
            .unwrap_or_else(|| panic!("stderr piped"));
        let lines = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = Arc::clone(&lines);
        let drained = std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                let Ok(line) = line else { break };
                sink.lock()
                    .unwrap_or_else(|poison| poison.into_inner())
                    .push(line);
            }
        });
        Self {
            child,
            lines,
            drained: Some(drained),
        }
    }

    fn lines(&self) -> Vec<String> {
        self.lines
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    /// Waits until a stderr line contains `needle` (`true`) or the process
    /// exits first (`false`).
    fn wait_for(&mut self, needle: &str) -> bool {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if self.lines().iter().any(|line| line.contains(needle)) {
                return true;
            }
            if let Ok(Some(_)) = self.child.try_wait() {
                self.drain();
                return self.lines().iter().any(|line| line.contains(needle));
            }
            assert!(
                Instant::now() < deadline,
                "jinnd answered in time: {:?}",
                self.lines()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn drain(&mut self) {
        if let Some(drained) = self.drained.take() {
            let _ = drained.join();
        }
    }

    fn interrupt(&self) {
        let status = Command::new("kill")
            .args(["-INT", &self.child.id().to_string()])
            .status()
            .unwrap_or_else(|error| panic!("kill: {error}"));
        assert!(status.success());
    }

    fn wait(&mut self) -> std::process::ExitStatus {
        let status = self
            .child
            .wait()
            .unwrap_or_else(|error| panic!("wait: {error}"));
        self.drain();
        status
    }
}

const READY: &str = r#""jinnd":"ready""#;

/// FINDINGS #18 + #12 minimum, through the binary: a RELATIVE `--profile`
/// resolves against the working directory — the daemon boots, arms its
/// watcher, and emits exactly one machine-readable readiness line AFTER
/// the boot reconcile; SIGINT then stops it cleanly (exit 0).
#[cfg(unix)]
#[test]
fn a_relative_profile_boots_watched_and_announces_readiness() {
    let home = home("relative");
    let (_, hash) = paths(&home);
    write_profile(&home, serde_json::json!([entry("scribe", &hash, "noop")]));
    let mut jinnd = Jinnd::spawn(
        &home.0,
        &["--profile", "profile.json", "--ledger", "ledger.sqlite"],
    );
    assert!(jinnd.wait_for(READY), "still serving: {:?}", jinnd.lines());
    let lines = jinnd.lines();
    let ready = lines
        .iter()
        .position(|line| line.contains(READY))
        .unwrap_or_else(|| panic!("a readiness line: {lines:?}"));
    let reconciled = lines
        .iter()
        .position(|line| line.contains("reconciled"))
        .unwrap_or_else(|| panic!("the boot reconcile logged: {lines:?}"));
    assert!(reconciled < ready, "readiness follows the boot reconcile");
    assert!(
        lines[ready].contains(r#""watcher":"armed""#),
        "{}",
        lines[ready]
    );
    assert!(home.0.join("ledger.sqlite").exists());

    jinnd.interrupt();
    let status = jinnd.wait();
    assert_eq!(status.code(), Some(0), "clean stop: {:?}", jinnd.lines());
}

/// FINDINGS #18: a watcher that cannot arm (the profile's directory does
/// not exist) refuses BEFORE the boot reconcile — no guest activates, no
/// ledger or data evidence is written, no readiness line — and exits 1.
#[cfg(unix)]
#[test]
fn a_refused_watcher_writes_no_evidence() {
    let home = home("unwatched");
    let (_, hash) = paths(&home);
    write_profile(&home, serde_json::json!([entry("scribe", &hash, "noop")]));
    let missing = home.0.join("missing");
    let profile = missing.join("profile.json");
    let ledger = home.0.join("ledger.sqlite");
    let mut jinnd = Jinnd::spawn(
        &home.0,
        &[
            "--profile",
            &profile.to_string_lossy(),
            "--ledger",
            &ledger.to_string_lossy(),
            "--artifacts",
            &home.0.join("artifacts").to_string_lossy(),
            "--data",
            &home.0.join("data").to_string_lossy(),
        ],
    );
    assert!(
        !jinnd.wait_for(READY),
        "no readiness line: {:?}",
        jinnd.lines()
    );
    let status = jinnd.wait();
    assert_eq!(status.code(), Some(1), "{:?}", jinnd.lines());
    let lines = jinnd.lines();
    assert!(
        !lines.iter().any(|line| line.contains("reconciled")),
        "{lines:?}"
    );
    assert!(!ledger.exists(), "no ledger evidence");
    assert!(!home.0.join("data").exists(), "no data evidence");
}
