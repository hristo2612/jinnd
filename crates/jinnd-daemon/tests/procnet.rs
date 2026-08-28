//! M2-K6 acceptance, through the real daemon assembly: a fixture plugin
//! spawns a long-lived child with streams, kills one with a typed signal,
//! and sees exactly the environment its grant allows; suspend kills the
//! child and ledgers it, the next activate re-spawns, dispose leaves no
//! zombie in the process table; a fixture listens on loopback and echoes a
//! real TCP connection, suspend releases the listener; and every grant
//! refusal — no grant, a link out of the exec allowlist, a port outside
//! the range, a non-loopback bind, a wrong-typed scope — lands on the
//! record fail-closed (Law 1, Law 2, R1, R9, R11).

mod support;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use jinnd_api::{FiberId, LedgerEventKind, LedgerRecord};
use jinnd_daemon::{Daemon, DaemonPaths};

struct Home(PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn home(name: &str) -> Home {
    let root =
        std::env::temp_dir().join(format!("jinnd-procnet-test-{name}-{}", std::process::id()));
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

fn entry(hash: &str, grants: serde_json::Value, mode: &str) -> serde_json::Value {
    serde_json::json!({
        "id": "worker",
        "package": "demo/counter-plugin",
        "version": "0.0.1",
        "hash": hash,
        "config": { "grants": grants, "data": mode },
    })
}

fn paths(home: &Home, grants: serde_json::Value, mode: &str) -> (DaemonPaths, String) {
    let (bytes, hash) = support::pinned_fixture();
    std::fs::write(home.0.join("artifacts/counter-plugin.wasm"), &bytes)
        .unwrap_or_else(|error| panic!("{error}"));
    write_profile(home, serde_json::json!([entry(&hash, grants, mode)]));
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

fn process_grant(exec: &[&str]) -> serde_json::Value {
    serde_json::json!({ "contract": "jinn:process", "scope": { "exec": exec, "env": "inherit-none" } })
}

fn net_grant(low: u16, high: u16) -> serde_json::Value {
    serde_json::json!({ "contract": "jinn:net", "scope": { "bind": [low, high] } })
}

fn grant_refusals(records: &[LedgerRecord], contract: &str) -> usize {
    records
        .iter()
        .filter(|record| {
            matches!(&record.kind, LedgerEventKind::GrantRefused { contract: refused } if refused == contract)
        })
        .count()
}

fn spawned_pids(records: &[LedgerRecord]) -> Vec<(u32, Option<FiberId>)> {
    records
        .iter()
        .filter_map(|record| match &record.kind {
            LedgerEventKind::ProcessSpawned { pid, .. } => Some((*pid, record.fiber)),
            _ => None,
        })
        .collect()
}

/// The child's process-table state: `None` once reaped (no row), `Some`
/// with the `ps` state otherwise (`Z` is a zombie).
fn process_state(pid: u32) -> Option<String> {
    let output = std::process::Command::new("ps")
        .args(["-o", "stat=", "-p", &pid.to_string()])
        .output()
        .unwrap_or_else(|error| panic!("ps: {error}"));
    let state = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    (!state.is_empty()).then_some(state)
}

fn assert_reaped(pid: u32) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match process_state(pid) {
            None => return,
            Some(state) => {
                assert!(
                    !state.starts_with('Z'),
                    "pid {pid} is a zombie: the host must reap"
                );
                assert!(Instant::now() < deadline, "pid {pid} still alive: {state}");
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .unwrap_or_else(|error| panic!("free port: {error}"))
}

fn connect(port: u16) -> Option<TcpStream> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(stream) = TcpStream::connect(("127.0.0.1", port)) {
            return Some(stream);
        }
        if Instant::now() > deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Card acceptance: a long-lived child through the real daemon —
/// stdin streamed in, stdout streamed out, a clean exit observed by a
/// bounded wait; spawn and exit ledgered with the fiber's attribution.
#[tokio::test]
async fn a_fixture_spawns_a_child_streams_stdin_to_stdout_and_waits() {
    let home = home("echo");
    let (paths, _) = paths(
        &home,
        serde_json::json!([process_grant(&["/bin/cat"]), "jinn:clock", "jinn:fs"]),
        "proc-echo",
    );
    let daemon = booted(paths.clone()).await;
    let fiber = daemon.entry_fiber("worker");
    assert_eq!(
        std::fs::read(paths.data.join("proc-echo.out")).ok(),
        Some(b"hello\n".to_vec()),
        "stdin round-tripped through the child's stdout"
    );
    let records = events(&daemon).await;
    assert!(
        records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::ProcessSpawned { command, .. } if command == "/bin/cat"
        ) && record.fiber == fiber),
        "the spawn is a ledger event with attribution: {records:?}"
    );
    assert!(
        records
            .iter()
            .any(|record| matches!(&record.kind, LedgerEventKind::ProcessExited { code: 0, .. })),
        "the exit is a ledger event: {records:?}"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// Card acceptance: suspend kills the child and ledgers it, the next
/// activate re-spawns, dispose leaves no zombie (the verifier's process
/// table check).
#[tokio::test]
async fn suspend_kills_the_child_activate_respawns_and_dispose_leaves_no_zombie() {
    let home = home("sleeper");
    let (paths, _) = paths(
        &home,
        serde_json::json!([process_grant(&["/bin/sleep"])]),
        "proc-sleeper",
    );
    let first = booted(paths.clone()).await;
    let fiber = first.entry_fiber("worker");
    let pids = spawned_pids(&events(&first).await);
    assert_eq!(pids.len(), 1, "one spawn on the record");
    let (pid, attributed) = pids[0];
    assert_eq!(attributed, fiber, "attributed to the spawning fiber");
    assert!(process_state(pid).is_some(), "the child is live");

    first
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
    assert_reaped(pid);
    let records = events(&first).await;
    assert!(
        records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::ProcessKilled { signal, .. } if signal == "kill"
        )),
        "suspend killed the child on the record: {records:?}"
    );
    assert!(
        records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::EffectWithdrawn { label, clean: true }
                if label.starts_with("jinn:process spawn")
        )),
        "the registration's release is ledgered: {records:?}"
    );
    assert!(
        records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::FiberSuspended { retained: 0 }
        )),
        "a kernel registration is never retained: {records:?}"
    );
    drop(first);

    // Activate re-spawns: the next incarnation holds a fresh child.
    let second = booted(paths.clone()).await;
    let pids = spawned_pids(&events(&second).await);
    assert_eq!(pids.len(), 2, "the successor incarnation re-spawned");
    let (respawned, _) = pids[1];
    assert!(process_state(respawned).is_some(), "the new child is live");

    // Dispose: the entry leaves the profile; the child is killed and
    // reaped — no zombie, nothing refused.
    write_profile(&home, serde_json::json!([]));
    let report = second
        .reload()
        .await
        .unwrap_or_else(|error| panic!("reload: {error:?}"));
    assert_eq!(report.disposed.len(), 1);
    assert_reaped(respawned);
    let records = events(&second).await;
    assert!(
        !records
            .iter()
            .any(|record| matches!(&record.kind, LedgerEventKind::ErrorRecorded { .. })),
        "a clean dispose: {records:?}"
    );
}

/// `kill` delivers a typed signal; `wait` answers the negated signal.
#[cfg(unix)]
#[tokio::test]
async fn kill_delivers_a_typed_signal_and_wait_reports_it() {
    let home = home("kill");
    let (paths, _) = paths(
        &home,
        serde_json::json!([process_grant(&["/bin/sleep"]), "jinn:clock", "jinn:fs"]),
        "proc-kill",
    );
    let daemon = booted(paths.clone()).await;
    assert_eq!(
        std::fs::read(paths.data.join("proc-kill.out")).ok(),
        Some(b"-15".to_vec()),
        "SIGTERM reported as the negated signal"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// Hostile probe (env leakage): under `inherit-none` the child sees the
/// guest's explicit variables and nothing of the daemon's environment.
#[tokio::test]
async fn inherit_none_leaks_nothing_of_the_daemons_environment() {
    let home = home("env");
    let (paths, _) = paths(
        &home,
        serde_json::json!([process_grant(&["/usr/bin/env"]), "jinn:clock", "jinn:fs"]),
        "proc-env",
    );
    let daemon = booted(paths.clone()).await;
    let listing = std::fs::read_to_string(paths.data.join("proc-env.out"))
        .unwrap_or_else(|error| panic!("listing: {error}"));
    assert!(
        listing.contains("JINND_GUEST_VAR=from-guest"),
        "the guest's explicit variable reaches the child: {listing}"
    );
    assert!(
        std::env::var_os("HOME").is_some(),
        "the test process itself has a HOME to leak"
    );
    assert!(
        !listing
            .lines()
            .any(|line| line.starts_with("HOME=") || line.starts_with("PATH=")),
        "nothing of the daemon's environment leaked: {listing}"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// The one-shot `run` keeps working through the same grant.
#[tokio::test]
async fn run_answers_a_one_shot_commands_output() {
    let home = home("run");
    let (paths, _) = paths(
        &home,
        serde_json::json!([process_grant(&["/bin/echo"]), "jinn:fs"]),
        "proc-run",
    );
    let daemon = booted(paths.clone()).await;
    assert_eq!(
        std::fs::read(paths.data.join("proc-run.out")).ok(),
        Some(b"hi\n".to_vec())
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// Round 4 (R3; the world mirrors its bundle): output past the cap reaches
/// the GUEST as the bundle's `output-truncated` variant — the fixture
/// matches it on the wire, so a clean activation is the proof; the ledger
/// reads truncated → killed → exited, and the child is reaped.
#[tokio::test]
async fn run_past_the_output_cap_is_the_typed_truncation_on_the_guest_wire() {
    let home = home("truncated");
    let (paths, _) = paths(
        &home,
        serde_json::json!([process_grant(&["/bin/cat"])]),
        "proc-truncated",
    );
    let daemon = booted(paths.clone()).await;
    let pids = spawned_pids(&events(&daemon).await);
    assert_eq!(pids.len(), 1, "one spawn on the record");
    assert_reaped(pids[0].0);
    let records = events(&daemon).await;
    let position = |probe: fn(&LedgerEventKind) -> bool| {
        records
            .iter()
            .position(|record| probe(&record.kind))
            .unwrap_or_else(|| panic!("on the record: {records:?}"))
    };
    let truncated = position(|kind| matches!(kind, LedgerEventKind::ProcessOutputTruncated { .. }));
    let killed = position(|kind| matches!(kind, LedgerEventKind::ProcessKilled { .. }));
    let exited =
        position(|kind| matches!(kind, LedgerEventKind::ProcessExited { code, .. } if *code < 0));
    assert!(truncated < killed && killed < exited, "{records:?}");
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// No grant, no spawn (red-first): refused at the broker choke point, on
/// the record; the entry activates cleanly around the refusal (R11).
#[tokio::test]
async fn no_grant_no_spawn_on_the_record() {
    let home = home("denied");
    let (paths, _) = paths(&home, serde_json::json!([]), "proc-denied");
    let daemon = booted(paths.clone()).await;
    let records = events(&daemon).await;
    assert_eq!(grant_refusals(&records, "jinn:process"), 1, "{records:?}");
    assert!(spawned_pids(&records).is_empty(), "nothing spawned");
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// Hostile probe (exec-allowlist escape via symlink): a link INSIDE an
/// allowlisted prefix pointing OUT of it is refused — containment is
/// decided on the fully resolved executable (K3 doctrine).
#[cfg(unix)]
#[tokio::test]
async fn the_exec_allowlist_is_decided_after_symlink_resolution() {
    let home = home("escape");
    let bin = home.0.join("bin");
    std::fs::create_dir_all(&bin).unwrap_or_else(|error| panic!("{error}"));
    let link = bin.join("innocent");
    std::os::unix::fs::symlink("/bin/sh", &link).unwrap_or_else(|error| panic!("{error}"));
    let allowed = bin.to_string_lossy().into_owned();
    let (paths, _) = paths(
        &home,
        serde_json::json!([process_grant(&[&allowed])]),
        &format!("proc-escape:{}", link.display()),
    );
    let daemon = booted(paths.clone()).await;
    let records = events(&daemon).await;
    assert_eq!(
        grant_refusals(&records, "jinn:process"),
        1,
        "the link out of the allowlist refused on the record: {records:?}"
    );
    assert!(spawned_pids(&records).is_empty(), "nothing spawned");
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// Red-first, one refusal per scope-type mismatch for both bundles
/// (M2-K2 law): a path scope on `jinn:process` and a rate scope on
/// `jinn:net` each refuse the grant at admission, ledgered per entry, and
/// authority is never widened — the spawn is still refused at the choke
/// point.
#[tokio::test]
async fn a_wrong_typed_scope_refuses_each_bundle_fail_closed() {
    let home = home("mismatch");
    let (paths, _) = paths(
        &home,
        serde_json::json!([
            { "contract": "jinn:process", "scope": "/bin" },
            { "contract": "jinn:net", "scope": 9 },
        ]),
        "proc-denied",
    );
    let daemon = booted(paths.clone()).await;
    let records = events(&daemon).await;
    let admission_refusal = |contract: &str, declared: &str| {
        records.iter().any(|record| match &record.kind {
            LedgerEventKind::ErrorRecorded { error } => {
                error.message.contains(contract) && error.message.contains(declared)
            }
            _ => false,
        })
    };
    assert!(
        admission_refusal("jinn:process", "process-policy"),
        "the process scope mismatch names the declared type: {records:?}"
    );
    assert!(
        admission_refusal("jinn:net", "net-policy"),
        "the net scope mismatch names the declared type: {records:?}"
    );
    assert_eq!(
        grant_refusals(&records, "jinn:process"),
        1,
        "never widened: the spawn refused at the choke point"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// Card acceptance: a loopback listener through the real daemon accepts a
/// real TCP connection from the test and echoes; listen and accept are
/// ledger events; suspend releases the listener (a fresh connect is
/// refused) and closes are on the record.
#[tokio::test]
async fn a_fixture_listens_on_loopback_echoes_and_suspend_releases_the_listener() {
    let home = home("echo-net");
    let port = free_port();
    let (paths, _) = paths(
        &home,
        serde_json::json!([net_grant(port, port), "jinn:clock"]),
        &format!("net-echo:{port}"),
    );
    let daemon = booted(paths.clone()).await;
    let fiber = daemon.entry_fiber("worker");

    // The peer runs off the runtime thread: a blocking read on the test's
    // own thread would starve the daemon's alarm, guest, and reactor.
    let echoed = tokio::task::spawn_blocking(move || {
        let mut stream = connect(port).unwrap_or_else(|| panic!("the listener accepts"));
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap_or_else(|error| panic!("{error}"));
        stream
            .write_all(b"ping")
            .unwrap_or_else(|error| panic!("write: {error}"));
        let mut echoed = [0u8; 4];
        stream
            .read_exact(&mut echoed)
            .unwrap_or_else(|error| panic!("the guest echoes: {error}"));
        echoed
    })
    .await
    .unwrap_or_else(|error| panic!("peer: {error}"));
    assert_eq!(&echoed, b"ping");

    let records = events(&daemon).await;
    assert!(
        records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::NetListening { port: bound, .. } if *bound == port
        ) && record.fiber == fiber),
        "the listen is a ledger event with attribution: {records:?}"
    );
    assert!(
        records
            .iter()
            .any(|record| matches!(&record.kind, LedgerEventKind::NetAccepted { .. })),
        "the accept is a ledger event: {records:?}"
    );

    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
    assert!(
        TcpStream::connect(("127.0.0.1", port)).is_err(),
        "suspend released the listener"
    );
    let records = events(&daemon).await;
    assert!(
        records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::EffectWithdrawn { label, clean: true }
                if label.starts_with("jinn:net listen")
        )),
        "the listener's release is ledgered: {records:?}"
    );
    assert!(
        records
            .iter()
            .any(|record| matches!(&record.kind, LedgerEventKind::NetClosed { .. })),
        "the close is a ledger event: {records:?}"
    );
}

/// Hostile probes: a port outside the granted range, a non-loopback
/// bind, and a bind with no grant each refuse on the record — the fixture
/// asserts the typed refusal, so a clean activation is the proof.
#[tokio::test]
async fn out_of_range_non_loopback_and_ungranted_binds_refuse() {
    let port = free_port();
    let probes = [
        (
            "range",
            net_grant(port + 1, port + 1),
            format!("127.0.0.1:{port}"),
        ),
        ("loopback", net_grant(port, port), format!("0.0.0.0:{port}")),
        (
            "ungranted",
            serde_json::json!("jinn:clock"),
            format!("127.0.0.1:{port}"),
        ),
    ];
    for (name, grant, addr) in probes {
        let home = home(&format!("refuse-{name}"));
        let (paths, _) = paths(
            &home,
            serde_json::json!([grant]),
            &format!("net-refused:{addr}"),
        );
        let daemon = booted(paths.clone()).await;
        let records = events(&daemon).await;
        assert_eq!(
            grant_refusals(&records, "jinn:net"),
            1,
            "{name}: refused on the record: {records:?}"
        );
        assert!(
            TcpStream::connect(("127.0.0.1", port)).is_err(),
            "{name}: nothing listens"
        );
        daemon
            .shutdown()
            .await
            .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
    }
}
