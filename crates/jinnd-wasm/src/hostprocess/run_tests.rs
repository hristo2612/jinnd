//! Pins for the bounded one-shot `run` (M2-K6 round 3; R9, R11): output
//! past the cap is a typed truncation — cut, killed, reaped on the record;
//! a descendant holding the pipe past the bound is cut off (EPIPE) and
//! the call refuses inside its deadline; an exited child whose descendant
//! merely holds stdout open refuses honestly, without a kill.

use std::time::{Duration, Instant};

use jinnd_api::{ErrorCode, KernelError, LedgerEventKind};

use super::child::RUN_CAP;
use super::collector::RUN_OUTPUT_CAP;
use super::tests::{Rig, rig};
use crate::hostcaps::PROCESS_CONTRACT;
use crate::hostwire::put_segment;

fn run_wire(command: &str, args: &[&str]) -> Vec<u8> {
    let mut wire = Vec::new();
    put_segment(&mut wire, command.as_bytes());
    for arg in args {
        put_segment(&mut wire, arg.as_bytes());
    }
    wire
}

async fn run(rig: &Rig, command: &str, args: &[&str]) -> Result<Vec<u8>, KernelError> {
    rig.broker
        .dispatch(rig.guest, PROCESS_CONTRACT, "run", run_wire(command, args))
        .await
}

/// The bound every `run` answers within: the cap, the reap bound, slack.
const ANSWER_WITHIN: Duration = Duration::from_secs(3);

fn position(kinds: &[(LedgerEventKind, Option<jinnd_api::FiberId>)], probe: impl Fn(&LedgerEventKind) -> bool) -> Option<usize> {
    kinds.iter().position(|(kind, _)| probe(kind))
}

async fn wait_for_exit(rig: &Rig, handle: u64) {
    let deadline = Instant::now() + ANSWER_WITHIN;
    loop {
        let kinds = rig.ledger.kinds();
        if position(&kinds, |kind| {
            matches!(kind, LedgerEventKind::ProcessExited { handle: exited, .. } if *exited == handle)
        })
        .is_some()
        {
            return;
        }
        assert!(Instant::now() < deadline, "the exit never landed: {kinds:?}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[cfg(unix)]
fn alive(pid: i32) -> bool {
    nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok()
}

#[cfg(unix)]
fn pid_file(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("jinnd-run-{tag}-{}.pid", std::process::id()))
}

#[cfg(unix)]
fn pid_in(path: &std::path::Path) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(pid) = std::fs::read_to_string(path)
            .ok()
            .and_then(|text| text.trim().parse().ok())
        {
            let _ = std::fs::remove_file(path);
            return pid;
        }
        assert!(Instant::now() < deadline, "the descendant's pid never landed");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Output past the declared cap is a TYPED truncation, never silent
/// growth (R9): the read end is cut, the child killed and reaped, and the
/// ledger reads truncated → killed → exited, all inside the bound.
#[tokio::test]
async fn output_past_the_cap_is_a_typed_truncation_cut_killed_and_reaped() {
    let rig = rig();
    let started = Instant::now();
    let error = run(&rig, "/usr/bin/yes", &[])
        .await
        .expect_err("a runaway writer never answers success");
    assert!(started.elapsed() < ANSWER_WITHIN, "{:?}", started.elapsed());
    assert_eq!(error.code, ErrorCode::PluginFailed);
    assert!(
        error.message.starts_with("output-truncated"),
        "typed by its class: {}",
        error.message
    );
    let kinds = rig.ledger.kinds();
    let truncated = position(&kinds, |kind| {
        matches!(kind, LedgerEventKind::ProcessOutputTruncated { cap, .. } if *cap == RUN_OUTPUT_CAP as u64)
    })
    .unwrap_or_else(|| panic!("the truncation is on the record: {kinds:?}"));
    let LedgerEventKind::ProcessOutputTruncated { handle, .. } = kinds[truncated].0 else {
        unreachable!()
    };
    wait_for_exit(&rig, handle).await;
    let kinds = rig.ledger.kinds();
    let killed = position(&kinds, |kind| {
        matches!(kind, LedgerEventKind::ProcessKilled { handle: killed, .. } if *killed == handle)
    })
    .unwrap_or_else(|| panic!("the kill is on the record: {kinds:?}"));
    let exited = position(&kinds, |kind| {
        matches!(kind, LedgerEventKind::ProcessExited { handle: exited, code } if *exited == handle && *code < 0)
    })
    .unwrap_or_else(|| panic!("the signal exit is on the record: {kinds:?}"));
    assert!(truncated < killed && killed < exited, "{kinds:?}");
    assert_eq!(rig.provider.live(), 0);
}

/// The verifier's shape: an allowed parent exits at once while a
/// descendant it backgrounded keeps writing into the inherited pipe. The
/// call answers inside its bound with bounded host memory, the host's read
/// end is closed, and the writer dies of EPIPE — no collector outlives the
/// call (R9).
#[cfg(unix)]
#[tokio::test]
async fn a_writing_descendant_is_cut_off_at_the_bound_and_dies_of_epipe() {
    let rig = rig();
    let pid_file = pid_file("writer");
    let script = format!(
        "while true; do echo x; done & echo $! > {}",
        pid_file.display()
    );
    let started = Instant::now();
    let error = run(&rig, "/bin/sh", &["-c", &script])
        .await
        .expect_err("a held-open, written pipe never answers success");
    assert!(
        started.elapsed() < RUN_CAP + Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(error.code, ErrorCode::PluginFailed);
    let pid = pid_in(&pid_file);
    let deadline = Instant::now() + Duration::from_secs(3);
    while alive(pid) {
        assert!(
            Instant::now() < deadline,
            "the writer survived the cut: pid {pid} still alive"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(rig.provider.live(), 0);
}

/// A child that exited cleanly while a silent descendant holds stdout
/// open: the call refuses at the bound, honestly — the exit is on the
/// record, nothing is killed (there is no child to kill), and the answer
/// is never a prefix passed off as the output.
#[cfg(unix)]
#[tokio::test]
async fn an_exited_child_whose_descendant_holds_stdout_refuses_without_a_kill() {
    let rig = rig();
    let pid_file = pid_file("holder");
    let script = format!("sleep 30 & echo $! > {}", pid_file.display());
    let error = run(&rig, "/bin/sh", &["-c", &script])
        .await
        .expect_err("a held-open pipe never answers success");
    let pid = pid_in(&pid_file);
    let _ = nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(pid),
        nix::sys::signal::Signal::SIGKILL,
    );
    assert_eq!(error.code, ErrorCode::PluginFailed);
    assert!(error.message.contains("held open"), "{}", error.message);
    let kinds = rig.ledger.kinds();
    assert!(
        position(&kinds, |kind| matches!(
            kind,
            LedgerEventKind::ProcessExited { code: 0, .. }
        ))
        .is_some(),
        "the clean exit is on the record: {kinds:?}"
    );
    assert!(
        position(&kinds, |kind| matches!(
            kind,
            LedgerEventKind::ProcessKilled { .. } | LedgerEventKind::ProcessOutputTruncated { .. }
        ))
        .is_none(),
        "nothing to kill, nothing truncated: {kinds:?}"
    );
}
