//! One spawned child behind the `jinn:process` provider (M2-K6; R1): a
//! supervisor task owns the `Child` and is the only reaper; the guest's
//! calls touch only clones — streams, an exit watch, a signal channel — so
//! no lock is ever held across an await and no call blocks past its bound.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use jinnd_api::{ErrorCode, FiberId, KernelError, LedgerEventKind};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, watch};

use super::HostProcess;
use super::stream::{Stream, feed, pump};
use crate::broker_state::refusal;
use crate::grants::{EnvPolicy, ProcessScope};
use crate::hostbase::Owned;
use crate::peer::PeerId;

/// The `wait` cap (R1: no host call blocks across the guest deadline).
pub(super) const WAIT_CAP: Duration = Duration::from_millis(1000);
/// The one-shot `run` bound; the child is killed at the bound (a short
/// test-build bound keeps the runaway pin on real time).
pub(super) const RUN_CAP: Duration = Duration::from_millis(if cfg!(test) { 250 } else { 4000 });
/// How long a release waits for the SIGKILLed child to be reaped.
const REAP_CAP: Duration = Duration::from_secs(3);

/// The signals a guest may deliver (contract `signal`).
#[derive(Clone, Copy, Debug)]
pub(super) enum Signal {
    Interrupt,
    Terminate,
    Kill,
}

impl Signal {
    pub(super) fn from_wire(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::Interrupt),
            1 => Some(Self::Terminate),
            2 => Some(Self::Kill),
            _ => None,
        }
    }

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Interrupt => "interrupt",
            Self::Terminate => "terminate",
            Self::Kill => "kill",
        }
    }
}

/// The guest-facing face of one child: every field is a cheap clone of a
/// shared piece, so a call takes the table lock only to copy the row out.
#[derive(Clone)]
pub(super) struct Row {
    pub(super) owner: PeerId,
    pub(super) fiber: Option<FiberId>,
    stdin: Stream,
    pub(super) stdout: Stream,
    pub(super) stderr: Stream,
    control: mpsc::UnboundedSender<Signal>,
    exit: watch::Receiver<Option<i32>>,
}

impl Owned for Row {
    fn owner(&self) -> PeerId {
        self.owner
    }
}

/// The command under the grant's env policy (bundle README): the child's
/// environment is exactly the guest's explicit pairs plus, under an
/// allowlist, the named daemon variables — never the daemon's whole
/// environment. `kill_on_drop` is the belt: a dropped child dies.
pub(super) fn command(
    program: &Path,
    args: &[String],
    cwd: Option<&str>,
    env: &[(String, String)],
    scope: &ProcessScope,
) -> Command {
    let mut command = Command::new(program);
    command.args(args).env_clear().kill_on_drop(true);
    if let EnvPolicy::Allow(names) = &scope.env {
        for name in names {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
    }
    for (name, value) in env {
        command.env(name, value);
    }
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    command
}

/// Takes the pipes off a freshly spawned child, starts its pumps, feeder,
/// and supervisor, and answers the row the table holds.
pub(super) fn spawn_row(
    provider: &Arc<HostProcess>,
    handle: u64,
    owner: PeerId,
    fiber: Option<FiberId>,
    mut child: Child,
) -> Row {
    let stdin = Stream::new();
    match child.stdin.take() {
        Some(pipe) => drop(tokio::spawn(feed(pipe, stdin.clone()))),
        None => stdin.close(),
    }
    let stdout = Stream::new();
    match child.stdout.take() {
        Some(pipe) => drop(tokio::spawn(pump(pipe, stdout.clone()))),
        None => stdout.close(),
    }
    let stderr = Stream::new();
    match child.stderr.take() {
        Some(pipe) => drop(tokio::spawn(pump(pipe, stderr.clone()))),
        None => stderr.close(),
    }
    let (control, control_rx) = mpsc::unbounded_channel();
    let (exit_tx, exit) = watch::channel(None);
    tokio::spawn(supervise(
        Arc::clone(provider),
        handle,
        child,
        Some(control_rx),
        exit_tx,
        fiber,
    ));
    Row {
        owner,
        fiber,
        stdin,
        stdout,
        stderr,
        control,
        exit,
    }
}

/// The exit status as the contract answers it: the code, or the negated
/// signal number for a signal termination.
pub(super) fn exit_code(status: std::process::ExitStatus) -> i32 {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        if let Some(signal) = status.signal() {
            return -signal;
        }
    }
    status.code().unwrap_or(-1)
}

/// Delivers one signal to a child this supervisor has not yet reaped (its
/// pid is therefore still the child's — no reuse race).
fn deliver(child: &mut Child, signal: Signal) {
    #[cfg(unix)]
    {
        let number = match signal {
            Signal::Interrupt => nix::sys::signal::Signal::SIGINT,
            Signal::Terminate => nix::sys::signal::Signal::SIGTERM,
            Signal::Kill => nix::sys::signal::Signal::SIGKILL,
        };
        if let Some(pid) = child.id().and_then(|pid| i32::try_from(pid).ok()) {
            let _ = nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), number);
            return;
        }
    }
    let _ = child.start_kill();
}

/// The one owner of the child: reaps it, delivering signals meanwhile;
/// the exit is a ledger event (Law 2) and the watch's final value.
async fn supervise(
    provider: Arc<HostProcess>,
    handle: u64,
    mut child: Child,
    mut control: Option<mpsc::UnboundedReceiver<Signal>>,
    exit: watch::Sender<Option<i32>>,
    fiber: Option<FiberId>,
) {
    let code = loop {
        let signal = async {
            match control.as_mut() {
                Some(receiver) => receiver.recv().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            status = child.wait() => break status.map_or(-1, exit_code),
            signal = signal => match signal {
                Some(signal) => deliver(&mut child, signal),
                None => control = None,
            },
        }
    };
    provider
        .core
        .sink
        .append(LedgerEventKind::ProcessExited { handle, code }, fiber);
    let _ = exit.send(Some(code));
}

impl Row {
    /// Offers bytes to stdin without blocking; answers the accepted count.
    ///
    /// # Errors
    ///
    /// A closed stdin.
    pub(super) fn write_stdin(&self, bytes: &[u8]) -> Result<usize, KernelError> {
        self.stdin.offer(bytes)
    }

    /// The child's stdin EOF (once the feeder drains what was accepted).
    pub(super) fn close_stdin(&self) {
        self.stdin.release();
    }

    pub(super) fn exited(&self) -> Option<i32> {
        *self.exit.borrow()
    }

    /// Waits up to `timeout_ms`, capped, for the exit.
    pub(super) async fn wait(&self, timeout_ms: u64) -> Option<i32> {
        let mut exit = self.exit.clone();
        let cap = Duration::from_millis(timeout_ms).min(WAIT_CAP);
        let _ = tokio::time::timeout(cap, exit.wait_for(|code| code.is_some())).await;
        *exit.borrow()
    }

    /// Delivers a signal; `false` when the child already exited.
    pub(super) fn signal(&self, signal: Signal) -> bool {
        self.exited().is_none() && self.control.send(signal).is_ok()
    }

    /// The registration's release (M2-K6 lifecycle class): streams end,
    /// stdin closes, the child is SIGKILLed and REAPED before this
    /// returns — no zombie survives a dispose or a suspend.
    ///
    /// # Errors
    ///
    /// A child not reaped within the cap (contained, recorded unclean).
    pub(super) async fn release(self) -> Result<(), KernelError> {
        self.stdout.release();
        self.stderr.release();
        self.close_stdin();
        if self.exited().is_some() {
            return Ok(());
        }
        let _ = self.control.send(Signal::Kill);
        let mut exit = self.exit.clone();
        tokio::time::timeout(REAP_CAP, exit.wait_for(|code| code.is_some()))
            .await
            .map(|_| ())
            .map_err(|_| {
                refusal(
                    ErrorCode::EffectFailed,
                    "process release: the child was not reaped after SIGKILL".to_owned(),
                )
            })
    }
}
