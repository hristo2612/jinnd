//! One spawned child behind the `jinn:process` provider (M2-K6; R1): a
//! supervisor task owns the `Child` and is the only reaper; one pump task
//! per output stream moves bytes into a bounded ring, one feeder task
//! drains the stdin ring into the pipe; the guest's calls touch only
//! clones — rings, an exit watch, a signal channel — so no lock is ever
//! held across an await, no call touches a pipe, and no call blocks past
//! its bound.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use jinnd_api::{ErrorCode, FiberId, KernelError, LedgerEventKind};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{Notify, mpsc, watch};

use super::HostProcess;
use super::ring::{Ring, STREAM_CAP};
use crate::broker_state::refusal;
use crate::grants::{EnvPolicy, ProcessScope};
use crate::peer::PeerId;

/// The `wait` cap (R1: no host call blocks across the guest deadline).
pub(super) const WAIT_CAP: Duration = Duration::from_millis(1000);
/// The one-shot `run` bound; the child is killed at the bound.
pub(super) const RUN_CAP: Duration = Duration::from_secs(4);
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

/// One stream ring and the signal that resumes the task waiting on it:
/// for an output stream a take wakes the stalled pump; for stdin an offer
/// wakes the idle feeder.
#[derive(Clone)]
pub(super) struct Stream {
    ring: Arc<Ring>,
    space: Arc<Notify>,
}

impl Stream {
    fn new() -> Self {
        Self {
            ring: Arc::new(Ring::new(STREAM_CAP)),
            space: Arc::new(Notify::new()),
        }
    }

    /// One non-blocking read: `(bytes, eof)`; a take that made room wakes
    /// the pump.
    pub(super) fn take(&self, max: usize) -> (Vec<u8>, bool) {
        let (data, eof) = self.ring.take(max);
        if !data.is_empty() {
            self.space.notify_one();
        }
        (data, eof)
    }

    /// Offers bytes to stdin without blocking; answers the accepted count
    /// (up to the ring's free space), waking the feeder.
    ///
    /// # Errors
    ///
    /// A closed stdin.
    fn offer(&self, bytes: &[u8]) -> Result<usize, KernelError> {
        if self.ring.is_closed() {
            return Err(refusal(
                ErrorCode::PluginFailed,
                "process stdin is closed".to_owned(),
            ));
        }
        let accepted = self.ring.offer(bytes);
        self.space.notify_one();
        Ok(accepted)
    }

    /// Ends the stream and frees a stalled pump or feeder.
    fn release(&self) {
        self.ring.close();
        self.space.notify_one();
    }

    #[cfg(test)]
    pub(super) fn buffered(&self) -> usize {
        self.ring.len()
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

/// Takes the pipes off a freshly spawned child, starts its pumps and its
/// supervisor, and answers the row the table holds.
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
        None => stdin.ring.close(),
    }
    let stdout = Stream::new();
    let stderr = Stream::new();
    match child.stdout.take() {
        Some(pipe) => drop(tokio::spawn(pump(pipe, stdout.clone()))),
        None => stdout.ring.close(),
    }
    match child.stderr.take() {
        Some(pipe) => drop(tokio::spawn(pump(pipe, stderr.clone()))),
        None => stderr.ring.close(),
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

/// Moves one pipe into its ring; stalls on a full ring until a take makes
/// room (backpressure, R9); closes the ring at the pipe's end.
async fn pump(mut pipe: impl AsyncRead + Unpin, stream: Stream) {
    let mut chunk = vec![0u8; 8192];
    loop {
        let read = match pipe.read(&mut chunk).await {
            Ok(0) | Err(_) => break,
            Ok(read) => read,
        };
        let mut offset = 0;
        while offset < read {
            offset += stream.ring.offer(&chunk[offset..read]);
            if offset < read {
                if stream.ring.is_closed() {
                    return;
                }
                stream.space.notified().await;
            }
        }
    }
    stream.ring.close();
}

/// Drains the stdin ring into the pipe; idles until an offer; the ring's
/// close (the guest's `close-stdin`, or the release) is the child's EOF.
async fn feed(mut pipe: ChildStdin, stream: Stream) {
    loop {
        let (chunk, eof) = stream.ring.take(8192);
        if !chunk.is_empty() {
            if pipe.write_all(&chunk).await.is_err() {
                stream.ring.close();
                return;
            }
        } else if eof {
            let _ = pipe.shutdown().await;
            return;
        } else {
            stream.space.notified().await;
        }
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
        let code = *exit.borrow();
        code
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
