//! The `jinn:process` operations behind the provider's broker face: the
//! bounded one-shot `run` (collector.rs owns its stdout) and the handle
//! operations (wit/plugin.wit
//! `interface process`). Split from `hostprocess.rs` by responsibility
//! (R10 file hygiene).

use std::sync::Arc;

use jinnd_api::{ErrorCode, FiberId, KernelError, LedgerEventKind};
use tokio::process::Child;

use super::HostProcess;
use super::child::{self, RUN_CAP, Signal, exit_code};
use super::collector::{Collector, Overflow, RUN_OUTPUT_CAP};
use super::reap::{RUN_REAP_CAP, reap_on_record};
use super::ring::STREAM_CAP;
use crate::broker_state::refusal;
use crate::hostwire::{Reader, TAG_DATA, TAG_TRUNCATED, TAG_WOULD_BLOCK, decode_run, encode_read};
use crate::peer::PeerId;

/// Why the bounded `run` loop stopped.
enum Stop {
    /// The child exited and its stdout reached EOF.
    Done,
    /// The child's `wait` itself failed.
    Failed,
    /// The collected total passed the cap.
    Overflow,
    /// The bound elapsed first.
    Bound,
}

impl HostProcess {
    /// The one-shot: spawn, collect stdout through an OWNED bounded
    /// collector (R9), all inside the bound. Past the cap or the bound the
    /// read end is cut (EPIPE for any descendant holding the pipe) and a
    /// live child is killed AND reaped on the record (Law 2); a torn pipe
    /// or dead collector is a typed error — never defaulted success. The
    /// answer is tagged: data then the bytes, or the truncation alone.
    pub(super) async fn run(&self, caller: PeerId, payload: &[u8]) -> Result<Vec<u8>, KernelError> {
        let (command, args) = decode_run(payload)?;
        let (program, scope) = self.authorize(caller, &command).await?;
        let fiber = self.core.attribution(caller);
        let failed = |what: String| {
            refusal(
                ErrorCode::PluginFailed,
                format!("process run {command:?}: {what}"),
            )
        };
        let mut child = child::command(&program, &args, None, &[], &scope)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|error| failed(error.to_string()))?;
        let handle = self.core.mint();
        self.core.sink.append(
            LedgerEventKind::ProcessSpawned {
                handle,
                command: command.clone(),
                pid: child.id().unwrap_or(0),
            },
            fiber,
        );
        let pipe = child
            .stdout
            .take()
            .ok_or_else(|| failed("no stdout pipe".to_owned()))?;
        let mut collector = Collector::start(pipe);
        let mut exit: Option<std::io::Result<std::process::ExitStatus>> = None;
        let stop = {
            let mut drain = std::pin::pin!(collector.drain());
            let mut deadline = std::pin::pin!(tokio::time::sleep(RUN_CAP));
            let mut output = None;
            loop {
                tokio::select! {
                    status = child.wait(), if exit.is_none() => {
                        if let Ok(status) = &status {
                            let code = exit_code(*status);
                            self.core
                                .sink
                                .append(LedgerEventKind::ProcessExited { handle, code }, fiber);
                        }
                        exit = Some(status);
                    }
                    result = &mut drain, if output.is_none() => output = Some(result),
                    () = &mut deadline => break Stop::Bound,
                }
                match (&exit, &output) {
                    (Some(Err(_)), _) => break Stop::Failed,
                    (_, Some(Err(Overflow))) => break Stop::Overflow,
                    (Some(Ok(_)), Some(Ok(()))) => break Stop::Done,
                    _ => {}
                }
            }
        };
        if matches!(stop, Stop::Done) {
            let mut wire = vec![TAG_DATA];
            wire.extend(collector.finish().await?);
            return Ok(wire);
        }
        collector.cut().await;
        let live = exit.is_none();
        match stop {
            Stop::Done => unreachable!("answered above"),
            Stop::Failed => Err(failed("the wait failed".to_owned())),
            Stop::Overflow => {
                self.core.sink.append(
                    LedgerEventKind::ProcessOutputTruncated {
                        handle,
                        cap: RUN_OUTPUT_CAP as u64,
                    },
                    fiber,
                );
                if live {
                    self.kill_and_reap(child, handle, fiber).await;
                }
                Ok(vec![TAG_TRUNCATED])
            }
            Stop::Bound if live => {
                self.kill_and_reap(child, handle, fiber).await;
                Err(failed("exceeded its bound and was killed".to_owned()))
            }
            Stop::Bound => Err(failed(
                "stdout held open past the bound after the exit (a descendant inherited the pipe; use spawn for long-lived children)"
                    .to_owned(),
            )),
        }
    }

    /// Kills a live child at the bound and reaps it on the record
    /// (reap.rs): `ProcessKilled` now, `ProcessExited` when it lands.
    async fn kill_and_reap(&self, mut child: Child, handle: u64, fiber: Option<FiberId>) {
        self.core.sink.append(
            LedgerEventKind::ProcessKilled {
                handle,
                signal: "kill".to_owned(),
            },
            fiber,
        );
        let _ = child.start_kill();
        let reap = async move { child.wait().await.map_or(-1, exit_code) };
        reap_on_record(
            Arc::clone(&self.core.sink),
            handle,
            fiber,
            reap,
            RUN_REAP_CAP,
        )
        .await;
    }

    /// One operation on a caller-owned handle: 8-byte LE handle first,
    /// then the operation's own fields (wire per the world).
    pub(super) async fn handle_op(
        &self,
        caller: PeerId,
        operation: &str,
        payload: &[u8],
    ) -> Result<Vec<u8>, KernelError> {
        let mut reader = Reader::new(payload, "process handle");
        let handle = reader.u64()?;
        let row = self.core.row(caller, handle)?;
        match operation {
            "write-stdin" => {
                let count = row.write_stdin(reader.rest())?;
                Ok(u32::try_from(count)
                    .unwrap_or(u32::MAX)
                    .to_le_bytes()
                    .to_vec())
            }
            "close-stdin" => {
                row.close_stdin();
                Ok(Vec::new())
            }
            "read" => {
                let which = reader.u8()?;
                let max = (reader.u32()? as usize).min(STREAM_CAP);
                let stream = if which == 0 { &row.stdout } else { &row.stderr };
                let (data, eof) = stream.take(max);
                Ok(encode_read((!data.is_empty()).then_some(data), eof))
            }
            "wait" => Ok(match row.wait(reader.u64()?).await {
                Some(code) => {
                    let mut wire = vec![TAG_DATA];
                    wire.extend(code.to_le_bytes());
                    wire
                }
                None => vec![TAG_WOULD_BLOCK],
            }),
            "kill" => {
                let signal = Signal::from_wire(reader.u8()?).ok_or_else(|| {
                    refusal(ErrorCode::PluginFailed, "unknown process signal".to_owned())
                })?;
                if row.signal(signal) {
                    self.core.sink.append(
                        LedgerEventKind::ProcessKilled {
                            handle,
                            signal: signal.name().to_owned(),
                        },
                        row.fiber,
                    );
                }
                Ok(Vec::new())
            }
            other => Err(refusal(
                ErrorCode::PluginFailed,
                format!("unknown process operation {other:?}"),
            )),
        }
    }
}
