//! The `jinn:process` operations behind the provider's broker face: the
//! bounded one-shot `run` and the handle operations (wit/plugin.wit
//! `interface process`). Split from `hostprocess.rs` by responsibility
//! (R10 file hygiene).

use std::sync::Arc;

use jinnd_api::{ErrorCode, KernelError, LedgerEventKind};
use tokio::io::AsyncReadExt;

use super::HostProcess;
use super::child::{self, RUN_CAP, Signal, exit_code};
use super::reap::{RUN_REAP_CAP, reap_on_record};
use super::ring::STREAM_CAP;
use crate::broker_state::refusal;
use crate::hostwire::{Reader, TAG_DATA, TAG_WOULD_BLOCK, decode_run, encode_read};
use crate::peer::PeerId;

impl HostProcess {
    /// The one-shot: spawn, collect stdout, bounded; at the bound the child
    /// is killed AND reaped on the record (Law 2) and the call refuses.
    pub(super) async fn run(&self, caller: PeerId, payload: &[u8]) -> Result<Vec<u8>, KernelError> {
        let (command, args) = decode_run(payload)?;
        let (program, scope) = self.authorize(caller, &command).await?;
        let fiber = self.core.attribution(caller);
        let mut child = child::command(&program, &args, None, &[], &scope)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .map_err(|error| {
                refusal(
                    ErrorCode::PluginFailed,
                    format!("process run {command:?}: {error}"),
                )
            })?;
        let handle = self.core.mint();
        self.core.sink.append(
            LedgerEventKind::ProcessSpawned {
                handle,
                command: command.clone(),
                pid: child.id().unwrap_or(0),
            },
            fiber,
        );
        let collector = child.stdout.take().map(|mut pipe| {
            tokio::spawn(async move {
                let mut stdout = Vec::new();
                let _ = pipe.read_to_end(&mut stdout).await;
                stdout
            })
        });
        match tokio::time::timeout(RUN_CAP, child.wait()).await {
            Ok(Ok(status)) => {
                let code = exit_code(status);
                self.core
                    .sink
                    .append(LedgerEventKind::ProcessExited { handle, code }, fiber);
                Ok(match collector {
                    Some(task) => task.await.unwrap_or_default(),
                    None => Vec::new(),
                })
            }
            Ok(Err(error)) => Err(refusal(
                ErrorCode::PluginFailed,
                format!("process run {command:?}: {error}"),
            )),
            // Killed at the bound, then reaped on the record (reap.rs).
            Err(_) => {
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
                Err(refusal(
                    ErrorCode::PluginFailed,
                    format!("process run {command:?} exceeded its bound and was killed"),
                ))
            }
        }
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
