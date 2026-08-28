//! The `jinn:process` operations behind the provider's broker face: the
//! bounded one-shot `run` and the handle operations (wit/plugin.wit
//! `interface process`). Split from `hostprocess.rs` by responsibility
//! (R10 file hygiene).

use jinnd_api::{ErrorCode, KernelError, LedgerEventKind};

use super::HostProcess;
use super::child::{self, RUN_CAP, Signal, exit_code};
use super::ring::STREAM_CAP;
use crate::broker_state::refusal;
use crate::hostwire::{Reader, TAG_DATA, TAG_WOULD_BLOCK, decode_run, encode_read};
use crate::peer::PeerId;

impl HostProcess {
    /// The one-shot: spawn, collect stdout, bounded; the child is killed at
    /// the bound (on the record) and the call refuses.
    pub(super) async fn run(&self, caller: PeerId, payload: &[u8]) -> Result<Vec<u8>, KernelError> {
        let (command, args) = decode_run(payload)?;
        let (program, scope) = self.authorize(caller, &command).await?;
        let fiber = self.core.attribution(caller);
        let child = child::command(&program, &args, None, &[], &scope)
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
        match tokio::time::timeout(RUN_CAP, child.wait_with_output()).await {
            Ok(Ok(output)) => {
                let code = exit_code(output.status);
                self.core
                    .sink
                    .append(LedgerEventKind::ProcessExited { handle, code }, fiber);
                Ok(output.stdout)
            }
            Ok(Err(error)) => Err(refusal(
                ErrorCode::PluginFailed,
                format!("process run {command:?}: {error}"),
            )),
            // The dropped child is killed (kill_on_drop) and reaped by the
            // runtime's orphan reaper; the kill is on the record.
            Err(_) => {
                self.core.sink.append(
                    LedgerEventKind::ProcessKilled {
                        handle,
                        signal: "kill".to_owned(),
                    },
                    fiber,
                );
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
