//! The base `jinn:process` host provider (M2-K6; R7): a native peer behind
//! the SAME broker choke point every guest crosses — grant check → ledger
//! append → dispatch — answering the contract bundle
//! `contracts/jinn-process`. Authority is the caller's typed
//! `process-policy` (`grants::ProcessScope`), enforced per call on the
//! FULLY RESOLVED executable (post-symlink; K3 doctrine) — a bare grant
//! allows nothing. A spawned child is a KERNEL REGISTRATION: the caller's
//! seat journals the handle and releases it through [`Peer::withdraw`] on
//! suspend and dispose alike — kill, reap, ledger — never retained. Every
//! spawn, exit, and kill is a ledger event with the caller's attribution
//! (Law 2); stream bytes are data plane and are not.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use jinnd_api::{ErrorCode, FiberId, KernelError, KernelFuture, LedgerEventKind};

use crate::broker::Broker;
use crate::broker_state::refusal;
use crate::grants::{GrantScope, ProcessScope};
use crate::hostcaps::PROCESS_CONTRACT;
use crate::hostwire::{
    Reader, TAG_DATA, TAG_WOULD_BLOCK, decode_run, decode_spawn, encode_handle, encode_read,
};
use crate::lane::lock;
use crate::peer::{LedgerSink, Peer, PeerId};

mod child;
mod ring;
#[cfg(all(test, feature = "loom"))]
mod ring_tests;
#[cfg(all(test, not(feature = "loom")))]
mod tests;

use child::{RUN_CAP, Row, Signal, exit_code, spawn_row};
use ring::STREAM_CAP;

/// The `jinn:process` provider: the table of live children.
pub struct HostProcess {
    sink: Arc<dyn LedgerSink>,
    broker: OnceLock<Weak<Broker>>,
    table: Mutex<HashMap<u64, Row>>,
    next: AtomicU64,
}

/// The resolved executable, or why it is refused: absolute, resolvable,
/// and under one resolved allowlisted prefix. Blocking (canonicalize).
fn allowed(command: &str, prefixes: &[String]) -> Result<PathBuf, String> {
    if !Path::new(command).is_absolute() {
        return Err(format!("process command must be absolute: {command:?}"));
    }
    let resolved = std::fs::canonicalize(command)
        .map_err(|error| format!("process command unresolvable, refused: {command:?}: {error}"))?;
    let permitted = prefixes.iter().any(|prefix| {
        std::fs::canonicalize(prefix).is_ok_and(|prefix| resolved.starts_with(prefix))
    });
    if permitted {
        Ok(resolved)
    } else {
        Err(format!(
            "process command outside the caller's exec allowlist: {command:?}"
        ))
    }
}

impl HostProcess {
    /// A provider appending its Law-2 events to `sink`.
    #[must_use]
    pub fn new(sink: Arc<dyn LedgerSink>) -> Arc<Self> {
        Arc::new(Self {
            sink,
            broker: OnceLock::new(),
            table: Mutex::new(HashMap::new()),
            next: AtomicU64::new(0),
        })
    }

    /// Registers this provider as a broker peer holding and providing the
    /// `jinn:process` contract (providing is authority). The broker is
    /// kept weakly for caller attribution and policies (R4).
    ///
    /// # Errors
    ///
    /// The broker's refusal of the provision.
    pub fn register(self: &Arc<Self>, broker: &Arc<Broker>) -> Result<(), KernelError> {
        let _ = self.broker.set(Arc::downgrade(broker));
        let peer = broker.register_peer(None);
        broker.grant(peer, PROCESS_CONTRACT);
        broker.provide(
            peer,
            PROCESS_CONTRACT,
            Arc::new(ProcessPeer(Arc::clone(self))),
        )
    }

    /// The children this provider still holds (released ones are gone).
    #[must_use]
    pub fn live(&self) -> usize {
        lock(&self.table).len()
    }

    fn broker(&self) -> Option<Arc<Broker>> {
        self.broker.get().and_then(Weak::upgrade)
    }

    fn attribution(&self, caller: PeerId) -> Option<FiberId> {
        self.broker().and_then(|broker| broker.attribution(caller))
    }

    /// One ledgered grant refusal with the caller's attribution (Law 2).
    fn refuse(&self, caller: PeerId, message: String) -> KernelError {
        self.sink.append(
            LedgerEventKind::GrantRefused {
                contract: PROCESS_CONTRACT.to_owned(),
            },
            self.attribution(caller),
        );
        refusal(ErrorCode::EffectFailed, message)
    }

    /// The single admission point per call: the caller's policy, then the
    /// resolved executable under it.
    async fn authorize(
        &self,
        caller: PeerId,
        command: &str,
    ) -> Result<(PathBuf, ProcessScope), KernelError> {
        let policy = self
            .broker()
            .and_then(|broker| broker.policy(caller, PROCESS_CONTRACT));
        let Some(GrantScope::Process(scope)) = policy else {
            return Err(self.refuse(caller, "process caller holds no policy".to_owned()));
        };
        let (command, prefixes) = (command.to_owned(), scope.exec.clone());
        let verdict = tokio::task::spawn_blocking(move || allowed(&command, &prefixes))
            .await
            .unwrap_or_else(|_| Err("process resolution task failed".to_owned()));
        match verdict {
            Ok(program) => Ok((program, scope)),
            Err(message) => Err(self.refuse(caller, message)),
        }
    }

    fn mint(&self) -> u64 {
        self.next.fetch_add(1, Ordering::SeqCst) + 1
    }

    async fn spawn(
        self: &Arc<Self>,
        caller: PeerId,
        payload: &[u8],
    ) -> Result<Vec<u8>, KernelError> {
        let (command, args, cwd, env) = decode_spawn(payload)?;
        let (program, scope) = self.authorize(caller, &command).await?;
        let fiber = self.attribution(caller);
        let child = child::command(&program, &args, cwd.as_deref(), &env, &scope)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|error| {
                refusal(
                    ErrorCode::PluginFailed,
                    format!("process spawn {command:?}: {error}"),
                )
            })?;
        let pid = child.id().unwrap_or(0);
        let handle = self.mint();
        let row = spawn_row(self, handle, caller, fiber, child);
        lock(&self.table).insert(handle, row);
        self.sink.append(
            LedgerEventKind::ProcessSpawned {
                handle,
                command: command.clone(),
                pid,
            },
            fiber,
        );
        tracing::info!(handle, pid, command = %command, "process spawned");
        Ok(encode_handle(handle))
    }

    /// The one-shot: spawn, collect stdout, bounded; the child is killed at
    /// the bound (on the record) and the call refuses.
    async fn run(&self, caller: PeerId, payload: &[u8]) -> Result<Vec<u8>, KernelError> {
        let (command, args) = decode_run(payload)?;
        let (program, scope) = self.authorize(caller, &command).await?;
        let fiber = self.attribution(caller);
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
        let handle = self.mint();
        self.sink.append(
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
                self.sink
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
                self.sink.append(
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

    /// The caller's own row, copied out under the lock (R4: a handle is
    /// valid only for the peer that minted it).
    fn row(&self, caller: PeerId, handle: u64) -> Result<Row, KernelError> {
        match lock(&self.table).get(&handle) {
            Some(row) if row.owner == caller => Ok(row.clone()),
            Some(_) => Err(self.refuse(
                caller,
                format!("process handle {handle} is not the caller's"),
            )),
            None => Err(refusal(
                ErrorCode::NotFound,
                format!("unknown process handle {handle}"),
            )),
        }
    }

    async fn handle_op(
        &self,
        caller: PeerId,
        operation: &str,
        payload: &[u8],
    ) -> Result<Vec<u8>, KernelError> {
        let mut reader = Reader::new(payload, "process handle");
        let handle = reader.u64()?;
        let row = self.row(caller, handle)?;
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
                    self.sink.append(
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

    /// The registration's release (suspend or dispose): the child is
    /// killed on the record and reaped; an already-released or exited
    /// child is a clean no-op.
    async fn withdraw(&self, handle: u64) -> Result<(), KernelError> {
        let Some(row) = lock(&self.table).remove(&handle) else {
            return Ok(());
        };
        if row.exited().is_none() {
            self.sink.append(
                LedgerEventKind::ProcessKilled {
                    handle,
                    signal: "kill".to_owned(),
                },
                row.fiber,
            );
        }
        row.release().await
    }
}

/// The provider's broker face.
struct ProcessPeer(Arc<HostProcess>);

impl Peer for ProcessPeer {
    fn call(
        &self,
        caller: PeerId,
        _contract: &str,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let provider = Arc::clone(&self.0);
        let operation = operation.to_owned();
        Box::pin(async move {
            match operation.as_str() {
                "run" => provider.run(caller, &payload).await,
                "spawn" => provider.spawn(caller, &payload).await,
                other => provider.handle_op(caller, other, &payload).await,
            }
        })
    }

    fn withdraw(&self, effect: u64) -> KernelFuture<'static, ()> {
        let provider = Arc::clone(&self.0);
        Box::pin(async move { provider.withdraw(effect).await })
    }
}
