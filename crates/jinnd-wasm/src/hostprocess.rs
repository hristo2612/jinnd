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

use std::path::{Path, PathBuf};
use std::sync::Arc;

use jinnd_api::{ErrorCode, KernelError, KernelFuture, LedgerEventKind, RefusalReason};

use crate::broker::Broker;
use crate::broker_state::refusal;
use crate::grants::{GrantScope, ProcessScope};
use crate::hostbase::ProviderCore;
use crate::hostcaps::PROCESS_CONTRACT;
use crate::hostwire::{decode_spawn, encode_handle};
use crate::peer::{LedgerSink, Peer, PeerId};

mod child;
mod collector;
#[cfg(all(test, not(feature = "loom")))]
mod collector_tests;
mod ops;
mod reap;
#[cfg(all(test, not(feature = "loom")))]
mod reap_tests;
mod ring;
#[cfg(all(test, feature = "loom"))]
mod ring_tests;
#[cfg(all(test, not(feature = "loom")))]
mod run_tests;
mod stream;
#[cfg(all(test, not(feature = "loom")))]
mod tests;

use child::{Row, spawn_row};

/// The `jinn:process` provider: the table of live children.
pub struct HostProcess {
    core: ProviderCore<Row>,
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
            core: ProviderCore::new(PROCESS_CONTRACT, sink),
        })
    }

    /// Registers this provider as a broker peer holding and providing the
    /// `jinn:process` contract (providing is authority).
    ///
    /// # Errors
    ///
    /// The broker's refusal of the provision.
    pub fn register(self: &Arc<Self>, broker: &Arc<Broker>) -> Result<(), KernelError> {
        self.core.attach(broker);
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
        self.core.len()
    }

    /// The single admission point per call: the caller's policy, then the
    /// resolved executable under it — refused on the record otherwise.
    async fn authorize(
        &self,
        caller: PeerId,
        command: &str,
    ) -> Result<(PathBuf, ProcessScope), KernelError> {
        let Some(GrantScope::Process(scope)) = self.core.policy(caller) else {
            return Err(self.core.refuse(
                caller,
                RefusalReason::NotGranted,
                "process caller holds no policy".to_owned(),
            ));
        };
        let (command, prefixes) = (command.to_owned(), scope.exec.clone());
        let verdict = tokio::task::spawn_blocking(move || allowed(&command, &prefixes))
            .await
            .unwrap_or_else(|_| Err("process resolution task failed".to_owned()));
        match verdict {
            Ok(program) => Ok((program, scope)),
            Err(message) => Err(self
                .core
                .refuse(caller, RefusalReason::ScopeMismatch, message)),
        }
    }

    async fn spawn(
        self: &Arc<Self>,
        caller: PeerId,
        payload: &[u8],
    ) -> Result<Vec<u8>, KernelError> {
        let (command, args, cwd, env) = decode_spawn(payload)?;
        let (program, scope) = self.authorize(caller, &command).await?;
        let fiber = self.core.attribution(caller);
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
        let handle = self.core.mint();
        self.core
            .insert(handle, spawn_row(self, handle, caller, fiber, child));
        self.core.sink.append(
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

    /// The registration's release (suspend or dispose): the child is
    /// killed on the record and reaped; an already-released or exited
    /// child is a clean no-op.
    async fn withdraw(&self, handle: u64) -> Result<(), KernelError> {
        let Some(row) = self.core.remove(handle) else {
            return Ok(());
        };
        if row.exited().is_none() {
            self.core.sink.append(
                LedgerEventKind::ProcessKilled {
                    handle,
                    signal: "kill".to_owned(),
                },
                row.fiber,
            );
        }
        row.release().await
    }

    #[cfg(test)]
    fn buffered(&self, caller: PeerId, handle: u64) -> usize {
        self.core
            .row(caller, handle)
            .map_or(0, |row| row.stdout.buffered())
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
