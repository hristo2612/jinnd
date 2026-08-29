//! The base `jinn:net` host provider (M2-K6; R7): a native peer behind the
//! SAME broker choke point every guest crosses, answering the contract
//! bundle `contracts/jinn-net`. Authority is the caller's typed
//! `net-policy` (`grants::NetScope`): `listen` binds LOOPBACK ONLY, at a
//! port inside the granted range, or refuses on the record; a bare grant
//! allows nothing. Listeners and accepted connections are KERNEL
//! REGISTRATIONS released through [`Peer::withdraw`] on suspend and
//! dispose alike (closed, ledgered). Every call is non-blocking (R1):
//! `accept` and `read` answer `would-block`, `write` answers what the
//! socket took. Bytes are data plane and are not ledgered. Readiness
//! (M2-K7, harness #23): every held socket has a wake task that delivers
//! `jinn:net/readable` to the holder when a listener has a pending
//! connection or a connection has bytes/EOF — one wake per readiness
//! transition the guest has not acted on, so a server holds NO alarm;
//! the guest that ignores wakes and polls still works.

use std::net::SocketAddr;
use std::sync::Arc;

use jinnd_api::{ErrorCode, KernelError, KernelFuture, LedgerEventKind, RefusalReason};
use tokio::io::unix::AsyncFd;

use crate::broker::Broker;
use crate::broker_state::refusal;
use crate::grants::GrantScope;
use crate::hostbase::ProviderCore;
use crate::hostcaps::NET_CONTRACT;
use crate::hostwire::encode_handle;
use crate::peer::{LedgerSink, Peer, PeerId};

mod ops;
mod readiness;
mod socket;
#[cfg(all(test, not(feature = "loom")))]
mod tests;
mod wake;
#[cfg(all(test, feature = "loom"))]
mod wake_model;

pub use readiness::READABLE_TOPIC;
use socket::{Socket, Wake};
use wake::WakeTable;

/// The `jinn:net` provider: the table of live sockets and their wake state.
pub struct HostNet {
    core: ProviderCore<Socket>,
    wakes: WakeTable,
}

fn failed(what: &str, error: &std::io::Error) -> KernelError {
    refusal(ErrorCode::PluginFailed, format!("net {what}: {error}"))
}

impl HostNet {
    /// A provider appending its Law-2 events to `sink`.
    #[must_use]
    pub fn new(sink: Arc<dyn LedgerSink>) -> Arc<Self> {
        Arc::new(Self {
            core: ProviderCore::new(NET_CONTRACT, sink),
            wakes: WakeTable::default(),
        })
    }

    /// Registers this provider as a broker peer holding and providing the
    /// `jinn:net` contract (providing is authority).
    ///
    /// # Errors
    ///
    /// The broker's refusal of the provision.
    pub fn register(self: &Arc<Self>, broker: &Arc<Broker>) -> Result<(), KernelError> {
        self.core.attach(broker);
        let peer = broker.register_peer(None);
        broker.grant(peer, NET_CONTRACT);
        broker.provide(peer, NET_CONTRACT, Arc::new(NetPeer(Arc::clone(self))))
    }

    /// The sockets this provider still holds.
    #[must_use]
    pub fn live(&self) -> usize {
        self.core.len()
    }

    /// The single admission point for a bind: loopback, inside the
    /// caller's granted range — else refused on the record.
    fn authorize_bind(&self, caller: PeerId, addr: &str) -> Result<SocketAddr, KernelError> {
        let parsed: SocketAddr = addr.parse().map_err(|_| {
            refusal(
                ErrorCode::InvalidProfile,
                format!("net listen: not an ip:port address: {addr:?}"),
            )
        })?;
        let Some(GrantScope::Net(scope)) = self.core.policy(caller) else {
            return Err(self.core.refuse(
                caller,
                RefusalReason::NotGranted,
                "net caller holds no policy".to_owned(),
            ));
        };
        if !parsed.ip().is_loopback() {
            return Err(self.core.refuse(
                caller,
                RefusalReason::NotLoopback,
                format!("net listen refused: {addr:?} is not loopback (v0.1 binds loopback only)"),
            ));
        }
        if scope.admits_port(parsed.port()) {
            Ok(parsed)
        } else {
            Err(self.core.refuse(
                caller,
                RefusalReason::ScopeMismatch,
                format!(
                    "net listen refused: port {} is outside the granted ranges",
                    parsed.port()
                ),
            ))
        }
    }

    /// Holds `socket` under a fresh handle and arms its wake task (M2-K7):
    /// the task runs on the timer runtime like an alarm's; without one the
    /// socket is still held and served, only unwoken (the guest polls).
    fn hold(self: &Arc<Self>, caller: PeerId, socket: Socket) -> u64 {
        let handle = self.core.mint();
        self.core.insert(handle, socket.clone());
        self.wakes.insert(handle);
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let fiber = socket.fiber();
            let wake = Arc::clone(socket.wake());
            let task = runtime.spawn(readiness::run(
                Arc::clone(self),
                handle,
                socket,
                Arc::clone(&wake),
                caller,
                fiber,
            ));
            // Uncontended: the task cannot release itself, and a release
            // finds the row only after this insert.
            if let Ok(mut slot) = wake.task.try_lock() {
                *slot = Some(task);
            }
        }
        handle
    }

    /// The guest acted on `socket`'s handle: re-arm its wake and kick the
    /// task so a still-pending readiness wakes again (M2-K7).
    fn rearm(&self, handle: u64, socket: &Socket) {
        if self.wakes.rearm(handle) {
            socket.wake().notify.notify_one();
        }
    }

    async fn listen(
        self: &Arc<Self>,
        caller: PeerId,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, KernelError> {
        let addr = String::from_utf8(payload).map_err(|_| {
            refusal(
                ErrorCode::PluginFailed,
                "malformed net listen payload".to_owned(),
            )
        })?;
        let addr = self.authorize_bind(caller, &addr)?;
        // A non-blocking std listener behind tokio readiness: bind is
        // instant on loopback (no resolution, no wait).
        let listener = std::net::TcpListener::bind(addr)
            .and_then(|listener| listener.set_nonblocking(true).map(|()| listener))
            .and_then(AsyncFd::new)
            .map_err(|error| failed("listen", &error))?;
        let fiber = self.core.attribution(caller);
        let handle = self.hold(
            caller,
            Socket::Listener {
                owner: caller,
                fiber,
                listener: Arc::new(listener),
                pending: Arc::default(),
                wake: Arc::new(Wake::default()),
            },
        );
        self.core.sink.append(
            LedgerEventKind::NetListening {
                handle,
                port: addr.port(),
            },
            fiber,
        );
        tracing::info!(handle, %addr, "net listening");
        Ok(encode_handle(handle))
    }

    /// The registration's release (suspend, dispose, or the guest's own
    /// close): the socket drops — closed — on the record; an unknown
    /// handle is a clean no-op.
    async fn withdraw(&self, handle: u64) -> Result<(), KernelError> {
        let Some(socket) = self.core.remove(handle) else {
            return Ok(());
        };
        // The row leaves the wake table BEFORE the socket closes: after
        // this returns no wake of the handle is ever appended (loom-pinned)
        // — and the wake task is gone, so the descriptor closes HERE.
        self.wakes.take(handle);
        let wake = Arc::clone(socket.wake());
        wake.notify.notify_one();
        let task = wake.task.lock().await.take();
        if let Some(task) = task {
            task.abort();
            if !wake.delivering.load(std::sync::atomic::Ordering::SeqCst) {
                let _ = task.await;
            }
        }
        self.core
            .sink
            .append(LedgerEventKind::NetClosed { handle }, socket.fiber());
        drop(socket);
        Ok(())
    }
}

/// The provider's broker face.
struct NetPeer(Arc<HostNet>);

impl Peer for NetPeer {
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
                "listen" => provider.listen(caller, payload).await,
                // Declared, not provided (R10: no HTTP client in the
                // kernel); typed so a caller classifies it, never a hang.
                "request" => Err(refusal(
                    ErrorCode::PluginFailed,
                    "jinn:net request is not provided in v0.1 (no HTTP client in the kernel)"
                        .to_owned(),
                )),
                other => provider.handle_op(caller, other, &payload).await,
            }
        })
    }

    fn withdraw(&self, effect: u64) -> KernelFuture<'static, ()> {
        let provider = Arc::clone(&self.0);
        Box::pin(async move { provider.withdraw(effect).await })
    }
}
