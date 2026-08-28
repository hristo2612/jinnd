//! The base `jinn:net` host provider (M2-K6; R7): a native peer behind the
//! SAME broker choke point every guest crosses, answering the contract
//! bundle `contracts/jinn-net`. Authority is the caller's typed
//! `net-policy` (`grants::NetScope`): `listen` binds LOOPBACK ONLY, at a
//! port inside the granted range, or refuses on the record; a bare grant
//! allows nothing. Listeners and accepted connections are KERNEL
//! REGISTRATIONS released through [`Peer::withdraw`] on suspend and
//! dispose alike (closed, ledgered). Every call is non-blocking (R1):
//! `accept` and `read` answer `would-block`, `write` answers what the
//! socket took. Bytes are data plane and are not ledgered.

use std::net::SocketAddr;
use std::sync::Arc;

use jinnd_api::{ErrorCode, FiberId, KernelError, KernelFuture, LedgerEventKind};
use tokio::net::{TcpListener, TcpStream};

use crate::broker::Broker;
use crate::broker_state::refusal;
use crate::grants::GrantScope;
use crate::hostbase::{Owned, ProviderCore};
use crate::hostcaps::NET_CONTRACT;
use crate::hostwire::encode_handle;
use crate::peer::{LedgerSink, Peer, PeerId};

mod ops;
#[cfg(all(test, not(feature = "loom")))]
mod tests;

/// One held socket: a listener or an accepted connection, owned by the
/// peer that minted it (R4).
#[derive(Clone)]
enum Socket {
    Listener {
        owner: PeerId,
        fiber: Option<FiberId>,
        listener: Arc<TcpListener>,
    },
    Conn {
        owner: PeerId,
        fiber: Option<FiberId>,
        stream: Arc<TcpStream>,
    },
}

impl Socket {
    fn fiber(&self) -> Option<FiberId> {
        match self {
            Self::Listener { fiber, .. } | Self::Conn { fiber, .. } => *fiber,
        }
    }
}

impl Owned for Socket {
    fn owner(&self) -> PeerId {
        match self {
            Self::Listener { owner, .. } | Self::Conn { owner, .. } => *owner,
        }
    }
}

/// The `jinn:net` provider: the table of live sockets.
pub struct HostNet {
    core: ProviderCore<Socket>,
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
            return Err(self
                .core
                .refuse(caller, "net caller holds no policy".to_owned()));
        };
        if !parsed.ip().is_loopback() {
            return Err(self.core.refuse(
                caller,
                format!("net listen refused: {addr:?} is not loopback (v0.1 binds loopback only)"),
            ));
        }
        match scope.bind {
            Some((low, high)) if (low..=high).contains(&parsed.port()) => Ok(parsed),
            _ => Err(self.core.refuse(
                caller,
                format!(
                    "net listen refused: port {} is outside the granted range",
                    parsed.port()
                ),
            )),
        }
    }

    async fn listen(&self, caller: PeerId, payload: Vec<u8>) -> Result<Vec<u8>, KernelError> {
        let addr = String::from_utf8(payload).map_err(|_| {
            refusal(
                ErrorCode::PluginFailed,
                "malformed net listen payload".to_owned(),
            )
        })?;
        let addr = self.authorize_bind(caller, &addr)?;
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|error| failed("listen", &error))?;
        let fiber = self.core.attribution(caller);
        let handle = self.core.mint();
        self.core.insert(
            handle,
            Socket::Listener {
                owner: caller,
                fiber,
                listener: Arc::new(listener),
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
