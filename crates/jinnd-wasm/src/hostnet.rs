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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::task::Poll;

use jinnd_api::{ErrorCode, FiberId, KernelError, KernelFuture, LedgerEventKind};
use tokio::net::{TcpListener, TcpStream};

use crate::broker::Broker;
use crate::broker_state::refusal;
use crate::grants::GrantScope;
use crate::hostcaps::NET_CONTRACT;
use crate::hostwire::{Reader, TAG_DATA, TAG_WOULD_BLOCK, encode_handle, encode_read};
use crate::lane::lock;
use crate::peer::{LedgerSink, Peer, PeerId};

#[cfg(all(test, not(feature = "loom")))]
mod tests;

/// The largest single read a guest may ask for.
const READ_CAP: usize = 64 * 1024;

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
    fn owner(&self) -> PeerId {
        match self {
            Self::Listener { owner, .. } | Self::Conn { owner, .. } => *owner,
        }
    }

    fn fiber(&self) -> Option<FiberId> {
        match self {
            Self::Listener { fiber, .. } | Self::Conn { fiber, .. } => *fiber,
        }
    }
}

/// The `jinn:net` provider: the table of live sockets.
pub struct HostNet {
    sink: Arc<dyn LedgerSink>,
    broker: OnceLock<Weak<Broker>>,
    table: Mutex<HashMap<u64, Socket>>,
    next: AtomicU64,
}

fn failed(what: &str, error: &std::io::Error) -> KernelError {
    refusal(ErrorCode::PluginFailed, format!("net {what}: {error}"))
}

impl HostNet {
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
    /// `jinn:net` contract (providing is authority).
    ///
    /// # Errors
    ///
    /// The broker's refusal of the provision.
    pub fn register(self: &Arc<Self>, broker: &Arc<Broker>) -> Result<(), KernelError> {
        let _ = self.broker.set(Arc::downgrade(broker));
        let peer = broker.register_peer(None);
        broker.grant(peer, NET_CONTRACT);
        broker.provide(peer, NET_CONTRACT, Arc::new(NetPeer(Arc::clone(self))))
    }

    /// The sockets this provider still holds.
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
                contract: NET_CONTRACT.to_owned(),
            },
            self.attribution(caller),
        );
        refusal(ErrorCode::EffectFailed, message)
    }

    fn mint(&self) -> u64 {
        self.next.fetch_add(1, Ordering::SeqCst) + 1
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
        let policy = self
            .broker()
            .and_then(|broker| broker.policy(caller, NET_CONTRACT));
        let Some(GrantScope::Net(scope)) = policy else {
            return Err(self.refuse(caller, "net caller holds no policy".to_owned()));
        };
        if !parsed.ip().is_loopback() {
            return Err(self.refuse(
                caller,
                format!("net listen refused: {addr:?} is not loopback (v0.1 binds loopback only)"),
            ));
        }
        match scope.bind {
            Some((low, high)) if (low..=high).contains(&parsed.port()) => Ok(parsed),
            _ => Err(self.refuse(
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
        let fiber = self.attribution(caller);
        let handle = self.mint();
        lock(&self.table).insert(
            handle,
            Socket::Listener {
                owner: caller,
                fiber,
                listener: Arc::new(listener),
            },
        );
        self.sink.append(
            LedgerEventKind::NetListening {
                handle,
                port: addr.port(),
            },
            fiber,
        );
        tracing::info!(handle, %addr, "net listening");
        Ok(encode_handle(handle))
    }

    /// The caller's own socket, copied out under the lock (R4).
    fn socket(&self, caller: PeerId, handle: u64) -> Result<Socket, KernelError> {
        match lock(&self.table).get(&handle) {
            Some(socket) if socket.owner() == caller => Ok(socket.clone()),
            Some(_) => Err(self.refuse(caller, format!("net handle {handle} is not the caller's"))),
            None => Err(refusal(
                ErrorCode::NotFound,
                format!("unknown net handle {handle}"),
            )),
        }
    }

    async fn handle_op(
        &self,
        caller: PeerId,
        operation: &str,
        payload: &[u8],
    ) -> Result<Vec<u8>, KernelError> {
        let mut reader = Reader::new(payload, "net handle");
        let handle = reader.u64()?;
        let socket = self.socket(caller, handle)?;
        match (operation, socket) {
            (
                "accept",
                Socket::Listener {
                    listener, fiber, ..
                },
            ) => {
                let pending =
                    std::future::poll_fn(|cx| Poll::Ready(listener.poll_accept(cx))).await;
                match pending {
                    Poll::Ready(Ok((stream, _))) => {
                        let conn = self.mint();
                        lock(&self.table).insert(
                            conn,
                            Socket::Conn {
                                owner: caller,
                                fiber,
                                stream: Arc::new(stream),
                            },
                        );
                        self.sink.append(
                            LedgerEventKind::NetAccepted {
                                listener: handle,
                                handle: conn,
                            },
                            fiber,
                        );
                        let mut wire = vec![TAG_DATA];
                        wire.extend(conn.to_le_bytes());
                        Ok(wire)
                    }
                    Poll::Ready(Err(error)) => Err(failed("accept", &error)),
                    Poll::Pending => Ok(vec![TAG_WOULD_BLOCK]),
                }
            }
            ("read", Socket::Conn { stream, .. }) => {
                let max = (reader.u32()? as usize).clamp(1, READ_CAP);
                let mut buffer = vec![0u8; max];
                Ok(match stream.try_read(&mut buffer) {
                    Ok(0) => encode_read(None, true),
                    Ok(count) => {
                        buffer.truncate(count);
                        encode_read(Some(buffer), false)
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        encode_read(None, false)
                    }
                    Err(error) => return Err(failed("read", &error)),
                })
            }
            ("write", Socket::Conn { stream, .. }) => {
                let count = match stream.try_write(reader.rest()) {
                    Ok(count) => count,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => 0,
                    Err(error) => return Err(failed("write", &error)),
                };
                Ok(u32::try_from(count)
                    .unwrap_or(u32::MAX)
                    .to_le_bytes()
                    .to_vec())
            }
            ("close", _) => {
                self.withdraw(handle).await?;
                Ok(Vec::new())
            }
            (other, _) => Err(refusal(
                ErrorCode::PluginFailed,
                format!("net operation {other:?} does not apply to handle {handle}"),
            )),
        }
    }

    /// The registration's release (suspend, dispose, or the guest's own
    /// close): the socket drops — closed — on the record; an unknown
    /// handle is a clean no-op.
    async fn withdraw(&self, handle: u64) -> Result<(), KernelError> {
        let Some(socket) = lock(&self.table).remove(&handle) else {
            return Ok(());
        };
        self.sink
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
