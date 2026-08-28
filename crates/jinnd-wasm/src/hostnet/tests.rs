//! Provider-seam pins for M2-K6 `jinn:net`: loopback-only binds inside
//! the granted range (every refusal on the record), non-blocking accept /
//! read / write with a real TCP peer, caller-scoped handles, the release
//! that closes and ledgers, and the declared-not-provided `request`.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use jinnd_api::{ErrorCode, FiberId, LedgerEventKind};

use super::HostNet;
use crate::broker::Broker;
use crate::grants::{GrantScope, NetScope};
use crate::hostcaps::NET_CONTRACT;
use crate::hostwire::{TAG_DATA, TAG_EOF, TAG_WOULD_BLOCK};
use crate::peer::LedgerSink;

struct Recording(Mutex<Vec<(LedgerEventKind, Option<FiberId>)>>);

impl LedgerSink for Recording {
    fn append(&self, kind: LedgerEventKind, fiber: Option<FiberId>) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((kind, fiber));
    }
}

impl Recording {
    fn kinds(&self) -> Vec<(LedgerEventKind, Option<FiberId>)> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn refusals(&self) -> usize {
        self.kinds()
            .iter()
            .filter(|(kind, _)| {
                matches!(kind, LedgerEventKind::GrantRefused { contract } if contract == NET_CONTRACT)
            })
            .count()
    }
}

struct Rig {
    ledger: Arc<Recording>,
    broker: Arc<Broker>,
    provider: Arc<HostNet>,
    /// Fiber 7, granted the bind range around `port`.
    guest: u64,
    /// Fiber 8, a bare grant: the empty policy.
    bare: u64,
    port: u16,
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .unwrap_or_else(|error| panic!("free port: {error}"))
}

fn rig() -> Rig {
    let ledger = Arc::new(Recording(Mutex::new(Vec::new())));
    let broker = Arc::new(Broker::new(Arc::clone(&ledger) as Arc<dyn LedgerSink>));
    let provider = HostNet::new(Arc::clone(&ledger) as Arc<dyn LedgerSink>);
    provider
        .register(&broker)
        .unwrap_or_else(|error| panic!("register: {error:?}"));
    let port = free_port();
    let guest = broker.register_peer(Some(FiberId(7)));
    broker.grant_with(
        guest,
        NET_CONTRACT,
        GrantScope::Net(NetScope {
            bind: Some((port, port)),
            outbound: Vec::new(),
        }),
    );
    let bare = broker.register_peer(Some(FiberId(8)));
    broker.grant_with(bare, NET_CONTRACT, GrantScope::Net(NetScope::default()));
    Rig {
        ledger,
        broker,
        provider,
        guest,
        bare,
        port,
    }
}

fn with_handle(handle: u64, tail: &[u8]) -> Vec<u8> {
    let mut wire = handle.to_le_bytes().to_vec();
    wire.extend(tail);
    wire
}

fn handle_of(answer: &[u8]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&answer[answer.len() - 8..]);
    u64::from_le_bytes(bytes)
}

impl Rig {
    async fn call(&self, peer: u64, op: &str, payload: Vec<u8>) -> Result<Vec<u8>, ErrorCode> {
        self.broker
            .dispatch(peer, NET_CONTRACT, op, payload)
            .await
            .map_err(|error| error.code)
    }

    async fn listen(&self) -> u64 {
        let answer = self
            .call(
                self.guest,
                "listen",
                format!("127.0.0.1:{}", self.port).into_bytes(),
            )
            .await
            .unwrap_or_else(|code| panic!("listen: {code:?}"));
        handle_of(&answer)
    }

    async fn accept(&self, listener: u64) -> Option<u64> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let answer = self
                .call(self.guest, "accept", with_handle(listener, &[]))
                .await
                .unwrap_or_else(|code| panic!("accept: {code:?}"));
            if answer[0] == TAG_DATA {
                return Some(handle_of(&answer));
            }
            if Instant::now() > deadline {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn read(&self, conn: u64) -> (u8, Vec<u8>) {
        let answer = self
            .call(
                self.guest,
                "read",
                with_handle(conn, &4096u32.to_le_bytes()),
            )
            .await
            .unwrap_or_else(|code| panic!("read: {code:?}"));
        (answer[0], answer[1..].to_vec())
    }

    async fn read_until(&self, conn: u64, tag: u8) -> Vec<u8> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let (got, data) = self.read(conn).await;
            if got == tag {
                return data;
            }
            assert!(Instant::now() < deadline, "tag {tag} arrives");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Every bind refusal on the record: a bare grant (empty policy), a port
/// outside the range, a non-loopback address, an ungranted caller; and a
/// malformed address is the typed invalid, not a grant event.
#[tokio::test]
async fn binds_outside_the_policy_refuse_on_the_record() {
    let rig = rig();
    let addr = format!("127.0.0.1:{}", rig.port);
    assert_eq!(
        rig.call(rig.bare, "listen", addr.clone().into_bytes())
            .await,
        Err(ErrorCode::EffectFailed),
        "the empty policy binds nothing"
    );
    assert_eq!(
        rig.call(
            rig.guest,
            "listen",
            format!("127.0.0.1:{}", rig.port.wrapping_add(1)).into_bytes()
        )
        .await,
        Err(ErrorCode::EffectFailed)
    );
    assert_eq!(
        rig.call(
            rig.guest,
            "listen",
            format!("0.0.0.0:{}", rig.port).into_bytes()
        )
        .await,
        Err(ErrorCode::EffectFailed)
    );
    let stranger = rig.broker.register_peer(Some(FiberId(9)));
    assert_eq!(
        rig.call(stranger, "listen", addr.into_bytes()).await,
        Err(ErrorCode::EffectFailed)
    );
    assert_eq!(rig.ledger.refusals(), 4, "each refusal ledgered");
    assert_eq!(
        rig.call(rig.guest, "listen", b"not-an-address".to_vec())
            .await,
        Err(ErrorCode::InvalidProfile)
    );
    assert_eq!(rig.provider.live(), 0);
}

/// A real TCP peer: accept answers would-block until a connect, then the
/// connection reads what the peer wrote, echoes, and reads EOF once the
/// peer closes; every registration event is on the record.
#[tokio::test]
async fn a_loopback_listener_accepts_reads_writes_and_sees_eof() {
    let rig = rig();
    let listener = rig.listen().await;
    let answer = rig
        .call(rig.guest, "accept", with_handle(listener, &[]))
        .await
        .unwrap_or_else(|code| panic!("accept: {code:?}"));
    assert_eq!(answer, vec![TAG_WOULD_BLOCK], "nothing pending yet");

    let port = rig.port;
    let peer = tokio::task::spawn_blocking(move || {
        let mut stream = TcpStream::connect(("127.0.0.1", port))
            .unwrap_or_else(|error| panic!("connect: {error}"));
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap_or_else(|error| panic!("{error}"));
        stream
            .write_all(b"ping")
            .unwrap_or_else(|error| panic!("{error}"));
        let mut echoed = [0u8; 4];
        stream
            .read_exact(&mut echoed)
            .unwrap_or_else(|error| panic!("echo: {error}"));
        echoed
    });

    let conn = rig
        .accept(listener)
        .await
        .unwrap_or_else(|| panic!("the connect is accepted"));
    let data = rig.read_until(conn, TAG_DATA).await;
    assert_eq!(data, b"ping");
    let written = rig
        .call(rig.guest, "write", with_handle(conn, &data))
        .await
        .unwrap_or_else(|code| panic!("write: {code:?}"));
    assert_eq!(written, 4u32.to_le_bytes().to_vec());
    assert_eq!(
        peer.await.unwrap_or_else(|error| panic!("peer: {error}")),
        *b"ping"
    );
    assert!(rig.read_until(conn, TAG_EOF).await.is_empty());

    let kinds = rig.ledger.kinds();
    assert!(kinds.iter().any(|(kind, fiber)| matches!(
        kind,
        LedgerEventKind::NetListening { handle, port } if *handle == listener && *port == rig.port
    ) && *fiber == Some(FiberId(7))));
    assert!(kinds.iter().any(|(kind, _)| matches!(
        kind,
        LedgerEventKind::NetAccepted { listener: from, handle } if *from == listener && *handle == conn
    )));
    assert_eq!(rig.provider.live(), 2);
}

/// A handle is the caller's alone (R4); the guest's `close` and the
/// kernel's release both drop the socket on the record — a fresh connect
/// to the released port is refused, and a second release is a no-op.
#[tokio::test]
async fn handles_are_caller_scoped_and_the_release_closes_on_the_record() {
    let rig = rig();
    let listener = rig.listen().await;
    assert_eq!(
        rig.call(rig.bare, "accept", with_handle(listener, &[]))
            .await,
        Err(ErrorCode::EffectFailed)
    );
    assert_eq!(rig.ledger.refusals(), 1);
    assert!(TcpStream::connect(("127.0.0.1", rig.port)).is_ok());
    rig.provider
        .withdraw(listener)
        .await
        .unwrap_or_else(|error| panic!("release: {error:?}"));
    assert_eq!(rig.provider.live(), 0);
    assert!(
        TcpStream::connect(("127.0.0.1", rig.port)).is_err(),
        "the port is released"
    );
    assert!(rig.ledger.kinds().iter().any(|(kind, fiber)| matches!(
        kind,
        LedgerEventKind::NetClosed { handle } if *handle == listener
    ) && *fiber == Some(FiberId(7))));
    rig.provider
        .withdraw(listener)
        .await
        .unwrap_or_else(|error| panic!("a second release is clean: {error:?}"));
    assert_eq!(
        rig.call(rig.guest, "accept", with_handle(listener, &[]))
            .await,
        Err(ErrorCode::NotFound)
    );
    let again = rig.listen().await;
    rig.call(rig.guest, "close", with_handle(again, &[]))
        .await
        .unwrap_or_else(|code| panic!("close: {code:?}"));
    assert_eq!(rig.provider.live(), 0, "the guest's close releases too");
}

/// `request` is declared and not provided in v0.1: a typed provider
/// failure, never a hang or a silent empty answer.
#[tokio::test]
async fn request_is_declared_not_provided() {
    let rig = rig();
    assert_eq!(
        rig.call(rig.guest, "request", Vec::new()).await,
        Err(ErrorCode::PluginFailed)
    );
}
