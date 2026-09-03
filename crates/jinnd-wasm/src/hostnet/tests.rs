//! Provider-seam pins for M2-K6 `jinn:net`: loopback-only binds inside
//! the granted range (every refusal on the record), non-blocking accept /
//! read / write with a real TCP peer, caller-scoped handles, the release
//! that closes and ledgers, and the declared-not-provided `request`.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use jinnd_api::{ErrorCode, FiberId, LedgerEventKind, RefusalReason};
use tokio::io::unix::AsyncFd;

use super::{HostNet, Socket};
use crate::broker::Broker;
use crate::grants::{GrantScope, NetScope};
use crate::hostcaps::NET_CONTRACT;
use crate::hostwire::{TAG_DATA, TAG_EOF, TAG_WOULD_BLOCK};
use crate::peer::LedgerSink;

pub(super) struct Recording(Mutex<Vec<(LedgerEventKind, Option<FiberId>)>>);

impl LedgerSink for Recording {
    fn append(&self, kind: LedgerEventKind, fiber: Option<FiberId>) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((kind, fiber));
    }
}

impl Recording {
    pub(super) fn new() -> Self {
        Self(Mutex::new(Vec::new()))
    }

    pub(super) fn kinds(&self) -> Vec<(LedgerEventKind, Option<FiberId>)> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn refusals(&self) -> usize {
        self.kinds()
            .iter()
            .filter(|(kind, _)| {
                matches!(kind, LedgerEventKind::GrantRefused { contract, .. } if contract == NET_CONTRACT)
            })
            .count()
    }
}

pub(super) struct Rig {
    pub(super) ledger: Arc<Recording>,
    pub(super) broker: Arc<Broker>,
    pub(super) provider: Arc<HostNet>,
    /// Fiber 7, granted the bind range around `port`.
    pub(super) guest: u64,
    /// Fiber 8, a bare grant: the empty policy.
    bare: u64,
    pub(super) port: u16,
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
        .unwrap_or_else(|error| panic!("free port: {error}"))
}

pub(super) fn rig() -> Rig {
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
            bind: vec![(port, port)],
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

pub(super) fn with_handle(handle: u64, tail: &[u8]) -> Vec<u8> {
    let mut wire = handle.to_le_bytes().to_vec();
    wire.extend(tail);
    wire
}

fn handle_of(answer: &[u8]) -> u64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&answer[answer.len() - 8..]);
    u64::from_le_bytes(bytes)
}

/// A weak handle on `listener`'s descriptor holder (M2-K20): dead once
/// every clone of the row is gone — the row's own, the wake task's — which
/// is the moment the descriptor closes. The property a release owns, as
/// distinct from what the box does with the port afterwards.
pub(super) fn descriptor_of(rig: &Rig, listener: u64) -> Weak<AsyncFd<std::net::TcpListener>> {
    match rig.provider.core.row(rig.guest, listener) {
        Ok(Socket::Listener { listener, .. }) => Arc::downgrade(&listener),
        _ => panic!("a live listener row"),
    }
}

impl Rig {
    pub(super) async fn call(
        &self,
        peer: u64,
        op: &str,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, ErrorCode> {
        self.broker
            .dispatch(peer, NET_CONTRACT, op, payload)
            .await
            .map_err(|error| error.code)
    }

    pub(super) async fn listen(&self) -> u64 {
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

    pub(super) async fn accept(&self, listener: u64) -> Option<u64> {
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
/// kernel's release both drop the socket on the record — the descriptor
/// is gone before the release returns (M2-K20), and a second release is a
/// no-op.
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
    let descriptor = descriptor_of(&rig, listener);
    rig.provider
        .withdraw(listener)
        .await
        .unwrap_or_else(|error| panic!("release: {error:?}"));
    assert_eq!(rig.provider.live(), 0);
    assert!(
        descriptor.upgrade().is_none(),
        "the descriptor is dropped — closed — before the release returns"
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
    // A fresh port for the guest's own close: a just-released port is the
    // OS's to hand back when it likes (M2-K20; `release_tests`).
    let fresh = self::rig();
    let again = fresh.listen().await;
    fresh
        .call(fresh.guest, "close", with_handle(again, &[]))
        .await
        .unwrap_or_else(|code| panic!("close: {code:?}"));
    assert_eq!(fresh.provider.live(), 0, "the guest's close releases too");
}

/// A malformed `request` payload is the typed malformed answer, never a
/// hang or a silent empty one. (`request` itself is provided from M2-K14;
/// its authority, bounds and record are pinned in `request_tests`.)
#[tokio::test]
async fn a_malformed_request_payload_is_typed() {
    let rig = rig();
    assert_eq!(
        rig.call(rig.guest, "request", Vec::new()).await,
        Err(ErrorCode::PluginFailed)
    );
}

/// A recording delivery face: what the wake task hands the holder.
pub(super) struct Face(pub(super) Mutex<Vec<(u64, String, Vec<u8>)>>);

impl crate::topics::EventTarget for Face {
    fn deliver(
        &self,
        token: u64,
        topic: &str,
        payload: Vec<u8>,
        _: Option<std::num::NonZeroU64>,
    ) -> jinnd_api::KernelFuture<'static, Vec<u8>> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((token, topic.to_owned(), payload));
        Box::pin(async { Ok(Vec::new()) })
    }
}

impl Face {
    pub(super) fn wakes(&self, handle: u64) -> usize {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .filter(|(token, topic, payload)| {
                *token == handle
                    && topic == super::READABLE_TOPIC
                    && payload == &handle.to_le_bytes()
            })
            .count()
    }
}

impl Recording {
    pub(super) fn readable_wakes(&self, handle: u64) -> usize {
        self.kinds()
            .iter()
            .filter(|(kind, fiber)| {
                matches!(kind, LedgerEventKind::NetReadable { handle: woken } if *woken == handle)
                    && *fiber == Some(FiberId(7))
            })
            .count()
    }
}

pub(super) async fn settle(condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !condition() {
        assert!(Instant::now() < deadline, "the wake lands");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// M2-K7 (harness #23): a listener with a pending connection and a
/// connection with bytes each wake the holder ONCE — `handle-event(handle,
/// "jinn:net/readable", handle)` on its own face, ledgered as `NetReadable`
/// with the holder's attribution — with no alarm anywhere; a guest read
/// re-arms, so the next transition wakes again; EOF wakes once.
#[tokio::test]
async fn readiness_wakes_the_holder_once_per_transition_with_no_alarm() {
    let rig = rig();
    let face = Arc::new(Face(Mutex::new(Vec::new())));
    rig.broker.attach_target(
        rig.guest,
        Arc::clone(&face) as Arc<dyn crate::topics::EventTarget>,
    );
    let listener = rig.listen().await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(face.wakes(listener), 0, "an idle listener never wakes");

    let mut client =
        TcpStream::connect(("127.0.0.1", rig.port)).unwrap_or_else(|error| panic!("{error}"));
    settle(|| face.wakes(listener) == 1).await;
    assert_eq!(rig.ledger.readable_wakes(listener), 1);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        face.wakes(listener),
        1,
        "one wake per transition, not per poll"
    );

    let conn = rig
        .accept(listener)
        .await
        .unwrap_or_else(|| panic!("accepted"));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(face.wakes(conn), 0, "a quiet connection never wakes");
    assert_eq!(
        face.wakes(listener),
        1,
        "the accept consumed the transition: one connect, one wake"
    );
    client
        .write_all(b"hello")
        .unwrap_or_else(|error| panic!("{error}"));
    settle(|| face.wakes(conn) == 1).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        face.wakes(conn),
        1,
        "unread bytes wake once until the guest reads"
    );
    assert_eq!(rig.read_until(conn, TAG_DATA).await, b"hello");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        face.wakes(conn),
        1,
        "a read that drained the bytes is not a new transition"
    );
    // The guest read re-armed; the next transition wakes again.
    client
        .write_all(b"again")
        .unwrap_or_else(|error| panic!("{error}"));
    settle(|| face.wakes(conn) == 2).await;
    assert_eq!(rig.read_until(conn, TAG_DATA).await, b"again");
    drop(client);
    settle(|| face.wakes(conn) == 3).await;
    assert_eq!(rig.read_until(conn, TAG_EOF).await, Vec::<u8>::new());
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(face.wakes(conn), 3, "EOF wakes exactly once");
    assert!(
        !rig.ledger
            .kinds()
            .iter()
            .any(|(kind, _)| matches!(kind, LedgerEventKind::AlarmWake { .. })),
        "no alarm anywhere"
    );
}

/// A guest that CLOSES the socket from inside its own readiness handler
/// (the echo shape on EOF): the release never awaits the wake task that
/// is awaiting the handler — no deadlock, the close lands on the record.
#[tokio::test]
async fn closing_from_inside_the_wake_handler_never_deadlocks() {
    struct Closer {
        broker: Arc<Broker>,
        guest: u64,
        /// Only this handle is closed from inside the handler; any other
        /// wake is answered with nothing.
        conn: u64,
    }
    impl crate::topics::EventTarget for Closer {
        fn deliver(
            &self,
            token: u64,
            _topic: &str,
            _payload: Vec<u8>,
            _: Option<std::num::NonZeroU64>,
        ) -> jinnd_api::KernelFuture<'static, Vec<u8>> {
            let broker = Arc::clone(&self.broker);
            let guest = self.guest;
            let conn = self.conn;
            Box::pin(async move {
                if token != conn {
                    return Ok(Vec::new());
                }
                broker
                    .dispatch(guest, NET_CONTRACT, "close", with_handle(token, &[]))
                    .await
            })
        }
    }
    let rig = rig();
    let listener = rig.listen().await;
    let mut client =
        TcpStream::connect(("127.0.0.1", rig.port)).unwrap_or_else(|error| panic!("{error}"));
    let conn = rig
        .accept(listener)
        .await
        .unwrap_or_else(|| panic!("accepted"));
    rig.broker.attach_target(
        rig.guest,
        Arc::new(Closer {
            broker: Arc::clone(&rig.broker),
            guest: rig.guest,
            conn,
        }) as Arc<dyn crate::topics::EventTarget>,
    );
    client
        .write_all(b"bye")
        .unwrap_or_else(|error| panic!("{error}"));
    let closed = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if rig.ledger.kinds().iter().any(|(kind, _)| {
                matches!(kind, LedgerEventKind::NetClosed { handle } if *handle == conn)
            }) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        closed.is_ok(),
        "the close from inside the handler lands, no deadlock"
    );
    assert_eq!(
        rig.provider.live(),
        1,
        "the connection is released, the listener stays"
    );
}

/// Two connections pending before the guest accepts: the first accept
/// consumes one and the OTHER is still pending — a new transition, exactly
/// one wake; the second accept drains the listener and no wake follows
/// (level-triggered: never lost, never doubled).
#[tokio::test]
async fn a_second_pending_connection_wakes_again_exactly_once() {
    let rig = rig();
    let face = Arc::new(Face(Mutex::new(Vec::new())));
    rig.broker.attach_target(
        rig.guest,
        Arc::clone(&face) as Arc<dyn crate::topics::EventTarget>,
    );
    let listener = rig.listen().await;
    let _first =
        TcpStream::connect(("127.0.0.1", rig.port)).unwrap_or_else(|error| panic!("{error}"));
    let _second =
        TcpStream::connect(("127.0.0.1", rig.port)).unwrap_or_else(|error| panic!("{error}"));
    settle(|| face.wakes(listener) == 1).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(face.wakes(listener), 1, "two connects, one transition");
    rig.accept(listener)
        .await
        .unwrap_or_else(|| panic!("first accepted"));
    settle(|| face.wakes(listener) == 2).await;
    rig.accept(listener)
        .await
        .unwrap_or_else(|| panic!("second accepted"));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        face.wakes(listener),
        2,
        "the drained listener does not wake"
    );
    assert_eq!(rig.ledger.readable_wakes(listener), 2);
    assert_eq!(rig.provider.live(), 3, "the listener and both connections");
}

/// Under a chatty peer the wake count is bounded by what the guest does,
/// never by the byte count (R9): a flood of writes lands ONE wake until
/// the guest reads; and once the socket is released no wake is ever
/// delivered or ledgered again, and its descriptor is gone before the
/// release returns (M2-K20: the descriptor, not the port — a foreign
/// wildcard listener may answer the released port; `release_tests`).
#[tokio::test]
async fn a_flood_wakes_once_and_a_released_socket_never_wakes_again() {
    let rig = rig();
    let face = Arc::new(Face(Mutex::new(Vec::new())));
    rig.broker.attach_target(
        rig.guest,
        Arc::clone(&face) as Arc<dyn crate::topics::EventTarget>,
    );
    let listener = rig.listen().await;
    let mut client =
        TcpStream::connect(("127.0.0.1", rig.port)).unwrap_or_else(|error| panic!("{error}"));
    let conn = rig
        .accept(listener)
        .await
        .unwrap_or_else(|| panic!("accepted"));
    for _ in 0..200 {
        client
            .write_all(b"x")
            .unwrap_or_else(|error| panic!("{error}"));
    }
    settle(|| face.wakes(conn) == 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(face.wakes(conn), 1, "200 writes, one wake");
    assert_eq!(rig.ledger.readable_wakes(conn), 1);
    let mut reads = 0;
    while rig.read(conn).await.0 == TAG_DATA {
        reads += 1;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        face.wakes(conn) <= 1 + reads,
        "wakes ({}) are bounded by guest reads ({reads}) plus one",
        face.wakes(conn)
    );
    rig.call(rig.guest, "close", with_handle(conn, &[]))
        .await
        .unwrap_or_else(|code| panic!("close: {code:?}"));
    let before = rig.ledger.readable_wakes(conn);
    let _ = client.write_all(b"after close");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        rig.ledger.readable_wakes(conn),
        before,
        "a released socket never wakes"
    );
    let before = rig.ledger.readable_wakes(listener);
    let descriptor = descriptor_of(&rig, listener);
    rig.provider
        .withdraw(listener)
        .await
        .unwrap_or_else(|error| panic!("release: {error:?}"));
    assert!(
        descriptor.upgrade().is_none(),
        "the descriptor is dropped — closed — before the release returns"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        rig.ledger.readable_wakes(listener),
        before,
        "no wake after release"
    );
}

/// Round-3 ruling (Law 1), red-first at the provider seam: a peer holding
/// two DISJOINT single-port grants may bind either granted port and is
/// refused, on the record, for the port between them — the hull an earlier
/// composition took would have served a port no grant conferred.
#[tokio::test]
async fn a_port_between_two_disjoint_grants_refuses_on_the_record() {
    let rig = rig();
    let peer = rig.broker.register_peer(Some(FiberId(11)));
    let low = free_port();
    let high = low.wrapping_add(2);
    for port in [low, high] {
        rig.broker.grant_with(
            peer,
            NET_CONTRACT,
            GrantScope::Net(NetScope {
                bind: vec![(port, port)],
                outbound: Vec::new(),
            }),
        );
    }
    let listen = |port: u16| rig.call(peer, "listen", format!("127.0.0.1:{port}").into_bytes());
    assert!(listen(low).await.is_ok(), "the lower granted port binds");
    assert!(listen(high).await.is_ok(), "the upper granted port binds");
    assert_eq!(
        listen(low.wrapping_add(1)).await,
        Err(ErrorCode::EffectFailed),
        "the port between two disjoint grants was never conferred"
    );
    assert_eq!(rig.ledger.refusals(), 1, "the refusal is on the record");
    assert!(
        rig.ledger.kinds().iter().any(|(kind, by)| matches!(
            kind,
            LedgerEventKind::GrantRefused {
                reason: RefusalReason::ScopeMismatch,
                contract,
                ..
            } if contract == NET_CONTRACT
        ) && *by == Some(FiberId(11))),
        "refused as a scope mismatch, attributed to the caller"
    );
}
