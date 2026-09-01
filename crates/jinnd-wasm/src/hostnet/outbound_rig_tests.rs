//! The loopback target and provider rig the M2-K14 outbound pins share.
//!
//! A rig is one `HostNet` behind one broker with one granted guest, so a
//! test states its authority as data and nothing is admitted by accident.
//! The target counts every ACCEPTED connection and keeps the request bytes,
//! so a test can prove both what never left the machine and what did.

use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jinnd_api::{ErrorCode, FiberId, KernelError, LedgerEventKind, RefusalReason};

use super::HostNet;
use super::tests::Recording;
use crate::broker::Broker;
use crate::grants::{GrantScope, NetScope};
use crate::hostcaps::NET_CONTRACT;
use crate::hostwire::{decode_response, encode_request, put_segment};
use crate::peer::LedgerSink;

/// A single-shot loopback HTTP target.
pub(super) struct Target {
    pub(super) port: u16,
    pub(super) hits: Arc<AtomicUsize>,
    pub(super) seen: Arc<Mutex<Vec<u8>>>,
}

/// What the target writes back once it has read a request head.
#[derive(Clone)]
pub(super) enum Answer {
    /// Raw response bytes.
    Raw(String),
    /// A 200 whose body is `size` bytes, with a correct content-length.
    Body(usize),
    /// Accept, read, and never answer (the bound's probe).
    Silent,
}

impl Target {
    /// A target at `port` with nothing recorded yet: the TLS rig (M2-K15)
    /// runs its own accept loop and fills these counters itself.
    pub(super) fn empty(port: u16) -> Self {
        Self {
            port,
            hits: Arc::new(AtomicUsize::new(0)),
            seen: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

pub(super) fn target(answer: Answer) -> Target {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").unwrap_or_else(|error| panic!("bind: {error}"));
    let port = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("addr: {error}"))
        .port();
    let hits = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let (counter, recorder) = (Arc::clone(&hits), Arc::clone(&seen));
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            counter.fetch_add(1, Ordering::SeqCst);
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                match stream.read(&mut byte) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => head.push(byte[0]),
                }
            }
            recorder
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .extend(head);
            match &answer {
                Answer::Raw(text) => {
                    let _ = stream.write_all(text.as_bytes());
                }
                Answer::Body(size) => {
                    let _ = stream.write_all(
                        format!("HTTP/1.1 200 OK\r\ncontent-length: {size}\r\n\r\n").as_bytes(),
                    );
                    let _ = stream.write_all(&vec![b'x'; *size]);
                }
                Answer::Silent => std::thread::sleep(Duration::from_secs(30)),
            }
        }
    });
    Target { port, hits, seen }
}

/// A target that answers `status` pointing at `port`.
pub(super) fn redirector(status: u16, port: u16) -> Target {
    target(Answer::Raw(format!(
        "HTTP/1.1 {status} Found\r\nlocation: http://127.0.0.1:{port}/taken\r\ncontent-length: 0\r\n\r\n"
    )))
}

/// The 200 every happy-path target answers.
pub(super) fn ok_body(body: &str) -> Answer {
    Answer::Raw(format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    ))
}

pub(super) struct Rig {
    pub(super) ledger: Arc<Recording>,
    broker: Arc<Broker>,
    guest: u64,
}

/// A rig whose guest holds exactly `outbound` as its allowlist.
pub(super) fn rig(outbound: &[&str]) -> Rig {
    let ledger = Arc::new(Recording::new());
    let broker = Arc::new(Broker::new(Arc::clone(&ledger) as Arc<dyn LedgerSink>));
    let provider = HostNet::new(Arc::clone(&ledger) as Arc<dyn LedgerSink>);
    provider
        .register(&broker)
        .unwrap_or_else(|error| panic!("register: {error:?}"));
    let guest = broker.register_peer(Some(FiberId(7)));
    broker.grant_with(
        guest,
        NET_CONTRACT,
        GrantScope::Net(NetScope {
            bind: Vec::new(),
            outbound: outbound.iter().map(|host| (*host).to_owned()).collect(),
        }),
    );
    Rig {
        ledger,
        broker,
        guest,
    }
}

/// A rig admitting exactly `port` on loopback.
pub(super) fn rig_pair(port: u16) -> Rig {
    rig(&[&format!("127.0.0.1:{port}")])
}

type Answered = Result<(u16, Vec<(String, String)>, Vec<u8>), ErrorCode>;

/// The two provided entry points. They are one door with two handles, and
/// that is a claim about the kernel's own code — so every property this
/// card asserts is proven entering through BOTH, never through the new one
/// with the old one assumed (COO round-2 steer). A legacy door that skips
/// the allowlist would be worse than no legacy door.
#[derive(Clone, Copy, Debug)]
pub(super) enum Door {
    /// `request` at its 0.1.0 declaration and 0.1.0 broker wire.
    Declared,
    /// `send-request`, the 0.2.0 whole-response edition.
    Whole,
}

pub(super) const DOORS: [Door; 2] = [Door::Declared, Door::Whole];

impl Rig {
    /// One `send-request`: the whole-response edition (0.2.0).
    pub(super) async fn request(
        &self,
        method: &str,
        url: &str,
        headers: &[(String, String)],
        body: &[u8],
    ) -> Answered {
        let payload = encode_request(method, url, headers, body);
        let answer = self
            .broker
            .dispatch(self.guest, NET_CONTRACT, "send-request", payload)
            .await
            .map_err(|error| error.code)?;
        decode_response(&answer).map_err(|error| error.code)
    }

    pub(super) async fn get(&self, url: &str) -> Answered {
        self.request("GET", url, &[], &[]).await
    }

    /// One `request` at its 0.1.0 declaration and 0.1.0 broker wire: no
    /// header count, no headers, the response BODY alone.
    pub(super) async fn legacy(
        &self,
        method: &str,
        url: &str,
        body: &[u8],
    ) -> Result<Vec<u8>, ErrorCode> {
        let mut wire = Vec::new();
        put_segment(&mut wire, method.as_bytes());
        put_segment(&mut wire, url.as_bytes());
        wire.extend(body);
        self.broker
            .dispatch(self.guest, NET_CONTRACT, "request", wire)
            .await
            .map_err(|error| error.code)
    }

    /// One call through `door`, answering the response BODY — the only
    /// part both shapes agree on, and enough for every refusal class.
    pub(super) async fn through(
        &self,
        door: Door,
        method: &str,
        url: &str,
        body: &[u8],
    ) -> Result<Vec<u8>, ErrorCode> {
        match door {
            Door::Declared => self.legacy(method, url, body).await,
            Door::Whole => self
                .request(method, url, &[], body)
                .await
                .map(|(_, _, body)| body),
        }
    }

    /// A GET through `door`.
    pub(super) async fn door_get(&self, door: Door, url: &str) -> Result<Vec<u8>, ErrorCode> {
        self.through(door, "GET", url, &[]).await
    }

    /// A GET through `door`, keeping the WHOLE error — what the caller is
    /// told, not just how it is classified. The M2-K15 redaction pins read
    /// the prose, so they need it (`door_get` throws it away).
    pub(super) async fn door_get_told(
        &self,
        door: Door,
        url: &str,
    ) -> Result<Vec<u8>, KernelError> {
        let payload = match door {
            Door::Declared => {
                let mut wire = Vec::new();
                put_segment(&mut wire, b"GET");
                put_segment(&mut wire, url.as_bytes());
                wire
            }
            Door::Whole => encode_request("GET", url, &[], &[]),
        };
        let operation = match door {
            Door::Declared => "request",
            Door::Whole => "send-request",
        };
        let answer = self
            .broker
            .dispatch(self.guest, NET_CONTRACT, operation, payload)
            .await?;
        match door {
            Door::Declared => Ok(answer),
            Door::Whole => decode_response(&answer).map(|(_, _, body)| body),
        }
    }

    /// The effect ids this rig recorded, in order.
    pub(super) fn effects(&self) -> Vec<u64> {
        self.requested()
            .iter()
            .map(|kind| match kind {
                LedgerEventKind::NetRequested { effect, .. } => effect.0,
                _ => panic!("not a request row"),
            })
            .collect()
    }

    /// Every `NetRequested` row this rig recorded.
    pub(super) fn requested(&self) -> Vec<LedgerEventKind> {
        self.ledger
            .kinds()
            .into_iter()
            .map(|(kind, _)| kind)
            .filter(|kind| matches!(kind, LedgerEventKind::NetRequested { .. }))
            .collect()
    }

    /// The ledgered refusals of one typed class — the record, read by the
    /// class the kernel wrote, never by its prose (M2-K15).
    pub(super) fn refusals(&self, reason: RefusalReason) -> usize {
        self.ledger
            .kinds()
            .iter()
            .filter(|(kind, _)| {
                matches!(kind, LedgerEventKind::GrantRefused { reason: wrote, .. } if *wrote == reason)
            })
            .count()
    }

    pub(super) fn scope_refusals(&self) -> usize {
        self.ledger
            .kinds()
            .iter()
            .filter(|(kind, _)| {
                matches!(
                    kind,
                    LedgerEventKind::GrantRefused {
                        reason: RefusalReason::ScopeMismatch,
                        ..
                    }
                )
            })
            .count()
    }
}
