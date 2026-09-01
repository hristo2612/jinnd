//! Provider-seam pins for M2-K14 `jinn:net.request`: the outbound
//! allowlist as ACCEPTANCE (an allowed authority succeeds, an
//! off-allowlist one refuses without ever connecting, a redirect is
//! answered and never followed, a malformed URL is a third distinct
//! answer), the Law-2 record that carries the call's shape and never its
//! credentials, and the bounds that keep an authorized call from stalling
//! the caller (R1, R9).
//!
//! Every refusal case asserts its OWN precondition: the same request under
//! a grant that admits it must succeed, or the refusal proves nothing
//! (M2-K8 round-3 lesson).

use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jinnd_api::{ErrorCode, FiberId, LedgerEventKind, RefusalReason};

use super::HostNet;
use crate::broker::Broker;
use crate::grants::{GrantScope, NetScope};
use crate::hostcaps::NET_CONTRACT;
use crate::hostwire::{decode_response, encode_request};
use crate::peer::LedgerSink;

use super::tests::Recording;

/// A single-shot loopback HTTP target. `hits` counts every ACCEPTED
/// connection, so a test can prove the kernel never dialled it at all;
/// `seen` keeps the request bytes, so a test can prove what did (and did
/// not) leave the machine.
struct Target {
    port: u16,
    hits: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<u8>>>,
}

/// What the target writes back once it has read a request head.
#[derive(Clone)]
enum Answer {
    /// Raw response bytes.
    Raw(String),
    /// A 200 whose body is `size` bytes, with a correct content-length.
    Body(usize),
    /// Accept, read, and never answer (the bound's probe).
    Silent,
}

fn target(answer: Answer) -> Target {
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

/// The 200 every happy-path target answers.
fn ok_body(body: &str) -> Answer {
    Answer::Raw(format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\n\r\n{body}",
        body.len()
    ))
}

struct Rig {
    ledger: Arc<Recording>,
    broker: Arc<Broker>,
    provider: Arc<HostNet>,
    guest: u64,
}

/// A rig whose guest holds exactly `outbound` as its allowlist.
fn rig(outbound: &[&str]) -> Rig {
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
        provider,
        guest,
    }
}

impl Rig {
    async fn request(
        &self,
        method: &str,
        url: &str,
        headers: &[(String, String)],
        body: &[u8],
    ) -> Result<(u16, Vec<(String, String)>, Vec<u8>), ErrorCode> {
        let payload = encode_request(method, url, headers, body);
        let answer = self
            .broker
            .dispatch(self.guest, NET_CONTRACT, "request", payload)
            .await
            .map_err(|error| error.code)?;
        decode_response(&answer).map_err(|error| error.code)
    }

    async fn get(&self, url: &str) -> Result<(u16, Vec<(String, String)>, Vec<u8>), ErrorCode> {
        self.request("GET", url, &[], &[]).await
    }

    /// Every `NetRequested` row this rig recorded.
    fn requested(&self) -> Vec<LedgerEventKind> {
        self.ledger
            .kinds()
            .into_iter()
            .map(|(kind, _)| kind)
            .filter(|kind| matches!(kind, LedgerEventKind::NetRequested { .. }))
            .collect()
    }

    fn scope_refusals(&self) -> usize {
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

/// The provided path: an allowed authority answers status, headers and
/// body, and lands exactly ONE record carrying the call's shape with the
/// caller's fiber attribution (Law 2).
#[tokio::test]
async fn an_allowed_authority_answers_and_lands_one_shaped_record() {
    let server = target(ok_body("pong"));
    let rig = rig(&[&format!("127.0.0.1:{}", server.port)]);
    let (status, headers, body) = rig
        .get(&format!("http://127.0.0.1:{}/probe", server.port))
        .await
        .unwrap_or_else(|code| panic!("request: {code:?}"));
    assert_eq!(status, 200);
    assert_eq!(body, b"pong");
    assert!(
        headers
            .iter()
            .any(|(name, value)| name == "content-type" && value == "text/plain"),
        "the response headers reach the caller: {headers:?}"
    );
    let rows = rig.requested();
    assert_eq!(rows.len(), 1, "exactly one record per call: {rows:?}");
    let LedgerEventKind::NetRequested {
        method,
        host,
        path,
        status: recorded,
        response_bytes,
        ..
    } = &rows[0]
    else {
        panic!("not a request row")
    };
    assert_eq!((method.as_str(), path.as_str()), ("GET", "/probe"));
    assert_eq!(host, &format!("127.0.0.1:{}", server.port));
    assert_eq!((*recorded, *response_bytes), (200, 4));
    let attributed = rig
        .ledger
        .kinds()
        .into_iter()
        .find(|(kind, _)| matches!(kind, LedgerEventKind::NetRequested { .. }))
        .and_then(|(_, fiber)| fiber);
    assert_eq!(attributed, Some(FiberId(7)), "attributed to the caller");
}

/// The allowlist is ACCEPTANCE, not preamble: an off-allowlist authority
/// refuses on the record and the kernel never dials it — and the SAME
/// request under a grant that admits it succeeds, so the refusal is not
/// vacuous.
#[tokio::test]
async fn an_off_allowlist_authority_refuses_without_ever_connecting() {
    let server = target(ok_body("pong"));
    let url = format!("http://127.0.0.1:{}/probe", server.port);
    let refused = rig(&["127.0.0.1:1"]);
    assert_eq!(refused.get(&url).await, Err(ErrorCode::EffectFailed));
    assert_eq!(server.hits.load(Ordering::SeqCst), 0, "never dialled");
    assert_eq!(refused.scope_refusals(), 1, "the refusal is on the record");
    assert!(refused.requested().is_empty(), "no record of a sent call");

    let allowed = rig(&[&format!("127.0.0.1:{}", server.port)]);
    assert!(allowed.get(&url).await.is_ok(), "the precondition holds");
    assert_eq!(server.hits.load(Ordering::SeqCst), 1);
}

/// A bare grant reaches nothing: the empty allowlist is default deny.
#[tokio::test]
async fn a_bare_grant_reaches_nothing() {
    let server = target(ok_body("pong"));
    let rig = rig(&[]);
    assert_eq!(
        rig.get(&format!("http://127.0.0.1:{}/probe", server.port))
            .await,
        Err(ErrorCode::EffectFailed)
    );
    assert_eq!(server.hits.load(Ordering::SeqCst), 0);
}

/// An entry admits its OWN authority and nothing beside it: a host alone
/// never means "every port" (Law 1, the M2-K8 hull ruling read for hosts).
#[tokio::test]
async fn an_entry_admits_its_own_authority_and_nothing_beside_it() {
    let server = target(ok_body("pong"));
    let rig = rig(&["127.0.0.1"]);
    assert_eq!(
        rig.get(&format!("http://127.0.0.1:{}/probe", server.port))
            .await,
        Err(ErrorCode::EffectFailed),
        "granting the host alone confers port 80, not this one"
    );
    assert_eq!(server.hits.load(Ordering::SeqCst), 0);
    // And the same spelling difference cuts the other way: `localhost` is
    // a different authority from `127.0.0.1`, never a silent alias.
    let aliased = rig_pair(server.port);
    assert_eq!(
        aliased
            .get(&format!("http://localhost:{}/probe", server.port))
            .await,
        Err(ErrorCode::EffectFailed)
    );
    assert_eq!(server.hits.load(Ordering::SeqCst), 0);
}

fn rig_pair(port: u16) -> Rig {
    rig(&[&format!("127.0.0.1:{port}")])
}

/// Three readings, three answers: off the allowlist is `denied`
/// (EffectFailed), a URL the provider cannot make sense of is `invalid`
/// (InvalidProfile) and never a grant event, and an authorized call the
/// network failed is `failed` (PluginFailed).
#[tokio::test]
async fn the_three_refusals_are_three_distinct_answers() {
    let rig = rig(&["127.0.0.1:1"]);
    for bad in [
        "not a url",
        "https://127.0.0.1:1/x",
        "http://",
        "http://user:pw@127.0.0.1:1/x",
        "http://127.0.0.1:notaport/x",
    ] {
        assert_eq!(
            rig.get(bad).await,
            Err(ErrorCode::InvalidProfile),
            "{bad} is invalid, not denied"
        );
    }
    assert_eq!(rig.scope_refusals(), 0, "a bad URL is not a grant refusal");
    // Authorized, and nothing is listening on port 1.
    assert_eq!(
        rig.get("http://127.0.0.1:1/x").await,
        Err(ErrorCode::PluginFailed),
        "a network failure is its own reading"
    );
}

/// A non-loopback target is refused as such even when the allowlist names
/// it: v0.2 reaches loopback only, and no resolver is ever consulted.
#[tokio::test]
async fn a_non_loopback_target_on_the_allowlist_is_still_refused() {
    let rig = rig(&["example.com:80", "203.0.113.7:80"]);
    for url in ["http://example.com/x", "http://203.0.113.7/x"] {
        assert_eq!(rig.get(url).await, Err(ErrorCode::EffectFailed), "{url}");
    }
    let reasons: Vec<RefusalReason> = rig
        .ledger
        .kinds()
        .into_iter()
        .filter_map(|(kind, _)| match kind {
            LedgerEventKind::GrantRefused { reason, .. } => Some(reason),
            _ => None,
        })
        .collect();
    assert_eq!(
        reasons,
        vec![RefusalReason::NotLoopback, RefusalReason::NotLoopback],
        "the record says WHY, and it is not a scope mismatch"
    );
}

/// The redirect hole, closed by construction: a 30x is ANSWERED, never
/// followed, so the kernel cannot make a second call the allowlist did not
/// admit — and the caller's own follow-up is authorized like any other.
#[tokio::test]
async fn a_redirect_off_the_allowlist_is_answered_never_followed() {
    let elsewhere = target(ok_body("secret"));
    let redirector = target(Answer::Raw(format!(
        "HTTP/1.1 302 Found\r\nlocation: http://127.0.0.1:{}/taken\r\ncontent-length: 0\r\n\r\n",
        elsewhere.port
    )));
    let rig = rig(&[&format!("127.0.0.1:{}", redirector.port)]);
    let (status, headers, _) = rig
        .get(&format!("http://127.0.0.1:{}/go", redirector.port))
        .await
        .unwrap_or_else(|code| panic!("request: {code:?}"));
    assert_eq!(status, 302, "the caller sees the redirect");
    assert!(
        headers.iter().any(|(name, _)| name == "location"),
        "and can read where it points: {headers:?}"
    );
    assert_eq!(
        elsewhere.hits.load(Ordering::SeqCst),
        0,
        "the kernel never dialled the redirect target"
    );
    assert_eq!(rig.requested().len(), 1, "one call, one record");
    // The caller's own follow-up crosses the allowlist like any other.
    assert_eq!(
        rig.get(&format!("http://127.0.0.1:{}/taken", elsewhere.port))
            .await,
        Err(ErrorCode::EffectFailed)
    );
    assert_eq!(elsewhere.hits.load(Ordering::SeqCst), 0);
}

/// Law 2 vs 02 §Redaction: the record carries the call's SHAPE. A
/// credential-bearing header and a credential-bearing query string reach
/// the target and NEVER the ledger.
#[tokio::test]
async fn no_credential_header_or_query_string_reaches_the_ledger() {
    const SECRET: &str = "sk-live-0xDEADBEEF-fixture-secret";
    let server = target(ok_body("pong"));
    let rig = rig(&[&format!("127.0.0.1:{}", server.port)]);
    let (status, _, _) = rig
        .request(
            "POST",
            &format!(
                "http://127.0.0.1:{}/v1/keys?access_token={SECRET}",
                server.port
            ),
            &[("authorization".to_owned(), format!("Bearer {SECRET}"))],
            SECRET.as_bytes(),
        )
        .await
        .unwrap_or_else(|code| panic!("request: {code:?}"));
    assert_eq!(status, 200);
    let sent = String::from_utf8_lossy(
        &server
            .seen
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone(),
    )
    .into_owned();
    assert!(
        sent.contains(&format!("Bearer {SECRET}")),
        "the credential DID reach the target: {sent}"
    );
    let recorded = format!("{:?}", rig.ledger.kinds());
    assert!(
        !recorded.contains(SECRET),
        "no ledger row carries the credential: {recorded}"
    );
    let rows = rig.requested();
    let LedgerEventKind::NetRequested { path, method, .. } = &rows[0] else {
        panic!("not a request row")
    };
    assert_eq!(
        (method.as_str(), path.as_str()),
        ("POST", "/v1/keys"),
        "the path stops at the query"
    );
}

/// A header a caller could smuggle a second request line through is
/// `invalid`, and so is one that would fight the kernel for the framing.
#[tokio::test]
async fn injected_and_framing_headers_are_invalid() {
    let server = target(ok_body("pong"));
    let rig = rig(&[&format!("127.0.0.1:{}", server.port)]);
    let url = format!("http://127.0.0.1:{}/probe", server.port);
    for (name, value) in [
        ("x-smuggle\r\nx-evil", "1"),
        ("x-smuggle", "1\r\nx-evil: 1"),
        ("content-length", "0"),
        ("connection", "keep-alive"),
        ("host", "elsewhere"),
    ] {
        assert_eq!(
            rig.request("GET", &url, &[(name.to_owned(), value.to_owned())], &[])
                .await,
            Err(ErrorCode::InvalidProfile),
            "{name}"
        );
    }
    assert_eq!(server.hits.load(Ordering::SeqCst), 0);
}

/// Bounded (R9): a response past the cap is a TYPED failure, never a
/// truncated body handed back as whole, and chunked framing is a named
/// typed failure rather than a silently mis-parsed body.
#[tokio::test]
async fn a_body_past_the_cap_and_chunked_framing_are_typed_failures() {
    let big = target(Answer::Body(super::request::BODY_CAP + 1));
    let capped = rig(&[&format!("127.0.0.1:{}", big.port)]);
    assert_eq!(
        capped
            .get(&format!("http://127.0.0.1:{}/big", big.port))
            .await,
        Err(ErrorCode::PluginFailed)
    );
    let chunked = target(Answer::Raw(
        "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n4\r\npong\r\n0\r\n\r\n".to_owned(),
    ));
    let rig = rig(&[&format!("127.0.0.1:{}", chunked.port)]);
    assert_eq!(
        rig.get(&format!("http://127.0.0.1:{}/x", chunked.port))
            .await,
        Err(ErrorCode::PluginFailed)
    );
    // Under the cap the same shape succeeds: the cap, not the plumbing.
    let fits = target(Answer::Body(1024));
    let ok = rig_pair(fits.port);
    let (status, _, body) = ok
        .get(&format!("http://127.0.0.1:{}/small", fits.port))
        .await
        .unwrap_or_else(|code| panic!("request: {code:?}"));
    assert_eq!((status, body.len()), (200, 1024));
}

/// A target that accepts and never answers cannot pin the caller: the
/// whole call is bounded, and the bound sits UNDER the guest deadline so
/// the guest is answered rather than killed (the M2-K12 lesson).
#[tokio::test(start_paused = true)]
async fn a_target_that_never_answers_is_bounded() {
    assert!(
        super::request::BOUND < crate::lane::DEADLINE,
        "the outbound bound must sit under the guest-call deadline"
    );
    let server = target(Answer::Silent);
    let rig = rig(&[&format!("127.0.0.1:{}", server.port)]);
    assert_eq!(
        rig.get(&format!("http://127.0.0.1:{}/hang", server.port))
            .await,
        Err(ErrorCode::PluginFailed),
        "the bound answers the caller"
    );
    let rows = rig.requested();
    assert_eq!(rows.len(), 1, "an authorized attempt is on the record");
    let LedgerEventKind::NetRequested { status, .. } = &rows[0] else {
        panic!("not a request row")
    };
    assert_eq!(*status, 0, "no status was read, and the row says so");
}

/// Every AUTHORIZED attempt registers an irreversible effect: the kernel
/// cannot know how much of a call reached its target, so it never claims
/// one is revertible. A refused call registers none.
#[tokio::test]
async fn every_authorized_attempt_registers_an_irreversible_effect() {
    let server = target(ok_body("pong"));
    let rig = rig(&[&format!("127.0.0.1:{}", server.port)]);
    assert!(rig.provider.requests().is_empty());
    assert_eq!(
        rig.get("http://127.0.0.1:1/nope").await,
        Err(ErrorCode::EffectFailed),
        "off the allowlist"
    );
    assert!(
        rig.provider.requests().is_empty(),
        "a refused call is not an effect"
    );
    rig.get(&format!("http://127.0.0.1:{}/probe", server.port))
        .await
        .unwrap_or_else(|code| panic!("request: {code:?}"));
    let effects = rig.provider.requests();
    assert_eq!(effects.len(), 1);
    assert!(
        effects[0].1.contains("GET") && effects[0].1.contains("/probe"),
        "the effect names the call it cannot undo: {:?}",
        effects[0]
    );
}
