//! The `jinn:net` outbound one-shots (M2-K14): the kernel's FIRST genuinely
//! irreversible effect (Law 3, constitution 03 §Irreversible).
//!
//! Two operations, ONE door. `request` is the 0.1.0 declaration finally
//! provided — body in, body out; `send-request` is the whole-response
//! shape added beside it in 0.2.0 (R12: additive within a major, never a
//! re-shaping). They differ only in how much of the call and the answer
//! the caller may see: same authority, same record, same effect class.
//!
//! Authority first, then reachability, then the wire. The order is the
//! point: an off-allowlist authority is refused before the kernel learns
//! anything about it — no connect, no resolution, no probe. Who may be
//! reached, initially and after a redirect, is `admit`; three readings
//! stay three answers (`denied` / `invalid` / `failed`, R3), because a
//! caller's next move differs for each.
//!
//! The effect is IRREVERSIBLE and that fact is DURABLE: the ledger row
//! carries the effect id, so a revert unit naming a sent request is
//! refused after a reopen exactly as in the process that sent it. There is
//! no second, in-memory register of outbound calls — one mutation
//! primitive, one record (R5).
//!
//! Nothing here blocks (R1): one tokio connect, write and read, the whole
//! call bounded UNDER the guest deadline so a slow target answers the
//! guest instead of killing its activation (the M2-K12 lesson). No lock is
//! held across an await.

use std::time::Instant;

use jinnd_api::{EffectId, KernelError, LedgerEventKind};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};

use super::HostNet;
use super::admit::Dial;
use super::http;
use super::tls;
use crate::hostwire::{decode_body_request, decode_request, encode_response};
use crate::peer::PeerId;

/// The whole call's bound. Deliberately under `lane::DEADLINE` (5s): a
/// guest calling from `activate` is charged this against its own deadline,
/// and an answered guest beats a killed one (R9, M2-K12).
pub(crate) const BOUND: Duration = Duration::from_secs(3);
/// The largest response body v0.2 will hold. Past it the answer is a typed
/// failure — never a truncated body handed back as whole.
pub(crate) const BODY_CAP: usize = 1024 * 1024;
/// The largest response head v0.2 will read.
const HEAD_CAP: usize = 64 * 1024;

/// One answered response: status, headers, body.
type Answered = (u16, Vec<(String, String)>, Vec<u8>);

impl HostNet {
    /// `request` (the 0.1.0 declaration, provided): one call carrying no
    /// caller header, answering the response BODY alone.
    ///
    /// # Errors
    ///
    /// As [`HostNet::outbound`].
    pub(super) async fn request(
        &self,
        caller: PeerId,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, KernelError> {
        let (method, url, body) = decode_body_request(&payload)?;
        let (_, _, body) = self.outbound(caller, method, url, Vec::new(), body).await?;
        Ok(body)
    }

    /// `send-request` (0.2.0, additive): one call carrying the caller's
    /// headers, answering the WHOLE response.
    ///
    /// # Errors
    ///
    /// As [`HostNet::outbound`].
    pub(super) async fn send_request(
        &self,
        caller: PeerId,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, KernelError> {
        let (method, url, headers, body) = decode_request(&payload)?;
        let (status, headers, body) = self.outbound(caller, method, url, headers, body).await?;
        Ok(encode_response(status, &headers, &body))
    }

    /// One outbound request: authorize, dial, send, read, record.
    ///
    /// # Errors
    ///
    /// `denied` off the allowlist or off loopback (ledgered), `invalid`
    /// for a URL or header this provider cannot make sense of, `failed`
    /// for an authorized call the network or the response failed.
    async fn outbound(
        &self,
        caller: PeerId,
        method: String,
        url: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<Answered, KernelError> {
        let target = http::parse(&url)?;
        let scope = self.outbound_scope(caller)?;
        let dial = self.admit(caller, &scope, &target, None)?;
        let head = http::head(&method, &target, &headers, &body)?;
        let fiber = self.core.attribution(caller);
        // The attempt is an irreversible effect the moment it is
        // authorized: the kernel cannot know how much of a call reached
        // its target, and over-claiming revertibility is the one failure
        // Law 3 exists to prevent.
        let effect = EffectId(self.core.mint());
        let started = Instant::now();
        let sent = (head.len() + body.len()) as u64;
        // The whole exchange — the TLS handshake included (M2-K15) — sits
        // inside the one bound, so a peer that stalls mid-handshake
        // answers the guest a typed failure instead of eating its deadline.
        let outcome = timeout(BOUND, exchange(dial, &target.authority, head, &body)).await;
        let answered = match outcome {
            Ok(answered) => answered,
            Err(_elapsed) => Err(http::failed(format!(
                "the target answered nothing within {BOUND:?}"
            ))),
        };
        let (status, response_bytes) = answered
            .as_ref()
            .map_or((0, 0), |(status, _, body)| (*status, body.len() as u64));
        // The row lands BEFORE any refusal of the answer: the call was
        // really sent, and a refusal is never a licence to forget it.
        self.core.sink.append(
            LedgerEventKind::NetRequested {
                effect,
                method,
                host: target.authority,
                path: target.path,
                status,
                request_bytes: sent,
                response_bytes,
                duration_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
            },
            fiber,
        );
        tracing::info!(effect = effect.0, status, "net request");
        let (status, headers, body) = answered?;
        self.admit_hop(caller, &scope, status, &headers)?;
        Ok((status, headers, body))
    }

    /// Seeds the provider's effect counter to `floor` so a fresh process
    /// never re-mints an id the durable record already spent (M2-K14): an
    /// outbound effect is irreversible forever, so its id must name one
    /// call forever. Called at boot from the ledger's own high-water mark.
    pub fn seed_effects(&self, floor: u64) {
        self.core.seed(floor);
    }
}

/// One exchange on one connection: dial the ADMITTED target, complete the
/// TLS handshake when the transport asks for one, then talk HTTP/1.1 over
/// whatever came back. The socket is dropped — closed — when this returns,
/// whatever the outcome.
///
/// The connection is dialled from a [`Dial`], which exists only on the far
/// side of the allowlist: there is no path here that reaches an address
/// admission never saw.
async fn exchange(
    dial: Dial,
    authority: &str,
    head: Vec<u8>,
    body: &[u8],
) -> Result<Answered, KernelError> {
    match dial {
        Dial::Plain(addr) => {
            let stream = TcpStream::connect(addr)
                .await
                .map_err(|error| http::failed(format!("connect {authority}: {error}")))?;
            talk(stream, authority, head, body).await
        }
        Dial::Tls(host, port) => {
            let stream = TcpStream::connect((host.as_str(), port))
                .await
                .map_err(|error| http::failed(format!("connect {authority}: {error}")))?;
            let stream = tls::connect(&host, authority, stream).await?;
            talk(stream, authority, head, body).await
        }
    }
}

/// One HTTP/1.1 exchange over an established stream, plaintext or TLS:
/// write the whole request, read the head, then exactly the body the head
/// declares (or to EOF).
async fn talk<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    authority: &str,
    head: Vec<u8>,
    body: &[u8],
) -> Result<Answered, KernelError> {
    let mut request = head;
    request.extend_from_slice(body);
    stream
        .write_all(&request)
        .await
        .map_err(|error| http::failed(format!("write {authority}: {error}")))?;
    let mut buffer = Vec::new();
    let (head, mut held) = loop {
        if let Ok(parsed) = http::response(&buffer) {
            break parsed;
        }
        if buffer.len() > HEAD_CAP {
            return Err(http::failed(format!(
                "the response head passed the {HEAD_CAP}-byte cap"
            )));
        }
        let mut chunk = [0u8; 8192];
        let read = stream
            .read(&mut chunk)
            .await
            .map_err(|error| http::failed(format!("read {authority}: {error}")))?;
        if read == 0 {
            // EOF with no complete head: let the parser name it.
            break http::response(&buffer)?;
        }
        buffer.extend_from_slice(&chunk[..read]);
    };
    let expected = head.expected()?;
    if expected.is_some_and(|length| length > BODY_CAP) {
        return Err(http::failed(format!(
            "the target declared a body past the {BODY_CAP}-byte cap"
        )));
    }
    while expected.is_none_or(|length| held.len() < length) {
        let mut chunk = [0u8; 8192];
        let read = stream
            .read(&mut chunk)
            .await
            .map_err(|error| http::failed(format!("read {authority}: {error}")))?;
        if read == 0 {
            break;
        }
        held.extend_from_slice(&chunk[..read]);
        if held.len() > BODY_CAP {
            return Err(http::failed(format!(
                "the response body passed the {BODY_CAP}-byte cap"
            )));
        }
    }
    held.truncate(expected.unwrap_or(held.len()));
    Ok((head.status, head.headers, held))
}
