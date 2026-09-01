//! The `jinn:net` outbound one-shot (M2-K14): the kernel's FIRST genuinely
//! irreversible effect (Law 3, constitution 03 §Irreversible).
//!
//! Authority first, then reachability, then the wire. The order is the
//! point: an off-allowlist authority is refused before the kernel learns
//! anything about it — no connect, no resolution, no probe. Three readings
//! stay three answers (`denied` / `invalid` / `failed`, R3), because a
//! caller's next move differs for each.
//!
//! A `30x` is ANSWERED, never followed. That closes the redirect hole by
//! construction rather than by re-checking: the provider makes exactly one
//! call, so it can never make a second the allowlist did not admit.
//!
//! Nothing here blocks (R1): one tokio connect, write and read, the whole
//! call bounded UNDER the guest deadline so a slow target answers the
//! guest instead of killing its activation (the M2-K12 lesson). No lock is
//! held across an await.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Instant;

use jinnd_api::{EffectId, KernelError, LedgerEventKind, RefusalReason};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};

use super::HostNet;
use super::http::{self, Target};
use crate::grants::GrantScope;
use crate::hostwire::{decode_request, encode_response};
use crate::lane::lock;
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

impl HostNet {
    /// One outbound request: authorize, dial, send, read, record.
    ///
    /// # Errors
    ///
    /// `denied` off the allowlist or off loopback (ledgered), `invalid`
    /// for a URL or header this provider cannot make sense of, `failed`
    /// for an authorized call the network or the response failed.
    pub(super) async fn request(
        &self,
        caller: PeerId,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, KernelError> {
        let (method, url, headers, body) = decode_request(&payload)?;
        let target = http::parse(&url)?;
        let addr = self.authorize_outbound(caller, &target)?;
        let head = http::head(&method, &target, &headers, &body)?;
        let fiber = self.core.attribution(caller);
        // The attempt is an irreversible effect the moment it is
        // authorized: the kernel cannot know how much of a call reached
        // its target, and over-claiming revertibility is the one failure
        // Law 3 exists to prevent.
        let effect = self.record_request(&method, &target, fiber);
        let started = Instant::now();
        let sent = (head.len() + body.len()) as u64;
        let outcome = timeout(BOUND, exchange(addr, head, &body)).await;
        let answered = match outcome {
            Ok(answered) => answered,
            Err(_elapsed) => Err(http::failed(format!(
                "the target answered nothing within {BOUND:?}"
            ))),
        };
        let (status, response_bytes) = answered
            .as_ref()
            .map_or((0, 0), |(status, _, body)| (*status, body.len() as u64));
        self.core.sink.append(
            LedgerEventKind::NetRequested {
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
        Ok(encode_response(status, &headers, &body))
    }

    /// The single admission point for an outbound call: the caller's own
    /// allowlist first (a refusal here teaches the caller nothing about
    /// the target), then v0.2's loopback limit. Both land on the record
    /// with their typed class (Law 2, R3).
    fn authorize_outbound(
        &self,
        caller: PeerId,
        target: &Target,
    ) -> Result<SocketAddr, KernelError> {
        let Some(GrantScope::Net(scope)) = self.core.policy(caller) else {
            return Err(self.core.refuse(
                caller,
                RefusalReason::NotGranted,
                "net caller holds no policy".to_owned(),
            ));
        };
        if !scope.admits_authority(&target.authority) {
            return Err(self.core.refuse(
                caller,
                RefusalReason::ScopeMismatch,
                format!(
                    "net request refused: {} is not on the granted outbound allowlist",
                    target.authority
                ),
            ));
        }
        // No resolver is consulted (R9: name resolution is ambient
        // authority, and a name that resolves off-loopback is exactly the
        // hole the allowlist exists to close). Literal loopback only.
        let ip = match target.host.parse::<IpAddr>() {
            Ok(ip) if ip.is_loopback() => ip,
            Err(_) if target.host == "localhost" => IpAddr::V4(Ipv4Addr::LOCALHOST),
            _ => {
                return Err(self.core.refuse(
                    caller,
                    RefusalReason::NotLoopback,
                    format!(
                        "net request refused: {} is not a loopback target (v0.2 reaches loopback only; TLS and real hosts are M2-K15)",
                        target.authority
                    ),
                ));
            }
        };
        Ok(SocketAddr::new(ip, target.port))
    }

    /// Registers one irreversible effect under the provider's own handle
    /// counter, so a request effect can never collide with a socket.
    fn record_request(
        &self,
        method: &str,
        target: &Target,
        fiber: Option<jinnd_api::FiberId>,
    ) -> EffectId {
        let effect = self.core.mint();
        let label = format!(
            "jinn:net request {method} {}{} [effect {effect}]",
            target.authority, target.path
        );
        lock(&self.requests).insert(effect, (label, fiber));
        EffectId(effect)
    }

    /// Every outbound call this provider has made, in id order: the
    /// irreversible effects a revert unit may name and must be refused
    /// for (Law 3).
    #[must_use]
    pub fn requests(&self) -> Vec<(EffectId, String)> {
        lock(&self.requests)
            .iter()
            .map(|(effect, (label, _))| (EffectId(*effect), label.clone()))
            .collect()
    }
}

/// One exchange on one connection: dial, write the whole request, read the
/// head, then exactly the body the head declares (or to EOF). The socket
/// is dropped — closed — when this returns, whatever the outcome.
async fn exchange(
    addr: SocketAddr,
    head: Vec<u8>,
    body: &[u8],
) -> Result<(u16, Vec<(String, String)>, Vec<u8>), KernelError> {
    let mut stream = TcpStream::connect(addr)
        .await
        .map_err(|error| http::failed(format!("connect {addr}: {error}")))?;
    let mut request = head;
    request.extend_from_slice(body);
    stream
        .write_all(&request)
        .await
        .map_err(|error| http::failed(format!("write {addr}: {error}")))?;
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
            .map_err(|error| http::failed(format!("read {addr}: {error}")))?;
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
            .map_err(|error| http::failed(format!("read {addr}: {error}")))?;
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
