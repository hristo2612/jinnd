//! Who an outbound call MAY reach (M2-K14): the caller's own allowlist,
//! v0.2's loopback limit, and the redirect that must cross both again.
//!
//! Deciding what a call IS (`http`) and deciding whether it MAY happen are
//! two jobs; this is the second, and it is the only door. Every refusal
//! here lands on the record with its typed class before the caller sees it
//! (Law 2, R3).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use jinnd_api::{KernelError, RefusalReason};

use super::HostNet;
use super::http::{self, Target};
use crate::grants::{GrantScope, NetScope};
use crate::peer::PeerId;

impl HostNet {
    /// The caller's typed outbound authority, or the ledgered refusal that
    /// it holds none.
    pub(super) fn outbound_scope(&self, caller: PeerId) -> Result<NetScope, KernelError> {
        match self.core.policy(caller) {
            Some(GrantScope::Net(scope)) => Ok(scope),
            _ => Err(self.core.refuse(
                caller,
                RefusalReason::NotGranted,
                "net caller holds no policy".to_owned(),
            )),
        }
    }

    /// The single admission point for an outbound authority: the caller's
    /// own allowlist first (a refusal here teaches the caller nothing
    /// about the target), then v0.2's loopback limit. Both land on the
    /// record with their typed class (Law 2, R3). `hop` names the redirect
    /// that produced this target, when one did.
    pub(super) fn admit(
        &self,
        caller: PeerId,
        scope: &NetScope,
        target: &Target,
        hop: Option<u16>,
    ) -> Result<SocketAddr, KernelError> {
        let via = hop.map_or(String::new(), |status| {
            format!("the target answered {status} redirecting to ")
        });
        if !scope.admits_authority(&target.authority) {
            return Err(self.core.refuse(
                caller,
                RefusalReason::ScopeMismatch,
                format!(
                    "net request refused: {via}{} is not on the granted outbound allowlist",
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
                        "net request refused: {via}{} is not a loopback target (v0.2 reaches loopback only; TLS and real hosts are M2-K15)",
                        target.authority
                    ),
                ));
            }
        };
        Ok(SocketAddr::new(ip, target.port))
    }

    /// A `30x` is never followed — and never handed back unless the hop it
    /// names is one this caller could have made itself.
    ///
    /// Answering the redirect and trusting the caller not to take it would
    /// move the allowlist boundary out of the kernel and into the guarded
    /// party, and an authority the guarded party enforces is not an
    /// authority at all (M2-K14 round 2). So the hop crosses the SAME
    /// admission the initial target crossed. Fail closed: a `location`
    /// this provider cannot parse cannot be proven admitted, so it is
    /// refused too. A `30x` naming no `location`, or a relative one, names
    /// no new authority and is answered — the only authority in play is
    /// the one already admitted.
    ///
    /// # Errors
    ///
    /// The ledgered `denied` naming the authority it would not hand back.
    pub(super) fn admit_hop(
        &self,
        caller: PeerId,
        scope: &NetScope,
        status: u16,
        headers: &[(String, String)],
    ) -> Result<(), KernelError> {
        if !(300..400).contains(&status) {
            return Ok(());
        }
        let Some(location) = headers
            .iter()
            .find(|(name, _)| name == "location")
            .map(|(_, value)| value.as_str())
        else {
            return Ok(());
        };
        if !location.contains("://") {
            return Ok(());
        }
        match http::parse(location) {
            Ok(hop) => self.admit(caller, scope, &hop, Some(status)).map(|_| ()),
            Err(_) => Err(self.core.refuse(
                caller,
                RefusalReason::ScopeMismatch,
                format!(
                    "net request refused: the target answered {status} redirecting to {location:?}, which this provider cannot read and therefore cannot prove is on the granted outbound allowlist"
                ),
            )),
        }
    }
}
