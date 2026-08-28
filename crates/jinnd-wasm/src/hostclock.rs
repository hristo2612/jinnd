//! The `jinn:clock` read provider (M2-K2; R7): a native peer behind the
//! SAME broker choke point every guest crosses — grant check → ledger
//! append → dispatch — so `now` is granted, and its only trace obligation
//! is the call ledger line the broker already writes. The alarm machinery
//! lives in `alarms.rs`; this peer answers the read surface only. Contract
//! bundle: `contracts/jinn-clock` (constitution 01).

use std::sync::Arc;

use jinnd_api::{ErrorCode, KernelError, KernelFuture};

use crate::alarms::{CLOCK_CONTRACT, now_unix_ms};
use crate::broker::Broker;
use crate::broker_state::refusal;
use crate::peer::{Peer, PeerId};

/// The `jinn:clock` provider: one stateless read surface.
pub struct HostClock;

impl HostClock {
    /// Registers the provider as a broker peer holding and providing the
    /// `jinn:clock` contract (providing is authority: the provider peer is
    /// granted what it provides).
    ///
    /// # Errors
    ///
    /// The broker's refusal of the provision.
    pub fn register(broker: &Broker) -> Result<(), KernelError> {
        let peer = broker.register_peer(None);
        broker.grant(peer, CLOCK_CONTRACT);
        broker.provide(peer, CLOCK_CONTRACT, Arc::new(ClockPeer))
    }
}

struct ClockPeer;

impl Peer for ClockPeer {
    fn call(
        &self,
        _caller: PeerId,
        _contract: &str,
        operation: &str,
        _payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let operation = operation.to_owned();
        Box::pin(async move {
            match operation.as_str() {
                // 8-byte LE milliseconds since the Unix epoch, per the
                // contract wire (wit/plugin.wit `interface clock`).
                "now" => Ok(now_unix_ms().to_le_bytes().to_vec()),
                other => Err(refusal(
                    ErrorCode::PluginFailed,
                    format!("unknown clock operation {other:?}"),
                )),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn now_answers_the_epoch_milliseconds_wire() {
        let answer = ClockPeer
            .call(0, CLOCK_CONTRACT, "now", Vec::new())
            .await
            .unwrap_or_else(|error| panic!("now answers: {error:?}"));
        assert_eq!(answer.len(), 8);
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&answer);
        let reading = u64::from_le_bytes(bytes);
        // The epoch-millisecond domain: after 2020, before 2100.
        assert!(reading > 1_577_836_800_000, "reads a real clock");
        assert!(reading < 4_102_444_800_000, "in milliseconds, not seconds");
    }

    #[tokio::test]
    async fn an_unknown_operation_is_refused() {
        assert!(
            ClockPeer
                .call(0, CLOCK_CONTRACT, "warp", Vec::new())
                .await
                .is_err()
        );
    }
}
