//! The `jinn:introspect` provider (M2-K7, harness #19; contract bundle
//! `contracts/jinn-introspect`): the composition as the kernel knows it —
//! every entry's fiber, state, incarnation, provisions, and kernel
//! registrations — plus the daemon's readiness. Read-only; granted like
//! any contract; every read is a ledgered contract call (the broker's
//! line); every answer is a SNAPSHOT of kernel-owned state taken under
//! brief locks — no guest is ever called on this path (R1).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use jinnd_api::{EntryId, KernelError, KernelFuture};
use jinnd_loader::Loader;
use jinnd_wasm::{Broker, INTROSPECT_CONTRACT, LaneCore, Peer, PeerId};

use super::Readiness;
use super::wire::{json, unknown};

/// The provider: the loader (entries, fibers, states), the lane (seats),
/// and the readiness flags.
pub(crate) struct HostIntrospect {
    loader: Arc<Loader>,
    lane: Arc<LaneCore>,
    readiness: Arc<Readiness>,
}

impl HostIntrospect {
    /// Registers the provider as a broker peer holding and providing the
    /// contract (providing is authority).
    ///
    /// # Errors
    ///
    /// The broker's refusal of the provision.
    pub(crate) fn register(
        broker: &Arc<Broker>,
        loader: Arc<Loader>,
        lane: Arc<LaneCore>,
        readiness: Arc<Readiness>,
    ) -> Result<(), KernelError> {
        let peer = broker.register_peer(None);
        broker.grant(peer, INTROSPECT_CONTRACT);
        broker.provide(
            peer,
            INTROSPECT_CONTRACT,
            Arc::new(Self {
                loader,
                lane,
                readiness,
            }),
        )
    }

    /// The bundle's `entry` record per committed profile entry, in
    /// document order.
    fn entries(&self) -> serde_json::Value {
        let ids: Vec<EntryId> = self
            .loader
            .persisted::<serde_json::Value>()
            .map(|profile| profile.entries.into_iter().map(|entry| entry.id).collect())
            .unwrap_or_default();
        let entries: Vec<serde_json::Value> = ids
            .into_iter()
            .map(|id| {
                let fiber = self.loader.entry_fiber(&id);
                let state = fiber
                    .and_then(|fiber| self.loader.fiber_state(fiber))
                    .map(|state| format!("{state:?}").to_lowercase());
                let (incarnation, seat) = self
                    .lane
                    .seat_summary(&id)
                    .map_or((None, None), |(incarnation, seat)| {
                        (Some(incarnation), Some(seat))
                    });
                let seat = seat.unwrap_or_default();
                serde_json::json!({
                    "id": id.0,
                    "fiber": fiber.map(|fiber| fiber.0),
                    "state": state,
                    "incarnation": incarnation,
                    "provisions": seat.provisions,
                    "registrations": {
                        "listeners": seat.listeners,
                        "alarms": seat.alarms,
                        "sockets": seat.sockets,
                        "processes": seat.processes,
                    },
                })
            })
            .collect();
        serde_json::Value::Array(entries)
    }

    fn readiness(&self) -> serde_json::Value {
        serde_json::json!({
            "boot-reconciled": self.readiness.boot_reconciled.load(Ordering::SeqCst),
            "watcher-armed": self.readiness.watcher_armed.load(Ordering::SeqCst),
        })
    }
}

impl Peer for HostIntrospect {
    fn call(
        &self,
        _caller: PeerId,
        _contract: &str,
        operation: &str,
        _payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let answer = match operation {
            "entries" => Ok(json(&self.entries())),
            "readiness" => Ok(json(&self.readiness())),
            other => Err(unknown(INTROSPECT_CONTRACT, other)),
        };
        Box::pin(async move { answer })
    }
}
