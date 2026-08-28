//! The daemon's wasm package lane: a profile entry naming a wasm package
//! instantiates the pinned artifact behind the one broker choke point
//! (R6, R7). Same shape as the harness lane it mirrors — one seat per fiber
//! in a [`SharedSlot`], swappable whole (R8) — but assembled from the
//! production crates only, and with the daemon's Law-2 obligation carried
//! here: guest effect registrations and withdrawals are ledger events.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use jinnd_api::{EntryId, FiberId, KernelError, KernelFuture, LedgerEventKind};
use jinnd_context::Context;
use jinnd_effects::Disposer;
use jinnd_fiber::{Fiber, FiberBody, Setup};
use jinnd_loader::host::{LaneHandle, Rebind, config_of};
use jinnd_loader::{PackageLane, SpawnRequest};
use jinnd_wasm::{
    Broker, LedgerSink, LoadedComponent, LocalTopics, NoRealms, PeerId, Registration, Seat,
    SeatState, SharedSlot, SwapCore, WasmHost,
};

use crate::seat::{SeatConfig, seat_config};
use crate::support::{DEADLINE, SharedFibers, Sink, Tracked, lock};

/// One live wasm entry, addressable by the swap machine.
pub(crate) struct Roster {
    pub(crate) slot: Arc<SharedSlot>,
    /// This activation's [`SwapCore`] key — never reused.
    pub(crate) slot_id: u64,
    pub(crate) peer: PeerId,
    pub(crate) fiber: FiberId,
    pub(crate) context: u64,
    pub(crate) config: Vec<u8>,
    pub(crate) component: Arc<Mutex<LoadedComponent>>,
}

/// Daemon-held wasm-lane state: ONE broker, one topic registry, one host,
/// one loom-modeled swap phase machine (R7; decision log 2026-08-25).
pub(crate) struct LaneState {
    pub(crate) broker: Arc<Broker>,
    pub(crate) topics: Arc<LocalTopics>,
    pub(crate) host: WasmHost,
    pub(crate) sink: Arc<Sink>,
    pub(crate) packages: Mutex<HashMap<String, Arc<Mutex<LoadedComponent>>>>,
    pub(crate) roster: Mutex<HashMap<EntryId, Roster>>,
    pub(crate) swap: SwapCore,
    next_slot: AtomicU64,
}

impl LaneState {
    pub(crate) fn new(ledger: jinnd_ledger::Ledger) -> Result<Self, KernelError> {
        let sink = Arc::new(Sink(ledger));
        Ok(Self {
            broker: Arc::new(Broker::new(Arc::clone(&sink) as Arc<dyn LedgerSink>)),
            topics: Arc::new(LocalTopics::default()),
            host: WasmHost::new()?,
            sink,
            packages: Mutex::new(HashMap::new()),
            roster: Mutex::new(HashMap::new()),
            swap: SwapCore::default(),
            next_slot: AtomicU64::new(0),
        })
    }
}

/// One wasm entry behind the fiber engine's body seam (mirrors the harness
/// lane's shape; round-2 blocker-4: the seat pairs the instance with its own
/// registrations).
struct WasmBody {
    state: Arc<LaneState>,
    entry: EntryId,
    component: Arc<Mutex<LoadedComponent>>,
    seat: Mutex<SeatConfig>,
    at: Mutex<Context<()>>,
    slot: Arc<SharedSlot>,
}

impl Rebind for WasmBody {
    fn rebind(&self, at: Context<()>) {
        *lock(&self.at) = at;
    }
}

impl FiberBody for WasmBody {
    fn activate<'a>(&'a self, mut setup: Setup<'a>) -> KernelFuture<'a, ()> {
        Box::pin(async move {
            let (grants, config) = {
                let seat = lock(&self.seat);
                (seat.grants.clone(), seat.payload.clone())
            };
            let at = lock(&self.at).clone();
            let fiber = setup.fiber();
            let state = Arc::clone(&self.state);
            let peer = state.broker.register_peer(Some(fiber));
            for contract in &grants {
                state.broker.grant(peer, contract);
            }
            let slot_id = state.next_slot.fetch_add(1, Ordering::SeqCst) + 1;
            let component = lock(&self.component).clone();
            let handle = state.host.instantiate(
                &component,
                Seat {
                    broker: Arc::clone(&state.broker),
                    topics: Arc::clone(&state.topics),
                    oracle: Arc::new(NoRealms),
                    peer,
                    fiber: Some(fiber),
                    context: at.id().0,
                    deadline: DEADLINE,
                    slot: Some(Arc::clone(&self.slot)),
                    staging: false,
                },
            );
            lock(&state.roster).insert(
                self.entry.clone(),
                Roster {
                    slot: Arc::clone(&self.slot),
                    slot_id,
                    peer,
                    fiber,
                    context: at.id().0,
                    config: config.clone(),
                    component: Arc::clone(&self.component),
                },
            );
            // ONE effect owns the whole guest contribution: tombstone the
            // swap slot FIRST, then retire the live seat exactly (I1, R5).
            let (slot, entry, owner) = (
                Arc::clone(&self.slot),
                self.entry.clone(),
                Arc::clone(&state),
            );
            setup.effect(
                "wasm guest seat",
                Disposer::future(move || async move {
                    owner.swap.dispose(slot_id);
                    // The seat retires with the daemon's Law-2 trail: every
                    // effect and listener withdrawal is a ledger event.
                    let ledger = Some((owner.sink.as_ref() as &dyn LedgerSink, fiber));
                    let retired = match slot.take() {
                        Some(seat) => {
                            seat.retire(&owner.broker, &owner.topics, peer, ledger)
                                .await
                        }
                        None => Ok(()),
                    };
                    owner.broker.remove_peer(peer);
                    lock(&owner.roster).remove(&entry);
                    retired
                }),
            )?;
            // The body runs once per fiber; its contribution commits into
            // the seat, success or failure alike (I1) — and each landed
            // registration is a ledger event (Law 2).
            let (outcome, contributed) = handle.activate(config).await;
            for registration in &contributed.registrations {
                let label = match registration {
                    Registration::Effect { label, .. } => label.clone(),
                    Registration::Listen(listen) => format!("listen {}", listen.topic),
                    // The broker ledgered the provide crossing itself (R6).
                    Registration::Provision { .. } => continue,
                };
                state
                    .sink
                    .append(LedgerEventKind::EffectRegistered { label }, Some(fiber));
            }
            if let Some(previous) = self.slot.install(SeatState::live(handle, contributed)) {
                previous.instance.dispose().await;
            }
            outcome
        })
    }
}

/// The package lane for one wasm package: entries spawn a [`WasmBody`] fiber
/// over the package's pinned component cell; config edits restate the seat
/// (the next activation reads the new grants and payload).
pub(crate) fn lane(
    state: Arc<LaneState>,
    fibers: SharedFibers,
    component: Arc<Mutex<LoadedComponent>>,
) -> PackageLane {
    PackageLane {
        injects: Vec::new(),
        provides: None,
        spawn: Box::new(move |request: SpawnRequest<'_>| {
            let config = config_of::<serde_json::Value>(request.config)?;
            let body = Arc::new(WasmBody {
                state: Arc::clone(&state),
                entry: request.entry.clone(),
                component: Arc::clone(&component),
                seat: Mutex::new(seat_config(&config)),
                at: Mutex::new(request.at.clone()),
                slot: Arc::new(SharedSlot::default()),
            });
            let fiber = Arc::new(Fiber::spawn(
                Arc::clone(&body) as Arc<dyn FiberBody>,
                request.signal,
            ));
            lock(&fibers).insert(
                fiber.id(),
                Tracked {
                    fiber: Arc::clone(&fiber),
                    recorded: 0,
                },
            );
            let restate = |body: &WasmBody, config: serde_json::Value| {
                *lock(&body.seat) = seat_config(&config);
                Ok(())
            };
            Ok(Arc::new(LaneHandle::new(fiber, body, restate)))
        }),
    }
}
