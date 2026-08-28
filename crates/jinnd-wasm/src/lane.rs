//! The production wasm package lane (M2-K1, lifted from the daemon per the
//! PLA-283 ruling: the right home for production host machinery is the
//! production host crate). One [`LaneCore`] per assembly — ONE broker, one
//! topic registry, one host, one loom-modeled swap phase machine (R7;
//! decision log 2026-08-25) — one [`WasmBody`] per entry behind the fiber
//! engine's body seam, and one [`wasm_lane`] builder over the loader's
//! package-lane seam. What stays with the assembly is policy, behind
//! explicit seams: how spawned fibers are tracked (`track`), how an entry's
//! config decodes to a [`SeatSpec`] (`decode`), and whether guest
//! registrations land a Law-2 ledger trail (`guest_trail` — the daemon's
//! obligation; the harness lane keeps its own pinned observables).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use jinnd_api::{EntryId, FiberId, KernelError, KernelFuture, LedgerEventKind};
use jinnd_context::Context;
use jinnd_effects::Disposer;
use jinnd_fiber::{Fiber, FiberBody, Setup, WatchReadiness};
use jinnd_loader::host::{LaneHandle, Rebind, config_of};
use jinnd_loader::{PackageLane, SpawnRequest};

use crate::broker::Broker;
use crate::handle::Registration;
use crate::host::{LoadedComponent, WasmHost};
use crate::instance::Seat;
use crate::peer::{LedgerSink, PeerId};
use crate::selector::NoRealms;
use crate::slot::{SeatState, SharedSlot};
use crate::swap::SwapCore;
use crate::topics::LocalTopics;

/// The guest-call deadline (R11 containment horizon).
pub(crate) const DEADLINE: Duration = Duration::from_secs(5);

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

/// One entry's seat configuration: `grants` are the contract names the
/// profile side grants the instance (constitution 01: requests are not
/// grants), `payload` is the opaque payload handed to the guest's `activate`
/// (R9: data, never behavior).
pub struct SeatSpec {
    pub grants: Vec<String>,
    pub payload: Vec<u8>,
}

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

/// Host-held wasm-lane state: ONE broker, one topic registry, one host, one
/// loom-modeled swap phase machine (R7). The assembly owns the ledger sink
/// every broker crossing lands on (R6).
pub struct LaneCore {
    pub broker: Arc<Broker>,
    pub topics: Arc<LocalTopics>,
    pub host: WasmHost,
    pub sink: Arc<dyn LedgerSink>,
    pub packages: Mutex<HashMap<String, Arc<Mutex<LoadedComponent>>>>,
    pub(crate) roster: Mutex<HashMap<EntryId, Roster>>,
    pub swap: SwapCore,
    next_slot: AtomicU64,
}

impl LaneCore {
    /// Assembles the lane state over the assembly's ledger sink.
    ///
    /// # Errors
    ///
    /// Wasm engine construction failures.
    pub fn new(sink: Arc<dyn LedgerSink>) -> Result<Self, KernelError> {
        Ok(Self {
            broker: Arc::new(Broker::new(Arc::clone(&sink))),
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

/// One wasm entry behind the fiber engine's body seam. Its instance lives
/// in a [`SharedSlot`] seat — instance PAIRED with its own registrations —
/// so Mode-1 swap replaces it whole without touching the fiber, and
/// teardown withdraws exactly the current instance's contribution with the
/// tokens that instance minted (I1, R5; round-2 blocker-4).
pub struct WasmBody {
    core: Arc<LaneCore>,
    entry: EntryId,
    component: Arc<Mutex<LoadedComponent>>,
    seat: Mutex<SeatSpec>,
    at: Mutex<Context<()>>,
    slot: Arc<SharedSlot>,
    /// Law 2: with the trail on, guest effect registrations and withdrawals
    /// are ledger events (the daemon's obligation).
    guest_trail: bool,
}

impl WasmBody {
    /// States a new seat for the next activation to read.
    pub fn restate_seat(&self, seat: SeatSpec) {
        *lock(&self.seat) = seat;
    }
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
            let core = Arc::clone(&self.core);
            let peer = core.broker.register_peer(Some(fiber));
            for contract in &grants {
                core.broker.grant(peer, contract);
            }
            let slot_id = core.next_slot.fetch_add(1, Ordering::SeqCst) + 1;
            let component = lock(&self.component).clone();
            let handle = core.host.instantiate(
                &component,
                Seat {
                    broker: Arc::clone(&core.broker),
                    topics: Arc::clone(&core.topics),
                    oracle: Arc::new(NoRealms),
                    peer,
                    fiber: Some(fiber),
                    context: at.id().0,
                    deadline: DEADLINE,
                    slot: Some(Arc::clone(&self.slot)),
                    staging: false,
                },
            );
            lock(&core.roster).insert(
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
            // swap slot FIRST (loom-modeled arbitration — a racing claim
            // refuses and discards), then retire the live seat exactly
            // (I1, R5).
            let trail = self.guest_trail;
            let (slot, entry, owner) = (
                Arc::clone(&self.slot),
                self.entry.clone(),
                Arc::clone(&core),
            );
            setup.effect(
                "wasm guest seat",
                Disposer::future(move || async move {
                    owner.swap.dispose(slot_id);
                    // With the trail on, the seat retires ledgered: every
                    // effect and listener withdrawal is a ledger event.
                    let ledger = trail.then_some((owner.sink.as_ref(), fiber));
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
            // the seat, success or failure alike — a failing activation
            // still owes its inverses (I1). With the trail on, each landed
            // registration is a ledger event (Law 2).
            let (outcome, contributed) = handle.activate(config).await;
            if self.guest_trail {
                for registration in &contributed.registrations {
                    let label = match registration {
                        Registration::Effect { label, .. } => label.clone(),
                        Registration::Listen(listen) => format!("listen {}", listen.topic),
                        // The broker ledgered the provide crossing itself
                        // (R6).
                        Registration::Provision { .. } => continue,
                    };
                    core.sink
                        .append(LedgerEventKind::EffectRegistered { label }, Some(fiber));
                }
            }
            if let Some(previous) = self.slot.install(SeatState::live(handle, contributed)) {
                previous.instance.dispose().await;
            }
            outcome
        })
    }
}

/// The package lane for one wasm package over `core`: entries spawn a
/// [`WasmBody`] fiber over the package's pinned component cell; a config
/// edit restates the seat through `decode` (the next activation reads the
/// new grants and payload). `track` is the assembly's fiber-tracking seam:
/// it spawns the body — [`Fiber::spawn`] gated on the request's signal —
/// and records the fiber wherever the assembly answers for it.
pub fn wasm_lane<C, D>(
    core: Arc<LaneCore>,
    component: Arc<Mutex<LoadedComponent>>,
    guest_trail: bool,
    decode: D,
    track: impl Fn(Arc<WasmBody>, WatchReadiness) -> Arc<Fiber> + Send + Sync + 'static,
) -> PackageLane
where
    C: Clone + 'static,
    D: Fn(&C) -> SeatSpec + Clone + Send + Sync + 'static,
{
    PackageLane {
        injects: Vec::new(),
        provides: None,
        spawn: Box::new(move |request: SpawnRequest<'_>| {
            let config = config_of::<C>(request.config)?;
            let body = Arc::new(WasmBody {
                core: Arc::clone(&core),
                entry: request.entry.clone(),
                component: Arc::clone(&component),
                seat: Mutex::new(decode(&config)),
                at: Mutex::new(request.at.clone()),
                slot: Arc::new(SharedSlot::default()),
                guest_trail,
            });
            let fiber = track(Arc::clone(&body), request.signal);
            let decode = decode.clone();
            let restate = move |body: &WasmBody, config: C| {
                body.restate_seat(decode(&config));
                Ok(())
            };
            Ok(Arc::new(LaneHandle::new(fiber, body, restate)))
        }),
    }
}
