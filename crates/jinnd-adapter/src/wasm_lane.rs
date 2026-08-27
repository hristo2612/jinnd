//! The wasm-backed package lane and the [`WasmLane`] facade impl (authorized
//! M1-P8 adapter delta): a profile entry naming a wasm package instantiates
//! the pinned artifact behind the SAME broker the harness lane calls — one
//! choke point, two transports (decision log 2026-08-25; R6, R7).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jinnd_api::{
    EffectId, EntryId, FiberId, KernelError, KernelFuture, LedgerEventKind, SwapReport,
    WasmArtifact, WasmLane,
};
use jinnd_context::Context;
use jinnd_effects::Disposer;
use jinnd_fiber::{FiberBody, Setup};
use jinnd_loader::PackageLane;
use jinnd_loader::host::{Rebind, config_of};
use jinnd_wasm::{
    Broker, LedgerSink, LoadedComponent, LocalTopics, NoRealms, PeerId, Seat, SeatState,
    SharedSlot, SwapCore, WasmHost, swap_batch,
};

use crate::{Adapter, KERNEL_SCOPE, lock};

mod wasm_swap;
use wasm_swap::LaneSlots;

/// The harness lane's guest-call deadline (R11 containment horizon).
const DEADLINE: Duration = Duration::from_secs(5);

/// Broker crossings land on the kernel ledger's ordered record lane (R6).
struct Sink(jinnd_ledger::Ledger);

impl LedgerSink for Sink {
    fn append(&self, kind: LedgerEventKind, fiber: Option<FiberId>) {
        self.0.record(kind, None, fiber);
    }
}

/// One live wasm entry, addressable by the swap machine.
struct Roster {
    slot: Arc<SharedSlot>,
    /// This activation's key in the shared [`SwapCore`] — never reused.
    slot_id: u64,
    peer: PeerId,
    fiber: FiberId,
    context: u64,
    config: Vec<u8>,
    component: Arc<Mutex<LoadedComponent>>,
}

/// Adapter-held wasm-lane state: ONE broker, one topic registry, one host,
/// one swap phase machine (the loom-modeled [`SwapCore`] IS the production
/// path — round-2 blocker-3).
pub(crate) struct WasmState {
    broker: Arc<Broker>,
    topics: Arc<LocalTopics>,
    host: WasmHost,
    sink: Arc<Sink>,
    packages: Mutex<HashMap<String, Arc<Mutex<LoadedComponent>>>>,
    roster: Mutex<HashMap<EntryId, Roster>>,
    harness: Mutex<Option<PeerId>>,
    swap: SwapCore,
    next_slot: AtomicU64,
}

impl WasmState {
    pub(crate) fn new(ledger: jinnd_ledger::Ledger) -> Result<Self, KernelError> {
        let sink = Arc::new(Sink(ledger));
        Ok(Self {
            broker: Arc::new(Broker::new(Arc::clone(&sink) as Arc<dyn LedgerSink>)),
            topics: Arc::new(LocalTopics::default()),
            host: WasmHost::new()?,
            sink,
            packages: Mutex::new(HashMap::new()),
            roster: Mutex::new(HashMap::new()),
            harness: Mutex::new(None),
            swap: SwapCore::default(),
            next_slot: AtomicU64::new(0),
        })
    }

    fn harness_peer(&self) -> PeerId {
        *lock(&self.harness).get_or_insert_with(|| self.broker.register_peer(Some(KERNEL_SCOPE)))
    }
}

/// One wasm entry behind the fiber engine's body seam. Its instance lives in
/// a [`SharedSlot`] seat so Mode-1 swap replaces it without touching the
/// fiber — the seat pairs the instance with ITS OWN registrations, so
/// teardown always withdraws exactly the current instance's contribution
/// with the tokens that instance minted (I1, R5; round-2 blocker-4).
struct WasmBody {
    state: Arc<WasmState>,
    entry: EntryId,
    component: Arc<Mutex<LoadedComponent>>,
    grants: Arc<Vec<String>>,
    at: Mutex<Context<()>>,
    config: Mutex<String>,
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
            let config = lock(&self.config).clone().into_bytes();
            let at = lock(&self.at).clone();
            let fiber = setup.fiber();
            let state = Arc::clone(&self.state);
            let peer = state.broker.register_peer(Some(fiber));
            for contract in self.grants.iter() {
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
            // ONE effect owns the whole guest contribution. It tombstones
            // the swap slot FIRST (the loom-modeled arbitration: a racing
            // swap claim refuses and discards), then retires the live seat —
            // guest inverses LIFO against the instance that minted them,
            // listeners, provisions, instance — exactly and nothing else.
            let (slot, broker, topics, entry) = (
                Arc::clone(&self.slot),
                Arc::clone(&state.broker),
                Arc::clone(&state.topics),
                self.entry.clone(),
            );
            let disposer_state = Arc::clone(&state);
            setup.effect(
                "wasm guest seat",
                Disposer::future(move || async move {
                    disposer_state.swap.dispose(slot_id);
                    let retired = match slot.take() {
                        Some(seat) => seat.retire(&broker, &topics, peer).await,
                        None => Ok(()),
                    };
                    broker.remove_peer(peer);
                    lock(&disposer_state.roster).remove(&entry);
                    retired
                }),
            )?;
            // The body runs once per fiber; its contribution commits into
            // the seat, success or failure alike — a failing activation
            // still owes its inverses (I1).
            let (outcome, contributed) = handle.activate(config).await;
            let seat = SeatState {
                instance: handle,
                effects: contributed.effects,
                provisions: contributed.provisions,
                listens: contributed
                    .listens
                    .iter()
                    .filter_map(|record| record.id)
                    .collect(),
            };
            if let Some(previous) = self.slot.install(seat) {
                // A predecessor was already retired by its own teardown;
                // anything still seated here is disposed defensively.
                previous.instance.dispose().await;
            }
            outcome
        })
    }
}

impl WasmLane for Adapter {
    fn register_wasm_package(
        &self,
        package: &str,
        artifact: WasmArtifact,
        grants: Vec<String>,
    ) -> Result<EffectId, KernelError> {
        let state = Arc::clone(&self.wasm);
        let component =
            state
                .host
                .load(artifact.bytes, &artifact.expected_hash, state.sink.as_ref())?;
        let shared = Arc::new(Mutex::new(component));
        let grants = Arc::new(grants);
        let fibers = Arc::clone(&self.fibers);
        let (lane_state, lane_shared, lane_grants) =
            (Arc::clone(&state), Arc::clone(&shared), Arc::clone(&grants));
        let lane = PackageLane {
            injects: Vec::new(),
            provides: None,
            spawn: Box::new(move |request| {
                let config = config_of::<String>(request.config)?;
                let body = Arc::new(WasmBody {
                    state: Arc::clone(&lane_state),
                    entry: request.entry.clone(),
                    component: Arc::clone(&lane_shared),
                    grants: Arc::clone(&lane_grants),
                    at: Mutex::new(request.at.clone()),
                    config: Mutex::new(config),
                    slot: Arc::new(SharedSlot::default()),
                });
                let restate = |body: &WasmBody, config: String| {
                    *lock(&body.config) = config;
                    Ok(())
                };
                Ok(crate::wiring::spawned(&fibers, body, request, restate))
            }),
        };
        let effect = self.register_lane_effect::<String>(package, lane)?;
        lock(&state.packages).insert(package.to_owned(), shared);
        Ok(effect)
    }

    fn broker_grant(&self, contract: &str) {
        self.wasm.broker.grant(self.wasm.harness_peer(), contract);
    }

    fn broker_resolve(&self, contract: &str) -> Result<u64, KernelError> {
        self.wasm.broker.resolve(self.wasm.harness_peer(), contract)
    }

    fn broker_call(
        &self,
        handle: u64,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'_, Vec<u8>> {
        self.wasm
            .broker
            .call(self.wasm.harness_peer(), handle, operation, payload)
    }

    fn swap_wasm_artifact(
        &self,
        old_hash: &str,
        artifact: WasmArtifact,
    ) -> KernelFuture<'_, SwapReport> {
        let state = Arc::clone(&self.wasm);
        let old_hash = old_hash.to_owned();
        Box::pin(async move {
            let fresh =
                state
                    .host
                    .load(artifact.bytes, &artifact.expected_hash, state.sink.as_ref())?;
            let slots = LaneSlots {
                state: Arc::clone(&state),
                fresh: fresh.clone(),
            };
            let outcome = swap_batch(
                &slots,
                &state.swap,
                &old_hash,
                fresh.hash(),
                state.sink.as_ref(),
            )
            .await?;
            if !outcome.rolled_back {
                // Retarget every package pinned to the old artifact: live
                // roster slots share these cells, and future activations of
                // the package use the new artifact too.
                for shared in lock(&state.packages).values() {
                    let mut pinned = lock(shared);
                    if pinned.hash() == old_hash {
                        *pinned = fresh.clone();
                    }
                }
            }
            Ok(SwapReport {
                swapped: outcome.swapped,
                rolled_back: outcome.rolled_back,
            })
        })
    }
}
