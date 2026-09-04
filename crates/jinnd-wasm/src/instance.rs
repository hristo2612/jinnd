//! The per-instance supervisor: one tokio task exclusively owns the wasmtime
//! `Store` and serves commands from its channel (R1 — no lock is ever held
//! across guest execution; nothing else can even reach the store). Fuel
//! yields keep a spinning guest cooperative on the executor, the per-call
//! deadline kills a hung guest, and a trap terminates the instance — each
//! contained to this one fiber (R11): the supervisor replies, drops the
//! store, and is gone (R7 instant dispose, I1).

use std::sync::Arc;
use std::time::Duration;

use jinnd_api::{FiberId, KernelError};
use tokio::sync::{mpsc, watch};
use wasmtime::Store;

use crate::bindings::PluginPre;
use crate::broker::Broker;
use crate::handle::{ActivationOutcome, Command, InstanceHandle, gone, pair, peer_face};
use crate::peer::PeerId;
use crate::selector::RealmOracle;
use crate::settle::{DeadlineControl, hung, trapped, within};
use crate::topics::LocalTopics;

mod guard;
mod late;
mod serve;

use late::refuse_all;
pub(crate) use late::sealed_error;
use serve::serve;

/// How often a guest yields back to the executor, in fuel units.
const FUEL_YIELD_INTERVAL: u64 = 10_000;

/// Everything one instance needs from its kernel surroundings.
pub struct Seat {
    pub broker: Arc<Broker>,
    pub topics: Arc<LocalTopics>,
    /// The lane's alarm registry (M2-K2): `jinn:clock` alarm requests arm
    /// here, wakes deliver to the requesting instance's own face.
    pub alarms: Arc<crate::alarms::Alarms>,
    pub oracle: Arc<dyn RealmOracle>,
    /// The broker identity this instance calls and provides as.
    pub peer: PeerId,
    /// Ledger attribution of the hosting fiber.
    pub fiber: Option<FiberId>,
    /// The context the instance's listeners register under (C4 selectors).
    pub context: u64,
    /// The guest-call horizon: a guest that neither returns nor traps by
    /// this deadline is killed, deactivating only its own fiber (R11).
    pub deadline: Duration,
    /// The `jinn:clock` resolution floor this entry's grants scope (M2-K2,
    /// R9): a periodic alarm finer than this is refused at request time.
    pub clock_floor_ms: u64,
    /// The provider face the broker routes contract calls to; `None` routes
    /// straight to this instance. A lane passes its per-fiber
    /// [`crate::SharedSlot`] so a Mode-1 swap redirects call routing
    /// atomically at commit (R8) — kept provisions never re-run. Listener
    /// deliveries never route here: each registration targets the delivery
    /// face of the instance that minted its token (round-2 blocker-4).
    pub slot: Option<Arc<crate::slot::SharedSlot>>,
    /// Instantiated as a STAGING seat (the not-yet-committed side of a
    /// Mode-1 swap, or a config restart's replacement — M2-K26 (b)): its
    /// provide/listen/alarm registrations are RECORDED but not routed until
    /// the commit flips the instance live (R8; amendment 2, harness #53).
    pub staging: bool,
}

/// The store data: the instance's kernel surroundings plus what the current
/// activation has registered so far (surfaces are implemented in
/// `surfaces.rs`).
pub(crate) struct HostState {
    pub(crate) seat: Seat,
    pub(crate) face: Arc<crate::handle::InstancePeer>,
    pub(crate) outcome: ActivationOutcome,
    pub(crate) horizon: DeadlineControl,
    /// The seat's staging state, flipped by the commit (M2-K26 (b)).
    pub(crate) staging: watch::Receiver<bool>,
}

impl HostState {
    /// True while the seat is staged: a registration made now is recorded
    /// for the commit to route, never routed itself.
    pub(crate) fn staging(&self) -> bool {
        *self.staging.borrow()
    }
}

/// Spawns the supervisor for one instance of a world-typechecked component
/// and returns its handle. Instantiation happens inside the task, under the
/// deadline, from the [`PluginPre`] the load-time validation produced.
pub(crate) fn spawn(
    engine: wasmtime::Engine,
    pre: PluginPre<HostState>,
    seat: Seat,
) -> InstanceHandle {
    let (handle, deaths, aborts, rx, staging) = pair(seat.deadline, seat.staging);
    let face = peer_face(&handle);
    let horizon = handle.horizon.clone();
    tokio::spawn(run(
        engine, pre, seat, face, horizon, deaths, aborts, rx, staging,
    ));
    handle
}

#[allow(clippy::too_many_arguments)]
async fn run(
    engine: wasmtime::Engine,
    pre: PluginPre<HostState>,
    seat: Seat,
    face: Arc<crate::handle::InstancePeer>,
    horizon: DeadlineControl,
    deaths: watch::Sender<Option<KernelError>>,
    aborts: watch::Receiver<Option<KernelError>>,
    mut rx: mpsc::Receiver<Command>,
    staging: watch::Receiver<bool>,
) {
    let deadline = seat.deadline;
    let mut store = Store::new(
        &engine,
        HostState {
            seat,
            face,
            outcome: ActivationOutcome::default(),
            horizon: horizon.clone(),
            staging,
        },
    );
    let fueled = store
        .set_fuel(u64::MAX)
        .and_then(|()| store.fuel_async_yield_interval(Some(FUEL_YIELD_INTERVAL)));
    if fueled.is_err() {
        refuse_all(&mut rx, gone()).await;
        return;
    }
    let instantiated = within(deadline, &horizon, pre.instantiate_async(&mut store)).await;
    let plugin = match instantiated {
        Ok(Ok(plugin)) => plugin,
        Ok(Err(trap)) => {
            refuse_all(&mut rx, trapped(&trap)).await;
            return;
        }
        Err(_) => {
            refuse_all(&mut rx, hung()).await;
            return;
        }
    };
    serve(&mut store, &plugin, deadline, &deaths, aborts, &mut rx).await;
    // The store drops here: the instance's memory, tables, and in-flight
    // state vanish together (R7 instant dispose; I1 — exactly its
    // contribution, because everything it contributed lives behind effects
    // the kernel replays separately).
}
