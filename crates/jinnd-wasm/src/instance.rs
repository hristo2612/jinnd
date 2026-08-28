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
use tokio::sync::mpsc;
use tokio::time::timeout;
use wasmtime::Store;

use crate::bindings::{Plugin, PluginPre};
use crate::broker::Broker;
use crate::handle::{ActivationOutcome, Command, InstanceHandle, gone, pair, peer_face};
use crate::peer::PeerId;
use crate::selector::RealmOracle;
use crate::settle::{Settled, hung, settle, trapped};
use crate::topics::LocalTopics;

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
    /// A staging instance (the not-yet-committed side of a Mode-1 swap):
    /// its provide/listen registrations are RECORDED but not routed — the
    /// old instance stays warm and fully routed until commit (R8).
    pub staging: bool,
}

/// The store data: the instance's kernel surroundings plus what the current
/// activation has registered so far (surfaces are implemented in
/// `surfaces.rs`).
pub(crate) struct HostState {
    pub(crate) seat: Seat,
    pub(crate) face: Arc<crate::handle::InstancePeer>,
    pub(crate) outcome: ActivationOutcome,
}

/// Spawns the supervisor for one instance of a world-typechecked component
/// and returns its handle. Instantiation happens inside the task, under the
/// deadline, from the [`PluginPre`] the load-time validation produced.
pub(crate) fn spawn(
    engine: wasmtime::Engine,
    pre: PluginPre<HostState>,
    seat: Seat,
) -> InstanceHandle {
    let (handle, rx) = pair();
    let face = peer_face(&handle);
    tokio::spawn(run(engine, pre, seat, face, rx));
    handle
}

async fn run(
    engine: wasmtime::Engine,
    pre: PluginPre<HostState>,
    seat: Seat,
    face: Arc<crate::handle::InstancePeer>,
    mut rx: mpsc::Receiver<Command>,
) {
    let deadline = seat.deadline;
    let mut store = Store::new(
        &engine,
        HostState {
            seat,
            face,
            outcome: ActivationOutcome::default(),
        },
    );
    let fueled = store
        .set_fuel(u64::MAX)
        .and_then(|()| store.fuel_async_yield_interval(Some(FUEL_YIELD_INTERVAL)));
    if fueled.is_err() {
        refuse_all(&mut rx, gone()).await;
        return;
    }
    let instantiated = timeout(deadline, pre.instantiate_async(&mut store)).await;
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
    serve(&mut store, &plugin, deadline, &mut rx).await;
    // The store drops here: the instance's memory, tables, and in-flight
    // state vanish together (R7 instant dispose; I1 — exactly its
    // contribution, because everything it contributed lives behind effects
    // the kernel replays separately).
}

async fn serve(
    store: &mut Store<HostState>,
    plugin: &Plugin,
    deadline: Duration,
    rx: &mut mpsc::Receiver<Command>,
) {
    let guest = plugin.jinn_plugin_lifecycle();
    let mut sealed = false;
    while let Some(command) = rx.recv().await {
        // A sealed instance runs no guest entry (M2-K4): the seat's journal
        // is closed, so nothing may register into it or escape it.
        match &command {
            Command::Activate { reply, .. } if sealed => {
                if let Command::Activate { reply, .. } = command {
                    let _ = reply.send((Err(sealed_error()), ActivationOutcome::default()));
                }
                continue;
            }
            Command::HandleCall { .. } | Command::Deliver { .. } if sealed => {
                match command {
                    Command::HandleCall { reply, .. } | Command::Deliver { reply, .. } => {
                        let _ = reply.send(Err(sealed_error()));
                    }
                    _ => {}
                }
                continue;
            }
            _ => {}
        }
        match command {
            Command::Shutdown => return,
            Command::Seal { reply } => {
                sealed = true;
                let _ = reply.send(());
            }
            Command::Activate { config, reply } => {
                let settled = settle(deadline, guest.call_activate(&mut *store, &config)).await;
                let outcome = std::mem::take(&mut store.data_mut().outcome);
                match settled {
                    Settled::Ok(()) => drop(reply.send((Ok(()), outcome))),
                    Settled::Fault(error) => drop(reply.send((Err(error), outcome))),
                    Settled::Dead(error) => {
                        let _ = reply.send((Err(error), outcome));
                        return;
                    }
                }
            }
            Command::Check { consumer, reply } => {
                match timeout(deadline, guest.call_check(&mut *store, consumer)).await {
                    Ok(Ok(vital)) => drop(reply.send(vital)),
                    // A trapped or hung check is not vital, and ends the
                    // instance (contained, recorded by the caller).
                    _ => {
                        let _ = reply.send(false);
                        return;
                    }
                }
            }
            Command::Undo { token, reply } => {
                match settle(deadline, guest.call_undo(&mut *store, token)).await {
                    Settled::Ok(()) => drop(reply.send(Ok(()))),
                    Settled::Fault(error) => drop(reply.send(Err(error))),
                    Settled::Dead(error) => {
                        let _ = reply.send(Err(error));
                        return;
                    }
                }
            }
            Command::HandleCall {
                caller,
                contract,
                operation,
                payload,
                reply,
            } => {
                let call =
                    guest.call_handle_call(&mut *store, caller, &contract, &operation, &payload);
                let settled = settle(deadline, call).await;
                commit_late(store);
                match settled {
                    Settled::Ok(answer) => drop(reply.send(Ok(answer))),
                    Settled::Fault(error) => drop(reply.send(Err(error))),
                    Settled::Dead(error) => {
                        let _ = reply.send(Err(error));
                        return;
                    }
                }
            }
            Command::Deliver {
                token,
                topic,
                payload,
                reply,
            } => {
                let call = guest.call_handle_event(&mut *store, token, &topic, &payload);
                let settled = settle(deadline, call).await;
                commit_late(store);
                match settled {
                    Settled::Ok(answer) => drop(reply.send(Ok(answer))),
                    Settled::Fault(error) => drop(reply.send(Err(error))),
                    Settled::Dead(error) => {
                        let _ = reply.send(Err(error));
                        return;
                    }
                }
            }
            Command::Snapshot { reply } => {
                match timeout(deadline, guest.call_snapshot(&mut *store)).await {
                    Ok(Ok(blob)) => drop(reply.send(Ok(blob))),
                    Ok(Err(trap)) => {
                        let _ = reply.send(Err(trapped(&trap)));
                        return;
                    }
                    Err(_) => {
                        let _ = reply.send(Err(hung()));
                        return;
                    }
                }
            }
            Command::Restore { blob, reply } => {
                match settle(deadline, guest.call_restore(&mut *store, &blob)).await {
                    Settled::Ok(()) => drop(reply.send(Ok(()))),
                    Settled::Fault(error) => drop(reply.send(Err(error))),
                    Settled::Dead(error) => {
                        let _ = reply.send(Err(error));
                        return;
                    }
                }
            }
        }
    }
}

/// The refusal a sealed seat answers (M2-K4): typed as the inactive
/// context it is — the instance's journal is closed for withdrawal.
pub(crate) fn sealed_error() -> KernelError {
    KernelError {
        code: jinnd_api::ErrorCode::InactiveContext,
        message: "refused: the seat's journal is sealed for withdrawal".to_owned(),
        fiber: None,
    }
}

/// Commits registrations a guest made outside its activation (from a
/// `handle-event` or `handle-call`) into the live seat's journal (M2-K3
/// round 2; R5, I1): an effect registered late is withdrawn LIFO with the
/// rest, never orphaned in the store. With no seat installed yet they
/// wait for the next drain.
fn commit_late(store: &mut Store<HostState>) {
    let data = store.data_mut();
    if data.outcome.registrations.is_empty() {
        return;
    }
    let late = std::mem::take(&mut data.outcome.registrations);
    if let Some(slot) = &data.seat.slot {
        if let Some(kept) = slot.extend(late) {
            data.outcome.registrations = kept;
        }
    } else {
        data.outcome.registrations = late;
    }
}

/// Answers every remaining command with `error` — an instance that failed to
/// come up never hangs its callers.
async fn refuse_all(rx: &mut mpsc::Receiver<Command>, error: KernelError) {
    while let Some(command) = rx.recv().await {
        match command {
            Command::Shutdown => return,
            Command::Seal { reply } => drop(reply.send(())),
            Command::Activate { reply, .. } => {
                let _ = reply.send((Err(error.clone()), ActivationOutcome::default()));
            }
            Command::Check { reply, .. } => drop(reply.send(false)),
            Command::Undo { reply, .. } | Command::Restore { reply, .. } => {
                drop(reply.send(Err(error.clone())))
            }
            Command::HandleCall { reply, .. }
            | Command::Deliver { reply, .. }
            | Command::Snapshot { reply } => drop(reply.send(Err(error.clone()))),
        }
    }
}
