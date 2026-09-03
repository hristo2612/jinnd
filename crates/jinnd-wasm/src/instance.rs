//! The per-instance supervisor: one tokio task exclusively owns the wasmtime
//! `Store` and serves commands from its channel (R1 — no lock is ever held
//! across guest execution; nothing else can even reach the store). Fuel
//! yields keep a spinning guest cooperative on the executor, the per-call
//! deadline kills a hung guest, and a trap terminates the instance — each
//! contained to this one fiber (R11): the supervisor replies, drops the
//! store, and is gone (R7 instant dispose, I1).

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use jinnd_api::{FiberId, KernelError, LedgerEventKind};
use tokio::sync::{mpsc, watch};
use wasmtime::Store;

use crate::bindings::{Plugin, PluginPre};
use crate::broker::Broker;
use crate::handle::{ActivationOutcome, Command, InstanceHandle, gone, pair, peer_face};
use crate::peer::PeerId;
use crate::selector::RealmOracle;
use crate::settle::{DeadlineControl, Settled, hung, settle, settle_delivery, trapped, within};
use crate::topics::LocalTopics;

mod late;

pub(crate) use late::sealed_error;
use late::{commit_late, refuse_all};

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
    pub(crate) horizon: DeadlineControl,
}

/// Spawns the supervisor for one instance of a world-typechecked component
/// and returns its handle. Instantiation happens inside the task, under the
/// deadline, from the [`PluginPre`] the load-time validation produced.
pub(crate) fn spawn(
    engine: wasmtime::Engine,
    pre: PluginPre<HostState>,
    seat: Seat,
) -> InstanceHandle {
    let deadline = seat.deadline;
    let (handle, deaths, aborts, rx) = pair(deadline);
    let face = peer_face(&handle);
    tokio::spawn(run(engine, pre, seat, face, deaths, aborts, rx));
    handle
}

async fn run(
    engine: wasmtime::Engine,
    pre: PluginPre<HostState>,
    seat: Seat,
    face: Arc<crate::handle::InstancePeer>,
    deaths: watch::Sender<Option<KernelError>>,
    aborts: watch::Receiver<Option<KernelError>>,
    mut rx: mpsc::Receiver<Command>,
) {
    let deadline = seat.deadline;
    let horizon = DeadlineControl::new();
    let mut store = Store::new(
        &engine,
        HostState {
            seat,
            face,
            outcome: ActivationOutcome::default(),
            horizon: horizon.clone(),
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

async fn serve(
    store: &mut Store<HostState>,
    plugin: &Plugin,
    deadline: Duration,
    deaths: &watch::Sender<Option<KernelError>>,
    mut aborts: watch::Receiver<Option<KernelError>>,
    rx: &mut mpsc::Receiver<Command>,
) {
    let guest = plugin.jinn_plugin_lifecycle();
    let mut sealed = false;
    let mut active = false;
    loop {
        let command = tokio::select! {
            biased;
            changed = aborts.changed() => {
                if changed.is_err() {
                    return;
                }
                if let Some(error) = aborts.borrow_and_update().clone() {
                    die(store, deaths, active, error);
                    return;
                }
                continue;
            }
            command = rx.recv() => match command {
                Some(command) => command,
                None => return,
            },
        };
        // A sealed instance runs no guest entry (M2-K4): the seat's journal
        // is closed, so nothing may register into it or escape it. A
        // CLOSING seat refuses at the door too (M2-K5 #16): the entry that
        // was already running drained under its deadline; the ones queued
        // behind it never start.
        let closing = store
            .data()
            .seat
            .slot
            .as_ref()
            .is_some_and(|slot| slot.closing());
        let shut = sealed || closing;
        match &command {
            Command::Activate { reply, .. } if shut => {
                if let Command::Activate { reply, .. } = command {
                    let _ = reply.send((Err(sealed_error()), ActivationOutcome::default()));
                }
                continue;
            }
            Command::HandleCall { .. } | Command::Deliver { .. } if shut => {
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
                let horizon = store.data().horizon.clone();
                let settled = interrupted(
                    &mut aborts,
                    settle(
                        deadline,
                        &horizon,
                        guest.call_activate(&mut *store, &config),
                    ),
                )
                .await
                .unwrap_or_else(Settled::Dead);
                let outcome = std::mem::take(&mut store.data_mut().outcome);
                match settled {
                    Settled::Ok(()) => {
                        active = true;
                        drop(reply.send((Ok(()), outcome)));
                    }
                    Settled::Fault(error) => drop(reply.send((Err(error), outcome))),
                    Settled::Dead(error) => {
                        let _ = reply.send((Err(error), outcome));
                        return;
                    }
                }
            }
            Command::Check { consumer, reply } => {
                let horizon = store.data().horizon.clone();
                match interrupted(
                    &mut aborts,
                    within(deadline, &horizon, guest.call_check(&mut *store, consumer)),
                )
                .await
                {
                    Ok(Ok(Ok(vital))) => drop(reply.send(vital)),
                    Ok(Ok(Err(trap))) => {
                        commit_late(store);
                        die(store, deaths, active, trapped(&trap));
                        let _ = reply.send(false);
                        return;
                    }
                    Ok(Err(_)) => {
                        commit_late(store);
                        die(store, deaths, active, hung());
                        let _ = reply.send(false);
                        return;
                    }
                    Err(error) => {
                        commit_late(store);
                        die(store, deaths, active, error);
                        let _ = reply.send(false);
                        return;
                    }
                }
            }
            Command::Undo { token, reply } => {
                let horizon = store.data().horizon.clone();
                let settled = interrupted(
                    &mut aborts,
                    settle(deadline, &horizon, guest.call_undo(&mut *store, token)),
                )
                .await
                .unwrap_or_else(Settled::Dead);
                commit_late(store);
                match settled {
                    Settled::Ok(()) => drop(reply.send(Ok(()))),
                    Settled::Fault(error) => drop(reply.send(Err(error))),
                    Settled::Dead(error) => {
                        let error = die(store, deaths, active, error);
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
                let horizon = store.data().horizon.clone();
                let call =
                    guest.call_handle_call(&mut *store, caller, &contract, &operation, &payload);
                let settled = interrupted(&mut aborts, settle(deadline, &horizon, call))
                    .await
                    .unwrap_or_else(Settled::Dead);
                commit_late(store);
                match settled {
                    Settled::Ok(answer) => drop(reply.send(Ok(answer))),
                    Settled::Fault(error) => drop(reply.send(Err(error))),
                    Settled::Dead(error) => {
                        let error = die(store, deaths, active, error);
                        let _ = reply.send(Err(error));
                        return;
                    }
                }
            }
            Command::Deliver {
                token,
                topic,
                payload,
                budget,
                reply,
            } => {
                let horizon = store.data().horizon.clone();
                if let Some(budget) = budget {
                    let _ = store.set_fuel(budget.get());
                }
                let call = guest.call_handle_event(&mut *store, token, &topic, &payload);
                let settled = interrupted(
                    &mut aborts,
                    settle_delivery(deadline, &horizon, budget.is_some(), call),
                )
                .await
                .unwrap_or_else(Settled::Dead);
                let _ = store.set_fuel(u64::MAX);
                commit_late(store);
                match settled {
                    Settled::Ok(answer) => drop(reply.send(Ok(answer))),
                    Settled::Fault(error) => drop(reply.send(Err(error))),
                    Settled::Dead(error) => {
                        let error = die(store, deaths, active, error);
                        let _ = reply.send(Err(error));
                        return;
                    }
                }
            }
            Command::Snapshot { reply } => {
                let horizon = store.data().horizon.clone();
                match interrupted(
                    &mut aborts,
                    within(deadline, &horizon, guest.call_snapshot(&mut *store)),
                )
                .await
                {
                    Ok(Ok(Ok(blob))) => drop(reply.send(Ok(blob))),
                    Ok(Ok(Err(trap))) => {
                        commit_late(store);
                        let error = die(store, deaths, active, trapped(&trap));
                        let _ = reply.send(Err(error));
                        return;
                    }
                    Ok(Err(_)) => {
                        commit_late(store);
                        let error = die(store, deaths, active, hung());
                        let _ = reply.send(Err(error));
                        return;
                    }
                    Err(error) => {
                        commit_late(store);
                        let error = die(store, deaths, active, error);
                        let _ = reply.send(Err(error));
                        return;
                    }
                }
            }
            Command::Restore { blob, reply } => {
                let horizon = store.data().horizon.clone();
                let settled = interrupted(
                    &mut aborts,
                    settle(deadline, &horizon, guest.call_restore(&mut *store, &blob)),
                )
                .await
                .unwrap_or_else(Settled::Dead);
                commit_late(store);
                let late = std::mem::take(&mut store.data_mut().outcome);
                match settled {
                    Settled::Ok(()) => drop(reply.send((Ok(()), late))),
                    Settled::Fault(error) => drop(reply.send((Err(error), late))),
                    Settled::Dead(error) => {
                        let error = die(store, deaths, active, error);
                        let _ = reply.send((Err(error), late));
                        return;
                    }
                }
            }
        }
    }
}

async fn interrupted<T>(
    aborts: &mut watch::Receiver<Option<KernelError>>,
    work: impl Future<Output = T>,
) -> Result<T, KernelError> {
    if let Some(error) = aborts.borrow_and_update().clone() {
        return Err(error);
    }
    tokio::select! {
        biased;
        _ = aborts.changed() => Err(aborts.borrow_and_update().clone().unwrap_or_else(hung)),
        value = work => Ok(value),
    }
}

fn die(
    store: &Store<HostState>,
    deaths: &watch::Sender<Option<KernelError>>,
    active: bool,
    mut error: KernelError,
) -> KernelError {
    if !active {
        return error;
    }
    error.fiber = store.data().seat.fiber;
    store.data().seat.broker.ledger().append(
        LedgerEventKind::ErrorRecorded {
            error: error.clone(),
        },
        store.data().seat.fiber,
    );
    deaths.send_replace(Some(error.clone()));
    error
}
