//! The supervisor's command loop: one guest entry at a time under the
//! instance's own clock, the sealed/closing door (M2-K4/K5), and the exit
//! on death. Split from `instance.rs` by responsibility (R10 file hygiene).

use std::time::Duration;

use jinnd_api::KernelError;
use tokio::sync::{mpsc, watch};
use wasmtime::Store;

use crate::bindings::Plugin;
use crate::handle::{ActivationOutcome, Command};
use crate::settle::{Settled, hung, settle, settle_delivery, trapped, within};

use super::HostState;
use super::guard::{die, interrupted};
use super::late::{commit_late, sealed_error};

pub(super) async fn serve(
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
    // The seat's staging watch (M2-K26 amendment 2): the commit's flip
    // wakes this loop once, to route what was recorded while staged.
    let mut staging = store.data().staging.clone();
    let mut staged = *staging.borrow_and_update();
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
            flipped = staging.changed(), if staged => {
                staged = flipped.is_ok() && *staging.borrow_and_update();
                if !staged {
                    commit_late(store);
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
