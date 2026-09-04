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

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jinnd_api::{EntryId, KernelFuture, LedgerEventKind};
use jinnd_context::Context;
use jinnd_effects::Disposer;
use jinnd_fiber::{FiberBody, Setup};
use jinnd_loader::host::Rebind;

use crate::alarms::clock_floor;
use crate::broker_state::refusal;
pub use crate::grants::{Grant, SeatSpec};
use crate::grants::{admission, attenuate, authority};
use crate::handle::Registration;
use crate::host::LoadedComponent;
use crate::instance::Seat;
use crate::selector::NoRealms;
use crate::slot::{SeatState, SharedSlot, commit_staged};

mod closing;
mod injects;
mod journal;
mod spawn;
mod state;

use closing::SeatClosing;

pub use injects::Declaration;
pub use spawn::{wasm_lane, wasm_lane_declaring};
pub use state::LaneCore;
pub(crate) use state::{Roster, lock};

/// The guest-call deadline (R11 containment horizon).
pub(crate) const DEADLINE: Duration = Duration::from_secs(5);

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
    /// The entry's string-lane gate (M2-K24): what it declares it injects.
    gate: Arc<injects::Gate>,
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

    /// The profile entry this body hosts (M2-K7: the ledger's entry column
    /// is filled from the fiber → entry mapping the lane knows).
    #[must_use]
    pub fn entry(&self) -> &EntryId {
        &self.entry
    }

    pub(crate) fn gate(&self) -> Arc<injects::Gate> {
        Arc::clone(&self.gate)
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
            let (grants, grant_faults, config) = {
                let seat = lock(&self.seat);
                (
                    seat.grants.clone(),
                    seat.faults.clone(),
                    seat.payload.clone(),
                )
            };
            let at = lock(&self.at).clone();
            let fiber = setup.fiber();
            let fault_sink = setup.faults();
            let core = Arc::clone(&self.core);
            let peer = core.broker.register_peer(Some(fiber));
            // Host providers retain this seat's inverses under the ENTRY
            // (M2-K4): the journal spans incarnations and processes.
            core.broker.attribute_entry(peer, &self.entry.0);
            // Fail-closed scope admission (round-3 ruling; Law 1, 01
            // §Grants): only admitted grants become broker authority; every
            // refusal — an invalid scope, or an entry unreadable as a grant
            // at all — is a ledgered per-entry error, never a silent drop
            // and never a widened unscoped grant.
            for fault in &grant_faults {
                core.sink.append(
                    LedgerEventKind::ErrorRecorded {
                        error: refusal(
                            jinnd_api::ErrorCode::EffectFailed,
                            format!("grant entry refused: {fault}"),
                        ),
                    },
                    Some(fiber),
                );
            }
            let (admitted, refusals) = admission(&grants);
            for error in refusals {
                core.sink
                    .append(LedgerEventKind::ErrorRecorded { error }, Some(fiber));
            }
            // An admitted scope travels to the broker as typed authority
            // (M2-K3 round 2, M2-K6): the provider enforces it per call.
            for grant in &admitted {
                core.broker
                    .grant_with(peer, &grant.contract, authority(grant));
                attenuate(&core.broker, peer, grant);
            }
            // The declaration is judged at this same fail-closed point
            // (M2-K24; R11, constitution 01): an element that is no
            // declaration, or a contract declared but not admitted as a
            // grant, is a per-entry fault ON THE RECORD — the entry loads
            // nothing, its siblings load normally, nothing widens.
            let refused = self.gate.admission(&admitted);
            if let Some(first) = refused.first().cloned() {
                for error in refused {
                    core.sink
                        .append(LedgerEventKind::ErrorRecorded { error }, Some(fiber));
                }
                core.broker.remove_peer(peer);
                return Err(first);
            }
            // The entry's granted `jinn:clock` resolution floor (M2-K2,
            // R9): grants scope alarm resolution per entry — read off the
            // ADMITTED grants only.
            let clock_floor_ms = clock_floor(&admitted);
            let slot_id = core.next_slot.fetch_add(1, Ordering::SeqCst) + 1;
            // A fresh incarnation opens a fresh journal: the previous
            // seat's seal landed before this activation was planned.
            self.slot.unseal();
            // A REPLACEMENT activates as a staging seat (M2-K26 (b); R8):
            // its listens and provisions are recorded, not routed, and land
            // atomically at the commit below in place of the tombstones the
            // suspension left — a walk selects tombstones (refused) before
            // the lock and live listeners after, never neither. A first
            // activation is unchanged: nothing is being replaced.
            let replacing = self.slot.ever_installed();
            let component = lock(&self.component).clone();
            let handle = core.host.instantiate(
                &component,
                Seat {
                    broker: Arc::clone(&core.broker),
                    topics: Arc::clone(&core.topics),
                    alarms: Arc::clone(&core.alarms),
                    oracle: Arc::new(NoRealms),
                    peer,
                    fiber: Some(fiber),
                    context: at.id().0,
                    deadline: DEADLINE,
                    clock_floor_ms,
                    slot: Some(Arc::clone(&self.slot)),
                    staging: replacing,
                },
            );
            // Host-provider wakes (M2-K7 `jinn:net/readable`) deliver to
            // THIS instance's own face, like a listener's or an alarm's.
            core.broker.attach_target(
                peer,
                crate::handle::peer_face(&handle) as Arc<dyn crate::topics::EventTarget>,
            );
            lock(&core.roster).insert(
                self.entry.clone(),
                Roster {
                    slot: Arc::clone(&self.slot),
                    slot_id,
                    peer,
                    fiber,
                    context: at.id().0,
                    clock_floor_ms,
                    config: config.clone(),
                    component: Arc::clone(&self.component),
                    faults: fault_sink.clone(),
                },
            );
            // ONE effect owns the whole guest contribution: tombstone the
            // swap slot FIRST (loom-modeled arbitration — a racing claim
            // refuses and discards), CLOSE the seat (M2-K5 #16: door shut,
            // the in-flight guest entry drained under its deadline, then
            // the journal sealed — every effect of a sub-deadline handler
            // lands, never a prefix), then retire the live seat exactly
            // (I1, R5) — or, on suspension, release its kernel
            // registrations and hand its world effects to the entry's
            // journal (decision log 2026-08-28).
            let trail = self.guest_trail;
            let closing = SeatClosing {
                slot: Arc::clone(&self.slot),
                entry: self.entry.clone(),
                owner: Arc::clone(&core),
                slot_id,
                peer,
            };
            let suspending = closing.clone();
            setup.suspendable_effect(
                "wasm guest seat",
                Disposer::future(move || async move {
                    let retired = match closing.close().await {
                        Some(seat) => {
                            let ledger = trail.then_some((closing.owner.sink.as_ref(), fiber));
                            let owner = &closing.owner;
                            let held = seat.host_effects();
                            let retired = seat
                                .retire(&owner.broker, &owner.topics, &owner.alarms, peer, ledger)
                                .await;
                            owner.release(&closing.entry, &held);
                            retired
                        }
                        None => Ok(()),
                    };
                    closing.forget();
                    retired
                }),
                Disposer::future(move || async move {
                    let closing = suspending;
                    if let Some(seat) = closing.close().await {
                        let ledger = trail.then_some((closing.owner.sink.as_ref(), fiber));
                        let owner = &closing.owner;
                        let tomb = (closing.entry.clone(), closing.slot_id);
                        let retained = seat
                            .suspend(
                                &owner.broker,
                                &owner.topics,
                                &owner.alarms,
                                peer,
                                ledger,
                                tomb,
                            )
                            .await;
                        let count = retained.len() as u64;
                        owner.inherit(&closing.entry, retained);
                        if trail {
                            owner.sink.append(
                                LedgerEventKind::FiberSuspended { retained: count },
                                Some(fiber),
                            );
                        }
                    }
                    closing.forget();
                    Ok(())
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
                        // An alarm request IS an effect (M2-K2, R5); its
                        // registration is a ledger event like any other.
                        Registration::Alarm(alarm) => alarm.label.clone(),
                        // The broker ledgered the provide crossing itself
                        // (R6); the host provider ledgered its own effect
                        // registration with this fiber's attribution (M2-K3;
                        // a kernel registration's spawn/listen line, M2-K6).
                        Registration::Provision { .. }
                        | Registration::Host(_)
                        | Registration::Kernel(_) => continue,
                    };
                    core.sink
                        .append(LedgerEventKind::EffectRegistered { label }, Some(fiber));
                }
            }
            let watched = handle.clone();
            let displaced = if replacing && outcome.is_ok() {
                // Mode 0 gets Mode 1's commit (R8): under the topic table's
                // one lock the tombstones go and the staged listens land;
                // the old subscription's withdrawal row lands HERE, when it
                // actually ended (Law 2) — replaced, never absent.
                let entombed = core.topics.entombed(fiber);
                let displaced = commit_staged(
                    &self.slot,
                    handle,
                    contributed,
                    &core.broker,
                    &core.topics,
                    &core.alarms,
                    peer,
                    Some(fiber),
                    at.id().0,
                    core.sink.as_ref(),
                );
                if self.guest_trail {
                    for (_, topic) in entombed {
                        core.sink.append(
                            LedgerEventKind::EffectWithdrawn {
                                label: format!("listen {topic}"),
                                clean: true,
                            },
                            Some(fiber),
                        );
                    }
                }
                displaced
            } else {
                // A failed staged activation installs uncommitted, exactly
                // as a failed first one: its recorded listens and
                // provisions were never routed (ids absent, so the replay
                // skips them) and its effects still owe their inverses
                // (I1); the tombstones leave with the fiber's `Failed`
                // rest (M2-K26 (c)).
                self.slot.install(SeatState::live(handle, contributed))
            };
            if let Some(previous) = displaced {
                previous.instance.dispose().await;
            }
            if outcome.is_ok() {
                core.track_death(Arc::clone(&self.slot), watched, fault_sink);
            }
            outcome
        })
    }
}
