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
use crate::grants::{admission, authority};
use crate::handle::Registration;
use crate::host::LoadedComponent;
use crate::instance::Seat;
use crate::peer::PeerId;
use crate::selector::NoRealms;
use crate::slot::{SeatState, SharedSlot};

mod journal;
mod spawn;
mod state;

pub use spawn::wasm_lane;
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
}

impl Rebind for WasmBody {
    fn rebind(&self, at: Context<()>) {
        *lock(&self.at) = at;
    }
}

impl FiberBody for WasmBody {
    fn activate<'a>(&'a self, mut setup: Setup<'a>) -> KernelFuture<'a, ()> {
        Box::pin(async move {
            let (grants, faults, config) = {
                let seat = lock(&self.seat);
                (
                    seat.grants.clone(),
                    seat.faults.clone(),
                    seat.payload.clone(),
                )
            };
            let at = lock(&self.at).clone();
            let fiber = setup.fiber();
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
            for fault in &faults {
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
            }
            // The entry's granted `jinn:clock` resolution floor (M2-K2,
            // R9): grants scope alarm resolution per entry — read off the
            // ADMITTED grants only.
            let clock_floor_ms = clock_floor(&admitted);
            let slot_id = core.next_slot.fetch_add(1, Ordering::SeqCst) + 1;
            // A fresh incarnation opens a fresh journal: the previous
            // seat's seal landed before this activation was planned.
            self.slot.unseal();
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
                    staging: false,
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
                        let retained = seat
                            .suspend(&owner.broker, &owner.topics, &owner.alarms, peer, ledger)
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
            if let Some(previous) = self.slot.install(SeatState::live(handle, contributed)) {
                previous.instance.dispose().await;
            }
            outcome
        })
    }
}

/// One seat's closing sequence (M2-K4), shared by its two inverses.
#[derive(Clone)]
struct SeatClosing {
    slot: Arc<SharedSlot>,
    entry: EntryId,
    owner: Arc<LaneCore>,
    slot_id: u64,
    peer: PeerId,
}

impl SeatClosing {
    /// Tombstones the swap slot, then closes the seat in law order (M2-K5
    /// #16): door shut, the instance's in-flight guest entry DRAINED under
    /// its deadline, journal sealed — and takes the seat, or `None` when no
    /// seat was ever installed.
    async fn close(&self) -> Option<SeatState> {
        self.owner.swap.dispose(self.slot_id);
        let instance = self.slot.current();
        self.slot
            .close(async move {
                if let Some(instance) = instance {
                    instance.seal().await;
                }
            })
            .await;
        self.slot.take()
    }

    /// The seat is gone: the peer and the roster row go with it.
    fn forget(&self) {
        self.owner.broker.remove_peer(self.peer);
        lock(&self.owner.roster).remove(&self.entry);
    }
}
