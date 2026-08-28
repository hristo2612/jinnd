//! Mode-1 hot-swap (R8): old instance warm until the new one is healthy,
//! auto-rollback on failure, batched over every entry sharing the artifact
//! hash (decision log 2026-08-25), every phase a ledger event.
//!
//! The interleaving-sensitive part — a swap racing an entry disposal — lives
//! in [`SwapCore`], a sync phase machine modeled under loom (`--features
//! loom`) and driven by [`swap_batch`] itself: the model and the production
//! path are ONE machine (round-2 blocker-3 ruling). The async driver never
//! holds the core's lock across an await (R1).

use jinnd_api::{EntryId, KernelError, KernelFuture, LedgerEventKind, SwapPhaseKind};

use crate::peer::LedgerSink;
use crate::sync::Mutex;
use std::collections::HashMap;

/// One slot's swap phase. `Tombstone` is a disposed entry: a swap must never
/// resurrect it (I1/I4 — disposal wins).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotPhase {
    Steady,
    Preparing,
    Tombstone,
}

/// The phase machine deciding ownership between a swap's commit and a
/// concurrent disposal. Exactly one side ends up owning a prepared
/// instance: a claim that loses to a tombstone reports `false` and the
/// disposer's answer says whether it observed a preparation to discard.
#[derive(Default)]
pub struct SwapCore {
    slots: Mutex<HashMap<u64, SlotPhase>>,
}

impl SwapCore {
    fn with<T>(&self, f: impl FnOnce(&mut HashMap<u64, SlotPhase>) -> T) -> T {
        let mut guard = self
            .slots
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        f(&mut guard)
    }

    /// Steady → Preparing. False for a tombstoned or already-preparing slot.
    pub fn begin(&self, slot: u64) -> bool {
        self.with(
            |slots| match slots.entry(slot).or_insert(SlotPhase::Steady) {
                phase @ SlotPhase::Steady => {
                    *phase = SlotPhase::Preparing;
                    true
                }
                _ => false,
            },
        )
    }

    /// The batch claim AND the commit phase, one critical section: when
    /// EVERY slot is still `Preparing`, `commit` — the infallible sync
    /// bookkeeping phase (round-3 ruling: zero fallible operations) — runs
    /// under the phase lock, then every slot returns to `Steady`. `None`
    /// when any slot left Preparing (a disposal tombstoned it): nothing
    /// ran, the caller rolls the whole batch back. A disposal's tombstone
    /// takes this same lock, so it lands entirely before the claim or
    /// entirely after the committed bookkeeping — the partial-commit window
    /// does not exist (R8 atomic replacement).
    pub fn commit_all_with<T>(&self, slots: &[u64], commit: impl FnOnce() -> T) -> Option<T> {
        self.with(|phases| {
            let all_preparing = slots
                .iter()
                .all(|slot| phases.get(slot) == Some(&SlotPhase::Preparing));
            if !all_preparing {
                return None;
            }
            let landed = commit();
            for slot in slots {
                phases.insert(*slot, SlotPhase::Steady);
            }
            Some(landed)
        })
    }

    /// Preparing → Steady with the old instance kept (rollback).
    pub fn rollback(&self, slot: u64) {
        self.with(|slots| {
            if let Some(phase) = slots.get_mut(&slot)
                && *phase == SlotPhase::Preparing
            {
                *phase = SlotPhase::Steady;
            }
        });
    }

    /// Any phase → Tombstone. Returns the phase the disposer observed: seeing
    /// `Preparing` transfers ownership of the prepared instance to the
    /// disposer's side — the swap's claim will refuse and discard it.
    pub fn dispose(&self, slot: u64) -> SlotPhase {
        self.with(|slots| {
            let previous = slots.insert(slot, SlotPhase::Tombstone);
            previous.unwrap_or(SlotPhase::Steady)
        })
    }

    /// True once `slot` was disposed — the installer's convergence check:
    /// an install that observes the tombstone retires what it installed.
    pub fn is_tombstone(&self, slot: u64) -> bool {
        self.with(|slots| slots.get(&slot) == Some(&SlotPhase::Tombstone))
    }
}

/// The lane seam the swap driver drives (transport-agnostic: the adapter's
/// wasm lane implements it over real instances; unit tests over mocks).
///
/// Contract: `entries_pinned_to` names each live entry with its slot key in
/// the shared [`SwapCore`]; `prepare` is where EVERY fallible operation
/// lives (round-3 ruling) — it builds and health-gates the NEW instance
/// (activate, offer the old snapshot, health check) while the old instance
/// stays warm and untouched; `commit` is pure infallible SYNC bookkeeping
/// (seat/pointer swaps, registry updates, ledger appends) run inside the
/// batch claim's critical section — it must never fail, block, await, or
/// call guest code (R1), and hands back the displaced seat; `retire_displaced`
/// disposes a displaced seat after the critical section; `discard` withdraws
/// a prepared-but-never-committed instance by REPLAYING its staged effects
/// in reverse before dropping it — a raw dispose that skips them leaves an
/// off-tree contribution (R5, I1) — without ever touching the old instance.
pub trait SwapSlots: Send + Sync {
    type Prepared: Send;
    type Displaced: Send;

    fn entries_pinned_to(&self, hash: &str) -> Vec<(EntryId, u64)>;

    fn prepare(&self, entry: &EntryId) -> KernelFuture<'_, Self::Prepared>;

    fn commit(&self, entry: &EntryId, prepared: Self::Prepared) -> Option<Self::Displaced>;

    fn retire_displaced(&self, entry: &EntryId, displaced: Self::Displaced)
    -> KernelFuture<'_, ()>;

    fn discard(&self, prepared: Self::Prepared) -> KernelFuture<'_, ()>;
}

/// One batch outcome (mirrored to the facade's `SwapReport` by the harness).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SwapOutcome {
    pub swapped: Vec<EntryId>,
    pub rolled_back: bool,
}

/// Drives one Mode-1 swap over every entry pinned to `old_hash` — the batch
/// is by artifact hash, never per entry. Phases run through `core`, the ONE
/// loom-modeled machine: begin each slot, prepare each instance (all the
/// fallible work), then claim every slot AND run the infallible commit
/// bookkeeping inside one critical section ([`SwapCore::commit_all_with`]).
/// A failed preparation, a refused begin, or a lost claim rolls the WHOLE
/// batch back — old instances stay warm, zero entries commit, every staged
/// instance is discarded with its effects replayed. Every phase is a ledger
/// event.
///
/// # Errors
///
/// None of its own: every failure path is the ROLLBACK outcome, recorded.
pub async fn swap_batch<S: SwapSlots>(
    slots: &S,
    core: &SwapCore,
    old_hash: &str,
    new_hash: &str,
    ledger: &dyn LedgerSink,
) -> Result<SwapOutcome, KernelError> {
    let entries = slots.entries_pinned_to(old_hash);
    ledger.append(
        LedgerEventKind::SwapPhase {
            artifact: new_hash.to_owned(),
            phase: SwapPhaseKind::Began,
        },
        None,
    );
    let mut prepared: Vec<(EntryId, S::Prepared)> = Vec::new();
    let mut begun: Vec<u64> = Vec::new();
    let mut lost = false;
    for (entry, slot) in &entries {
        if !core.begin(*slot) {
            lost = true;
            break;
        }
        begun.push(*slot);
        match slots.prepare(entry).await {
            Ok(instance) => {
                ledger.append(
                    LedgerEventKind::SwapPhase {
                        artifact: new_hash.to_owned(),
                        phase: SwapPhaseKind::InstanceHealthy,
                    },
                    None,
                );
                prepared.push((entry.clone(), instance));
            }
            Err(_) => {
                lost = true;
                break;
            }
        }
    }
    let leftover = if lost {
        prepared
    } else {
        // The commit phase holds zero fallible operations (round-3 ruling):
        // it runs inside the claim's critical section, so a concurrent
        // disposal either tombstones first — the claim refuses, the whole
        // batch rolls back below — or arrives after every seat landed and
        // retires the committed seat itself.
        let mut moved = Some(prepared);
        let landed = core.commit_all_with(&begun, || {
            let mut swapped = Vec::new();
            let mut displaced = Vec::new();
            for (entry, instance) in moved.take().unwrap_or_default() {
                if let Some(seat) = slots.commit(&entry, instance) {
                    displaced.push((entry.clone(), seat));
                }
                swapped.push(entry);
            }
            (swapped, displaced)
        });
        if let Some((swapped, displaced)) = landed {
            ledger.append(
                LedgerEventKind::SwapPhase {
                    artifact: new_hash.to_owned(),
                    phase: SwapPhaseKind::Committed,
                },
                None,
            );
            for (entry, seat) in displaced {
                let _ = slots.retire_displaced(&entry, seat).await;
            }
            return Ok(SwapOutcome {
                swapped,
                rolled_back: false,
            });
        }
        moved.take().unwrap_or_default()
    };
    for slot in &begun {
        core.rollback(*slot);
    }
    for (_, instance) in leftover {
        let _ = slots.discard(instance).await;
    }
    ledger.append(
        LedgerEventKind::SwapPhase {
            artifact: new_hash.to_owned(),
            phase: SwapPhaseKind::RolledBack,
        },
        None,
    );
    Ok(SwapOutcome {
        swapped: Vec::new(),
        rolled_back: true,
    })
}

#[cfg(all(test, feature = "loom"))]
mod swap_model;

#[cfg(all(test, not(feature = "loom")))]
mod swap_tests;
