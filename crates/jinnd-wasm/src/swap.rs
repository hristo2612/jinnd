//! Mode-1 hot-swap (R8): old instance warm until the new one is healthy,
//! auto-rollback on failure, batched over every entry sharing the artifact
//! hash (decision log 2026-08-25), every phase a ledger event.
//!
//! The interleaving-sensitive part — a swap racing an entry disposal — lives
//! in [`SwapCore`], a sync phase machine modeled under loom (`--features
//! loom`). The async driver above it never holds the core's lock across an
//! await (R1).

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
/// concurrent disposal. Exactly one side ends up owning the prepared
/// instance: a commit that loses to a tombstone reports `false` and the
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

    /// Preparing → Steady with the new instance live. False when disposal
    /// won the race: the caller must discard the prepared instance and must
    /// not resurrect the slot.
    pub fn commit(&self, slot: u64) -> bool {
        self.with(|slots| match slots.get_mut(&slot) {
            Some(phase @ SlotPhase::Preparing) => {
                *phase = SlotPhase::Steady;
                true
            }
            _ => false,
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
    /// disposer, which discards it.
    pub fn dispose(&self, slot: u64) -> SlotPhase {
        self.with(|slots| {
            let previous = slots.insert(slot, SlotPhase::Tombstone);
            previous.unwrap_or(SlotPhase::Steady)
        })
    }
}

/// The lane seam the swap driver drives (transport-agnostic: the adapter's
/// wasm lane implements it over real instances; unit tests over mocks).
///
/// Contract: `prepare` builds and health-gates the NEW instance — activate,
/// offer the old snapshot, health check — while the old instance stays warm
/// and untouched; `commit` atomically replaces old with new; `discard` drops
/// a prepared instance without ever touching the old one.
pub trait SwapSlots: Send + Sync {
    type Prepared: Send;

    fn entries_pinned_to(&self, hash: &str) -> Vec<EntryId>;

    fn prepare(&self, entry: &EntryId) -> KernelFuture<'_, Self::Prepared>;

    fn commit(&self, entry: &EntryId, prepared: Self::Prepared) -> KernelFuture<'_, ()>;

    fn discard(&self, prepared: Self::Prepared) -> KernelFuture<'_, ()>;
}

/// One batch outcome (mirrored to the facade's `SwapReport` by the harness).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SwapOutcome {
    pub swapped: Vec<EntryId>,
    pub rolled_back: bool,
}

/// Drives one Mode-1 swap over every entry pinned to `old_hash` — the batch
/// is by artifact hash, never per entry. Every phase is a ledger event.
///
/// # Errors
///
/// None of its own: a failed preparation is the ROLLBACK outcome, recorded,
/// with every already-prepared instance discarded and every old instance
/// still live.
pub async fn swap_batch<S: SwapSlots>(
    slots: &S,
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
    for entry in &entries {
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
                for (_, instance) in prepared {
                    let _ = slots.discard(instance).await;
                }
                ledger.append(
                    LedgerEventKind::SwapPhase {
                        artifact: new_hash.to_owned(),
                        phase: SwapPhaseKind::RolledBack,
                    },
                    None,
                );
                return Ok(SwapOutcome {
                    swapped: Vec::new(),
                    rolled_back: true,
                });
            }
        }
    }
    let mut swapped = Vec::new();
    for (entry, instance) in prepared {
        slots.commit(&entry, instance).await?;
        swapped.push(entry);
    }
    ledger.append(
        LedgerEventKind::SwapPhase {
            artifact: new_hash.to_owned(),
            phase: SwapPhaseKind::Committed,
        },
        None,
    );
    Ok(SwapOutcome {
        swapped,
        rolled_back: false,
    })
}

#[cfg(all(test, feature = "loom"))]
mod swap_model;

#[cfg(all(test, not(feature = "loom")))]
mod swap_tests;
