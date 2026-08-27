//! Branch reconstruction from the ledger record stream (constitution 03).
//!
//! Pure: the fold sees records, never the store. The bound key is the first
//! durable intent's; the state is the last recorded resolution, derived from
//! the durable completion's witness verdict when the process died before
//! resolving; an intent with no completion at all reconstructs *resumable* —
//! the same-key retry runs the inverse to completion instead of being
//! answered from the seed (exactly-once = durable at-least-once intent plus
//! idempotent same-key completion). The witness of a reconstructed branch is
//! unverifiable by construction (it died with its process), so it reads as
//! failing rather than vacuously passing.

use jinnd_api::{EffectId, LedgerEventKind, LedgerRecord, RevertKey, RevertResolution};

use crate::claim::Branch;

/// Folds `effect`'s revert history out of `records`: `None` when no durable
/// intent ever landed for it.
pub(crate) fn branch_from(records: Vec<LedgerRecord>, effect: EffectId) -> Option<Branch> {
    let mut bound: Option<RevertKey> = None;
    let mut state: Option<RevertResolution> = None;
    let mut completed = false;
    for record in records {
        match record.kind {
            LedgerEventKind::RevertIntent { key, effect: at } if at == effect => {
                bound.get_or_insert(RevertKey(key));
                state.get_or_insert(RevertResolution::PendingRevert);
            }
            LedgerEventKind::RevertCompleted {
                effect: at, clean, ..
            } if at == effect => {
                completed = true;
                state = Some(if clean {
                    RevertResolution::Reverted
                } else {
                    RevertResolution::PendingRevert
                });
            }
            LedgerEventKind::RevertResolved {
                effect: at,
                resolution,
            } if at == effect => {
                completed = true;
                state = Some(resolution);
            }
            _ => {}
        }
    }
    match (bound, state) {
        (Some(key), Some(state)) => Some(Branch {
            key,
            state,
            witness: std::sync::Arc::new(|| false),
            resumable: !completed,
        }),
        _ => None,
    }
}
