//! The keyed exactly-once claim step, isolated so loom can pin it.
//!
//! Everything racy about the revert lane happens here, synchronously, under
//! one mutex that is never held across an await or an inverse (R1): exactly
//! one caller wins a fresh branch; everyone else observes the recorded state
//! or is refused for bringing a distinct key. The async protocol built on top
//! (`revert.rs`) runs no inverse without a `Claim::Fresh` in hand, which is
//! what makes at-most-one inverse execution structural (constitution 03).

use std::collections::HashMap;

use jinnd_api::{EffectId, RevertKey, RevertResolution, Witness};

#[cfg(feature = "loom")]
use loom::sync::Mutex;
#[cfg(not(feature = "loom"))]
use std::sync::Mutex;

/// One revert branch's recorded protocol state.
pub(crate) struct Branch {
    pub(crate) key: RevertKey,
    pub(crate) state: RevertResolution,
    pub(crate) witness: Witness,
}

/// What one claim attempt observed.
#[derive(Clone)]
pub enum Claim {
    /// The caller owns the branch: it alone may run the inverse.
    Fresh,
    /// The branch exists; the recorded state is returned without re-running
    /// anything (same-key retry idempotency).
    Recorded(RevertResolution),
    /// The key differs from the branch's bound key: refused (constitution 03
    /// — the kernel never issues an unkeyed or re-keyed retry).
    Refused,
}

/// The branch table behind the revert lane.
#[derive(Default)]
pub(crate) struct Branches {
    branches: Mutex<HashMap<EffectId, Branch>>,
}

impl Branches {
    /// Claims `effect` for a revert under `key`, binding `witness` when the
    /// claim is fresh.
    pub(crate) fn claim(&self, effect: EffectId, key: &RevertKey, witness: &Witness) -> Claim {
        let mut branches = lock(&self.branches);
        match branches.get(&effect) {
            Some(branch) if branch.key != *key => Claim::Refused,
            Some(branch) => Claim::Recorded(branch.state),
            None => {
                branches.insert(
                    effect,
                    Branch {
                        key: key.clone(),
                        state: RevertResolution::PendingRevert,
                        witness: witness.clone(),
                    },
                );
                Claim::Fresh
            }
        }
    }

    /// The branch's recorded state, if one exists.
    pub(crate) fn state(&self, effect: EffectId) -> Option<RevertResolution> {
        lock(&self.branches).get(&effect).map(|branch| branch.state)
    }

    /// The branch's bound witness, when it is pending compensation.
    pub(crate) fn pending_witness(&self, effect: EffectId) -> Option<Witness> {
        let branches = lock(&self.branches);
        branches.get(&effect).and_then(|branch| {
            matches!(branch.state, RevertResolution::PendingRevert)
                .then(|| branch.witness.clone())
        })
    }

    /// Records `state` as the branch's resolution.
    pub(crate) fn resolve(&self, effect: EffectId, state: RevertResolution) {
        if let Some(branch) = lock(&self.branches).get_mut(&effect) {
            branch.state = state;
        }
    }
}

#[cfg(not(feature = "loom"))]
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

#[cfg(feature = "loom")]
fn lock<T>(mutex: &Mutex<T>) -> loom::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

/// Loom model: two concurrent claimants, one branch — exactly one wins fresh
/// whatever the interleaving, and a distinct key never wins at all (the
/// writer-seam and claim-step interleaving the packet card names).
#[cfg(all(test, feature = "loom"))]
mod loom_model {
    use std::sync::Arc;

    use jinnd_api::{EffectId, RevertKey, Witness};

    use super::{Branches, Claim};

    #[test]
    fn exactly_one_same_key_claim_is_fresh() {
        loom::model(|| {
            let branches = Arc::new(Branches::default());
            let witness: Witness = Arc::new(|| true);
            let contenders: Vec<_> = (0..2)
                .map(|_| {
                    let branches = Arc::clone(&branches);
                    let witness = witness.clone();
                    loom::thread::spawn(move || {
                        matches!(
                            branches.claim(EffectId(1), &RevertKey("k".to_owned()), &witness),
                            Claim::Fresh
                        )
                    })
                })
                .collect();
            let fresh = contenders
                .into_iter()
                .map(|handle| handle.join().unwrap_or(false))
                .filter(|fresh| *fresh)
                .count();
            assert_eq!(fresh, 1, "exactly one claimant may run the inverse");
        });
    }

    #[test]
    fn a_distinct_key_never_wins_a_claimed_branch() {
        loom::model(|| {
            let branches = Arc::new(Branches::default());
            let witness: Witness = Arc::new(|| true);
            let first = {
                let branches = Arc::clone(&branches);
                let witness = witness.clone();
                loom::thread::spawn(move || {
                    branches.claim(EffectId(1), &RevertKey("a".to_owned()), &witness)
                })
            };
            let second = {
                let branches = Arc::clone(&branches);
                let witness = witness.clone();
                loom::thread::spawn(move || {
                    branches.claim(EffectId(1), &RevertKey("b".to_owned()), &witness)
                })
            };
            let outcomes = [
                first.join().map(|claim| matches!(claim, Claim::Fresh)),
                second.join().map(|claim| matches!(claim, Claim::Fresh)),
            ];
            let fresh = outcomes
                .into_iter()
                .filter(|outcome| matches!(outcome, Ok(true)))
                .count();
            assert_eq!(fresh, 1, "one key binds the branch; the other is refused");
        });
    }
}
