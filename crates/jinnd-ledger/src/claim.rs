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
    /// True only for a branch hydrated from a durable intent that never
    /// completed: the next same-key claimant resumes it (constitution 03 —
    /// exactly-once is durable at-least-once intent plus idempotent same-key
    /// completion; an interrupted inverse may run again under its key).
    pub(crate) resumable: bool,
}

/// What one claim attempt observed.
#[derive(Clone)]
pub enum Claim {
    /// The caller owns the branch: it alone may run the inverse.
    Fresh,
    /// The caller resumes an interrupted branch whose intent is already
    /// durable: it alone may run the inverse, and no new intent is appended
    /// (constitution 03 crash safety — same key, at-least-once intent).
    Resumed,
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
        match branches.get_mut(&effect) {
            Some(branch) if branch.key != *key => Claim::Refused,
            Some(branch) if branch.resumable => {
                // Exactly one claimant wins the resume; the hydrated witness
                // died with its process, so the resuming caller's verifiable
                // one replaces it.
                branch.resumable = false;
                branch.witness = witness.clone();
                Claim::Resumed
            }
            Some(branch) => Claim::Recorded(branch.state),
            None => {
                branches.insert(
                    effect,
                    Branch {
                        key: key.clone(),
                        state: RevertResolution::PendingRevert,
                        witness: witness.clone(),
                        resumable: false,
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
            matches!(branch.state, RevertResolution::PendingRevert).then(|| branch.witness.clone())
        })
    }

    /// Records `state` as the branch's resolution.
    pub(crate) fn resolve(&self, effect: EffectId, state: RevertResolution) {
        if let Some(branch) = lock(&self.branches).get_mut(&effect) {
            branch.state = state;
        }
    }

    /// Seeds a branch reconstructed from the ledger, keeping any branch this
    /// process already holds: memory is never overwritten by hydration, so a
    /// concurrent live claim and a hydration of the same effect still admit at
    /// most one inverse execution (constitution 03).
    pub(crate) fn seed(&self, effect: EffectId, branch: Branch) {
        lock(&self.branches).entry(effect).or_insert(branch);
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
    fn a_hydrated_branch_never_grants_a_fresh_claim() {
        // Reopen semantics (constitution 03): intent is durable before any
        // inverse runs, so every claimant that found ledger history seeds
        // before claiming. Whatever the interleaving of two such claimants,
        // no one wins Fresh — the recorded state answers instead.
        loom::model(|| {
            let branches = Arc::new(Branches::default());
            let witness: Witness = Arc::new(|| true);
            let contenders: Vec<_> = (0..2)
                .map(|_| {
                    let branches = Arc::clone(&branches);
                    let witness = witness.clone();
                    loom::thread::spawn(move || {
                        branches.seed(
                            EffectId(1),
                            super::Branch {
                                key: RevertKey("k".to_owned()),
                                state: jinnd_api::RevertResolution::Reverted,
                                witness: witness.clone(),
                                resumable: false,
                            },
                        );
                        matches!(
                            branches.claim(EffectId(1), &RevertKey("k".to_owned()), &witness),
                            Claim::Fresh
                        )
                    })
                })
                .collect();
            let fresh = contenders
                .into_iter()
                .map(|handle| handle.join().unwrap_or(true))
                .filter(|fresh| *fresh)
                .count();
            assert_eq!(fresh, 0, "a hydrated branch never re-runs its inverse");
        });
    }

    #[test]
    fn exactly_one_same_key_claimant_resumes_an_interrupted_branch() {
        // Crash-resume race (PLA-276 round 3): both claimants hydrate the
        // same durable intent-without-completion, then claim. Whatever the
        // interleaving, exactly one wins the resume — the inverse still runs
        // at most once per reopened process.
        loom::model(|| {
            let branches = Arc::new(Branches::default());
            let witness: Witness = Arc::new(|| true);
            let contenders: Vec<_> = (0..2)
                .map(|_| {
                    let branches = Arc::clone(&branches);
                    let witness = witness.clone();
                    loom::thread::spawn(move || {
                        branches.seed(
                            EffectId(1),
                            super::Branch {
                                key: RevertKey("k".to_owned()),
                                state: jinnd_api::RevertResolution::PendingRevert,
                                witness: witness.clone(),
                                resumable: true,
                            },
                        );
                        matches!(
                            branches.claim(EffectId(1), &RevertKey("k".to_owned()), &witness),
                            Claim::Resumed
                        )
                    })
                })
                .collect();
            let resumed = contenders
                .into_iter()
                .map(|handle| handle.join().unwrap_or(false))
                .filter(|resumed| *resumed)
                .count();
            assert_eq!(resumed, 1, "exactly one claimant resumes the branch");
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
