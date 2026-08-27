//! The keyed exactly-once revert protocol (constitution 03, Law 3).
//!
//! Order is the contract: durable intent lands in the ledger *before* the
//! inverse may run; the inverse runs at most once per branch (the claim step
//! guarantees it); completion is recorded only after the executable witness
//! passes; and the three resolution states are exactly the ratified ones.
//! Reverts are events, never erasures — every step of the protocol appends.

use jinnd_api::{
    EffectId, ErrorCode, KernelError, KernelFuture, LedgerEventKind, RevertKey, RevertResolution,
    Witness,
};

use crate::claim::{Branches, Claim};
use crate::store::Ledger;

/// One branch's inverse (or compensator), executable exactly once. Panic
/// containment for plugin-reachable code lives inside the executable — the
/// effect engine's withdrawal machinery already contains it (R11).
pub type Inverse = Box<dyn FnOnce() -> KernelFuture<'static, ()> + Send + 'static>;

/// The revert lane over one ledger.
pub struct RevertLane {
    ledger: Ledger,
    branches: Branches,
}

impl RevertLane {
    #[must_use]
    pub fn new(ledger: Ledger) -> Self {
        Self {
            ledger,
            branches: Branches::default(),
        }
    }

    /// The branch's recorded resolution, if one exists.
    #[must_use]
    pub fn resolution(&self, effect: EffectId) -> Option<RevertResolution> {
        self.branches.state(effect)
    }

    /// Runs the revert protocol for `effect` under `key`.
    ///
    /// A same-key retry returns the recorded state without re-running the
    /// inverse; a fresh claim records intent durably, runs the inverse once,
    /// checks `witness`, and resolves `Reverted` only on a clean pass —
    /// anything else stays `PendingRevert`, visibly.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::EffectFailed`] for a distinct key against an existing
    /// branch, or when the ledger cannot record the protocol's events.
    pub async fn revert(
        &self,
        effect: EffectId,
        key: RevertKey,
        witness: Witness,
        inverse: Inverse,
    ) -> Result<RevertResolution, KernelError> {
        match self.branches.claim(effect, &key, &witness) {
            Claim::Refused => Err(refused("a distinct key against an existing revert branch")),
            Claim::Recorded(state) => Ok(state),
            Claim::Fresh => {
                // Intent is durable before the inverse may run (03 step 1).
                self.append(
                    LedgerEventKind::RevertIntent { key: key.0.clone() },
                    effect,
                )
                .await?;
                match inverse().await {
                    Ok(()) => {
                        let clean = passes(&witness);
                        self.append(
                            LedgerEventKind::RevertCompleted {
                                key: key.0.clone(),
                                clean,
                            },
                            effect,
                        )
                        .await?;
                        let state = if clean {
                            RevertResolution::Reverted
                        } else {
                            // A completed-looking inverse whose witness fails
                            // is a failed inverse (03 step 3).
                            RevertResolution::PendingRevert
                        };
                        self.resolve(effect, state).await?;
                        Ok(state)
                    }
                    Err(error) => {
                        self.append(LedgerEventKind::ErrorRecorded { error }, effect)
                            .await?;
                        self.resolve(effect, RevertResolution::PendingRevert).await?;
                        Ok(RevertResolution::PendingRevert)
                    }
                }
            }
        }
    }

    /// Runs an operator-confirmed declared compensator against a
    /// `PendingRevert` branch: the outcome is `Compensated`, never
    /// `Reverted`, and it is clean only when the branch's *original* witness
    /// passes (constitution 03 §irreversible effects).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::EffectFailed`] without operator confirmation, for an
    /// unknown branch, or for a branch not in `PendingRevert`.
    pub async fn compensate(
        &self,
        effect: EffectId,
        key: RevertKey,
        compensator: Inverse,
        operator_confirmed: bool,
    ) -> Result<RevertResolution, KernelError> {
        if !operator_confirmed {
            return Err(refused(
                "compensation is a distinct, operator-confirmed operation",
            ));
        }
        let Some(witness) = self.branches.pending_witness(effect) else {
            return Err(refused(
                "compensation applies only to a pending-revert branch",
            ));
        };
        self.append(LedgerEventKind::RevertIntent { key: key.0.clone() }, effect)
            .await?;
        match compensator().await {
            Ok(()) => {
                let clean = passes(&witness);
                self.append(
                    LedgerEventKind::RevertCompleted {
                        key: key.0.clone(),
                        clean,
                    },
                    effect,
                )
                .await?;
                let state = RevertResolution::Compensated { clean };
                self.resolve(effect, state).await?;
                Ok(state)
            }
            Err(error) => {
                self.append(LedgerEventKind::ErrorRecorded { error }, effect)
                    .await?;
                Ok(RevertResolution::PendingRevert)
            }
        }
    }

    async fn resolve(
        &self,
        effect: EffectId,
        state: RevertResolution,
    ) -> Result<(), KernelError> {
        self.branches.resolve(effect, state);
        self.append(LedgerEventKind::RevertResolved { resolution: state }, effect)
            .await?;
        Ok(())
    }

    async fn append(&self, kind: LedgerEventKind, effect: EffectId) -> Result<(), KernelError> {
        // Branch attribution rides the fiber lane: the facade's effects are
        // charged to the kernel pseudo-fiber, and an entry-charged effect to
        // its own. The effect id itself is not a ledger column; the key is.
        let _ = effect;
        self.ledger
            .append(kind, None, None)
            .await
            .map_err(|error| KernelError {
                code: ErrorCode::EffectFailed,
                message: error.to_string(),
                fiber: None,
            })?;
        Ok(())
    }
}

/// Checks the witness with its panic contained: a panicking witness is a
/// failing witness (R11).
fn passes(witness: &Witness) -> bool {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (witness)())).unwrap_or(false)
}

fn refused(message: &str) -> KernelError {
    KernelError {
        code: ErrorCode::EffectFailed,
        message: message.to_owned(),
        fiber: None,
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use jinnd_api::{
        EffectId, ErrorCode, KernelError, LedgerEventKind, LedgerQuery, RevertKey,
        RevertResolution, Witness,
    };

    use super::{Inverse, RevertLane};
    use crate::store::Ledger;

    fn lane() -> RevertLane {
        RevertLane::new(
            Ledger::open_in_memory().unwrap_or_else(|error| panic!("open: {error}")),
        )
    }

    fn key(value: &str) -> RevertKey {
        RevertKey(value.to_owned())
    }

    fn counting_inverse(runs: &Arc<AtomicUsize>) -> Inverse {
        let runs = Arc::clone(runs);
        Box::new(move || {
            runs.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        })
    }

    fn failing_inverse() -> Inverse {
        Box::new(|| {
            Box::pin(async {
                Err(KernelError {
                    code: ErrorCode::EffectFailed,
                    message: "the inverse refused".to_owned(),
                    fiber: None,
                })
            })
        })
    }

    #[tokio::test]
    async fn a_clean_inverse_with_a_passing_witness_resolves_reverted() {
        let lane = lane();
        let runs = Arc::new(AtomicUsize::new(0));
        let witness: Witness = Arc::new(|| true);
        let state = lane
            .revert(EffectId(1), key("k"), witness, counting_inverse(&runs))
            .await
            .unwrap_or_else(|error| panic!("revert: {error:?}"));
        assert_eq!(state, RevertResolution::Reverted);
        assert_eq!(runs.load(Ordering::SeqCst), 1);
        assert_eq!(lane.resolution(EffectId(1)), Some(RevertResolution::Reverted));
    }

    #[tokio::test]
    async fn a_same_key_retry_returns_the_recorded_state_without_rerunning() {
        let lane = lane();
        let runs = Arc::new(AtomicUsize::new(0));
        let witness: Witness = Arc::new(|| true);
        let first = lane
            .revert(EffectId(1), key("k"), witness.clone(), counting_inverse(&runs))
            .await
            .unwrap_or_else(|error| panic!("revert: {error:?}"));
        let second = lane
            .revert(EffectId(1), key("k"), witness, counting_inverse(&runs))
            .await
            .unwrap_or_else(|error| panic!("retry: {error:?}"));
        assert_eq!(first, second);
        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "the inverse runs exactly once per branch"
        );
    }

    #[tokio::test]
    async fn a_distinct_key_against_an_existing_branch_is_refused() {
        let lane = lane();
        let runs = Arc::new(AtomicUsize::new(0));
        let witness: Witness = Arc::new(|| true);
        lane.revert(EffectId(1), key("a"), witness.clone(), counting_inverse(&runs))
            .await
            .unwrap_or_else(|error| panic!("revert: {error:?}"));
        let refused = lane
            .revert(EffectId(1), key("b"), witness, counting_inverse(&runs))
            .await;
        assert!(refused.is_err(), "a distinct key is refused, never retried");
        assert_eq!(runs.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_failing_witness_leaves_the_branch_pending_revert_visibly() {
        let lane = lane();
        let runs = Arc::new(AtomicUsize::new(0));
        let witness: Witness = Arc::new(|| false);
        let state = lane
            .revert(EffectId(1), key("k"), witness, counting_inverse(&runs))
            .await
            .unwrap_or_else(|error| panic!("revert: {error:?}"));
        assert_eq!(state, RevertResolution::PendingRevert);
        assert_eq!(
            lane.resolution(EffectId(1)),
            Some(RevertResolution::PendingRevert),
            "the unresolved branch stays pending-revert, visibly"
        );
    }

    #[tokio::test]
    async fn a_failing_inverse_stays_pending_and_records_the_error() {
        let lane = lane();
        let witness: Witness = Arc::new(|| true);
        let state = lane
            .revert(EffectId(1), key("k"), witness, failing_inverse())
            .await
            .unwrap_or_else(|error| panic!("revert: {error:?}"));
        assert_eq!(state, RevertResolution::PendingRevert);
    }

    #[tokio::test]
    async fn compensation_resolves_compensated_never_reverted() {
        let lane = lane();
        let witness: Witness = Arc::new(|| true);
        lane.revert(EffectId(1), key("k"), witness, failing_inverse())
            .await
            .unwrap_or_else(|error| panic!("revert: {error:?}"));
        let runs = Arc::new(AtomicUsize::new(0));
        let state = lane
            .compensate(EffectId(1), key("comp"), counting_inverse(&runs), true)
            .await
            .unwrap_or_else(|error| panic!("compensate: {error:?}"));
        assert_eq!(
            state,
            RevertResolution::Compensated { clean: true },
            "the original witness passes, so the compensation is clean"
        );
        assert_ne!(state, RevertResolution::Reverted);
    }

    #[tokio::test]
    async fn a_compensation_failing_the_original_witness_stays_unclean() {
        let lane = lane();
        let witness: Witness = Arc::new(|| false);
        lane.revert(EffectId(1), key("k"), witness, failing_inverse())
            .await
            .unwrap_or_else(|error| panic!("revert: {error:?}"));
        let runs = Arc::new(AtomicUsize::new(0));
        let state = lane
            .compensate(EffectId(1), key("comp"), counting_inverse(&runs), true)
            .await
            .unwrap_or_else(|error| panic!("compensate: {error:?}"));
        assert_eq!(
            state,
            RevertResolution::Compensated { clean: false },
            "unless compensation satisfies the original witness, the branch is unclean"
        );
    }

    #[tokio::test]
    async fn compensation_without_operator_confirmation_is_refused() {
        let lane = lane();
        let witness: Witness = Arc::new(|| true);
        lane.revert(EffectId(1), key("k"), witness, failing_inverse())
            .await
            .unwrap_or_else(|error| panic!("revert: {error:?}"));
        let runs = Arc::new(AtomicUsize::new(0));
        assert!(
            lane.compensate(EffectId(1), key("comp"), counting_inverse(&runs), false)
                .await
                .is_err(),
            "compensation is a distinct, operator-confirmed operation"
        );
        assert_eq!(runs.load(Ordering::SeqCst), 0);
        assert_eq!(
            lane.resolution(EffectId(1)),
            Some(RevertResolution::PendingRevert)
        );
    }

    #[tokio::test]
    async fn compensating_a_resolved_or_unknown_branch_is_refused() {
        let lane = lane();
        let runs = Arc::new(AtomicUsize::new(0));
        assert!(
            lane.compensate(EffectId(9), key("comp"), counting_inverse(&runs), true)
                .await
                .is_err(),
            "an unknown branch cannot be compensated"
        );
        let witness: Witness = Arc::new(|| true);
        lane.revert(EffectId(1), key("k"), witness, counting_inverse(&runs))
            .await
            .unwrap_or_else(|error| panic!("revert: {error:?}"));
        assert!(
            lane.compensate(EffectId(1), key("comp"), counting_inverse(&runs), true)
                .await
                .is_err(),
            "a reverted branch is closed; compensation applies to pending-revert only"
        );
    }

    #[tokio::test]
    async fn intent_is_recorded_before_completion() {
        let ledger =
            Ledger::open_in_memory().unwrap_or_else(|error| panic!("open: {error}"));
        let lane = RevertLane::new(ledger.clone());
        let runs = Arc::new(AtomicUsize::new(0));
        let witness: Witness = Arc::new(|| true);
        lane.revert(EffectId(1), key("k"), witness, counting_inverse(&runs))
            .await
            .unwrap_or_else(|error| panic!("revert: {error:?}"));
        let records = ledger
            .events(LedgerQuery::default())
            .await
            .unwrap_or_else(|error| panic!("events: {error}"));
        let kinds: Vec<&LedgerEventKind> = records.iter().map(|record| &record.kind).collect();
        assert!(matches!(kinds[0], LedgerEventKind::RevertIntent { .. }));
        assert!(matches!(
            kinds[1],
            LedgerEventKind::RevertCompleted { clean: true, .. }
        ));
        assert!(matches!(kinds[2], LedgerEventKind::RevertResolved { .. }));
    }
}
