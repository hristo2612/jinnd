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
                self.append(LedgerEventKind::RevertIntent { key: key.0.clone() }, effect)
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
                        self.resolve(effect, RevertResolution::PendingRevert)
                            .await?;
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

    async fn resolve(&self, effect: EffectId, state: RevertResolution) -> Result<(), KernelError> {
        self.branches.resolve(effect, state);
        self.append(
            LedgerEventKind::RevertResolved { resolution: state },
            effect,
        )
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
