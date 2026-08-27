//! The keyed exactly-once revert protocol (constitution 03, Law 3).
//!
//! Order is the contract: durable intent lands in the ledger *before* the
//! inverse may run; the inverse runs at most once per branch (the claim step
//! guarantees it); completion is recorded only after the executable witness
//! passes; and the three resolution states are exactly the ratified ones.
//! Reverts are events, never erasures — every step of the protocol appends.
//!
//! Crash safety: the claim IS a ledger event. Intent, completion, and
//! resolution all carry the effect they concern, so a lane reopened over the
//! same ledger reconstructs every branch before claiming. A same-key retry
//! after a process death answers from the record when completion is durable
//! — the inverse never re-runs — and *resumes to completion* when the death
//! interrupted the branch between intent and completion: exactly-once is
//! durable at-least-once intent plus idempotent same-key completion (PLA-276
//! round-2 blocker 3, round-3 item 2). A distinct key is refused either way.

use jinnd_api::{
    EffectId, EntryId, ErrorCode, FiberId, KernelError, KernelFuture, LedgerEventKind, RevertKey,
    RevertResolution, Witness,
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

    /// The branch's recorded resolution, if this process holds one. Branches
    /// that live only in the ledger are hydrated by [`RevertLane::revert`] and
    /// [`RevertLane::compensate`], never here: this observation is synchronous.
    #[must_use]
    pub fn resolution(&self, effect: EffectId) -> Option<RevertResolution> {
        self.branches.state(effect)
    }

    /// Runs the revert protocol for `effect` under `key`, attributing the
    /// protocol's ledger events to `entry`/`fiber` (R6: every event traceable
    /// to the entry/fiber that caused it).
    ///
    /// A same-key retry — in-process or after a reopen — returns the recorded
    /// state without re-running the inverse; a fresh claim records intent
    /// durably, runs the inverse once, checks `witness`, and resolves
    /// `Reverted` only on a clean pass — anything else stays `PendingRevert`,
    /// visibly.
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
        entry: Option<EntryId>,
        fiber: Option<FiberId>,
    ) -> Result<RevertResolution, KernelError> {
        self.hydrate(effect).await?;
        match self.branches.claim(effect, &key, &witness) {
            Claim::Refused => Err(refused("a distinct key against an existing revert branch")),
            Claim::Recorded(state) => Ok(state),
            Claim::Fresh => {
                // Intent is durable before the inverse may run (03 step 1).
                self.append(
                    LedgerEventKind::RevertIntent {
                        key: key.0.clone(),
                        effect,
                    },
                    entry.clone(),
                    fiber,
                )
                .await?;
                self.complete(effect, key, witness, inverse, entry, fiber)
                    .await
            }
            // The interrupted branch's intent is already durable; the retry
            // runs the inverse to completion under the same key without
            // appending a second intent (constitution 03 crash safety).
            Claim::Resumed => {
                self.complete(effect, key, witness, inverse, entry, fiber)
                    .await
            }
        }
    }

    /// Runs the claimed branch's inverse and records its outcome: completion
    /// with the witness verdict, then the resolution — `Reverted` only on a
    /// clean pass, `PendingRevert` visibly otherwise (03 step 3).
    async fn complete(
        &self,
        effect: EffectId,
        key: RevertKey,
        witness: Witness,
        inverse: Inverse,
        entry: Option<EntryId>,
        fiber: Option<FiberId>,
    ) -> Result<RevertResolution, KernelError> {
        match inverse().await {
            Ok(()) => {
                let clean = passes(&witness);
                self.append(
                    LedgerEventKind::RevertCompleted {
                        key: key.0.clone(),
                        effect,
                        clean,
                    },
                    entry.clone(),
                    fiber,
                )
                .await?;
                let state = if clean {
                    RevertResolution::Reverted
                } else {
                    // A completed-looking inverse whose witness fails is a
                    // failed inverse (03 step 3).
                    RevertResolution::PendingRevert
                };
                self.resolve(effect, state, entry, fiber).await?;
                Ok(state)
            }
            Err(error) => {
                self.append(
                    LedgerEventKind::ErrorRecorded { error },
                    entry.clone(),
                    fiber,
                )
                .await?;
                self.resolve(effect, RevertResolution::PendingRevert, entry, fiber)
                    .await?;
                Ok(RevertResolution::PendingRevert)
            }
        }
    }

    /// Runs an operator-confirmed declared compensator against a
    /// `PendingRevert` branch: the outcome is `Compensated`, never
    /// `Reverted`, and it is clean only when the branch's *original* witness
    /// passes (constitution 03 §irreversible effects). A branch hydrated from
    /// the ledger lost its witness with its process; compensation against it
    /// stays marked unclean, honestly — an unverifiable equivalence is not a
    /// clean one.
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
        entry: Option<EntryId>,
        fiber: Option<FiberId>,
    ) -> Result<RevertResolution, KernelError> {
        if !operator_confirmed {
            return Err(refused(
                "compensation is a distinct, operator-confirmed operation",
            ));
        }
        self.hydrate(effect).await?;
        let Some(witness) = self.branches.pending_witness(effect) else {
            return Err(refused(
                "compensation applies only to a pending-revert branch",
            ));
        };
        self.append(
            LedgerEventKind::RevertIntent {
                key: key.0.clone(),
                effect,
            },
            entry.clone(),
            fiber,
        )
        .await?;
        match compensator().await {
            Ok(()) => {
                let clean = passes(&witness);
                self.append(
                    LedgerEventKind::RevertCompleted {
                        key: key.0.clone(),
                        effect,
                        clean,
                    },
                    entry.clone(),
                    fiber,
                )
                .await?;
                let state = RevertResolution::Compensated { clean };
                self.resolve(effect, state, entry, fiber).await?;
                Ok(state)
            }
            Err(error) => {
                self.append(LedgerEventKind::ErrorRecorded { error }, entry, fiber)
                    .await?;
                Ok(RevertResolution::PendingRevert)
            }
        }
    }

    /// Reconstructs `effect`'s branch from the ledger when this process holds
    /// none (the fold's semantics live in [`crate::hydrate`]). The seed never
    /// overwrites a live branch.
    async fn hydrate(&self, effect: EffectId) -> Result<(), KernelError> {
        if self.branches.state(effect).is_some() {
            return Ok(());
        }
        let records = self
            .ledger
            .events(jinnd_api::LedgerQuery::default())
            .await
            .map_err(|error| refused(&error.to_string()))?;
        if let Some(branch) = crate::hydrate::branch_from(records, effect) {
            self.branches.seed(effect, branch);
        }
        Ok(())
    }

    async fn resolve(
        &self,
        effect: EffectId,
        state: RevertResolution,
        entry: Option<EntryId>,
        fiber: Option<FiberId>,
    ) -> Result<(), KernelError> {
        self.branches.resolve(effect, state);
        self.append(
            LedgerEventKind::RevertResolved {
                effect,
                resolution: state,
            },
            entry,
            fiber,
        )
        .await?;
        Ok(())
    }

    async fn append(
        &self,
        kind: LedgerEventKind,
        entry: Option<EntryId>,
        fiber: Option<FiberId>,
    ) -> Result<(), KernelError> {
        self.ledger
            .append(kind, entry, fiber)
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
