//! What one replay withdrew, per effect.

use jinnd_api::{EffectId, KernelError};

/// How one registered inverse ended.
///
/// Every variant except [`UndoOutcome::Done`] means the effect's contribution may
/// still be partly in place; the kernel records it rather than pretending the
/// withdrawal was exact (R6 — the ledger gets the truth, not a `last_error` string).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum UndoOutcome {
    /// The inverse ran to completion.
    Done,
    /// A stepwise inverse observed cancellation at a step boundary and stopped there.
    ///
    /// `completed` steps ran; `remaining` did not. The effect is half-withdrawn by
    /// construction, which is why cancellation is reported rather than swallowed.
    Cancelled { completed: usize, remaining: usize },
    /// The inverse returned an error. Replay carried on with the next effect (R9).
    Failed(KernelError),
    /// The inverse panicked. The panic was contained here (R11) and its payload
    /// rendered for the report; replay carried on with the next effect.
    Panicked(String),
}

impl UndoOutcome {
    /// True only when the inverse ran to completion.
    #[must_use]
    pub fn is_done(&self) -> bool {
        matches!(self, Self::Done)
    }
}

/// One effect's line in a replay report.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectReport {
    pub id: EffectId,
    pub label: String,
    pub outcome: UndoOutcome,
}

/// Every effect a replay withdrew, in the order their inverses ran.
///
/// The order is the teardown trace: strict LIFO, children before the effect they
/// nested under.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplayReport {
    pub effects: Vec<EffectReport>,
}

impl ReplayReport {
    /// True when every inverse in this replay ran to completion.
    ///
    /// A clean replay is the precondition for claiming recovery exactness (I1); an
    /// unclean one names exactly which contributions may survive.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.effects.iter().all(|effect| effect.outcome.is_done())
    }

    /// The effects whose inverses did not run to completion, in replay order.
    pub fn unclean(&self) -> impl Iterator<Item = &EffectReport> {
        self.effects
            .iter()
            .filter(|effect| !effect.outcome.is_done())
    }
}
