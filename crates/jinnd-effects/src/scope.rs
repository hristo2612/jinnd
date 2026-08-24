//! The live effect tree and its last-in-first-out teardown.

use std::mem;

use jinnd_api::{EffectDescriptor, EffectId, ErrorCode, KernelError};

use crate::contain::contained;
use crate::disposer::Disposer;
use crate::report::{EffectReport, ReplayReport, UndoOutcome};
use crate::tree::{Record, describe, find, flatten, next_id};
use crate::undo::StepwiseUndo;

/// One scope's live effect tree.
///
/// Effects are withdrawn only by [`EffectScope::replay`]. Dropping a scope discards
/// the inverses without running them: a scope is the record of what must be undone,
/// not a guard that undoes it, because withdrawal is async and a destructor cannot
/// await (R1).
pub struct EffectScope {
    roots: Vec<Record>,
    replayed: bool,
}

impl EffectScope {
    /// An empty scope.
    #[must_use]
    pub fn new() -> Self {
        Self {
            roots: Vec::new(),
            replayed: false,
        }
    }

    /// Registers an effect at the top of this scope.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InactiveContext`] once the scope has been replayed: an effect
    /// registered then would never be withdrawn.
    pub fn register(
        &mut self,
        label: impl Into<String>,
        disposer: Disposer,
    ) -> Result<EffectId, KernelError> {
        let record = self.record(label, disposer)?;
        let id = record.id;
        self.roots.push(record);
        Ok(id)
    }

    /// Registers an effect nested under `parent`, so that withdrawing `parent`
    /// withdraws this effect first.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InactiveContext`] on a replayed scope, or
    /// [`ErrorCode::EffectFailed`] when `parent` is not live in this scope.
    pub fn register_child(
        &mut self,
        parent: EffectId,
        label: impl Into<String>,
        disposer: Disposer,
    ) -> Result<EffectId, KernelError> {
        let record = self.record(label, disposer)?;
        let id = record.id;
        let Some(parent) = find(&mut self.roots, parent) else {
            return Err(error(
                ErrorCode::EffectFailed,
                "no such effect is live in this scope",
            ));
        };
        parent.children.push(record);
        Ok(id)
    }

    /// The live effect tree, with labels and nesting, in registration order.
    ///
    /// Free introspection (R5): the tree the kernel would replay is the tree it
    /// publishes.
    #[must_use]
    pub fn tree(&self) -> Vec<EffectDescriptor> {
        describe(&self.roots)
    }

    /// True when this scope holds no live effect.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.roots.is_empty()
    }

    /// Withdraws every live effect, last registered first, and reports what each
    /// inverse did.
    ///
    /// Children are withdrawn before the effect they nested under, so a subtree
    /// cascades structurally. An inverse that fails or panics is recorded and the
    /// remaining inverses still run (R9, R11). The whole tree is moved out of the
    /// scope before the first inverse is touched, so nothing of this scope is held
    /// while plugin-authored code runs (R1).
    ///
    /// Replaying twice withdraws nothing the second time.
    pub async fn replay(&mut self) -> ReplayReport {
        self.replayed = true;
        let order = flatten(mem::take(&mut self.roots));
        let mut effects = Vec::with_capacity(order.len());
        for record in order {
            let outcome = withdraw(record.disposer).await;
            effects.push(EffectReport {
                id: record.id,
                label: record.label,
                outcome,
            });
        }
        ReplayReport { effects }
    }

    fn record(&self, label: impl Into<String>, disposer: Disposer) -> Result<Record, KernelError> {
        if self.replayed {
            return Err(error(
                ErrorCode::InactiveContext,
                "cannot register an effect on a replayed scope",
            ));
        }
        Ok(Record {
            id: next_id(),
            label: label.into(),
            disposer,
            children: Vec::new(),
        })
    }
}

impl Default for EffectScope {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for EffectScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EffectScope")
            .field("effects", &self.tree())
            .field("replayed", &self.replayed)
            .finish()
    }
}

/// Discards the tree without recursing (see [`crate::tree`]).
impl Drop for EffectScope {
    fn drop(&mut self) {
        drop(flatten(mem::take(&mut self.roots)));
    }
}

fn error(code: ErrorCode, message: &str) -> KernelError {
    KernelError {
        code,
        message: message.to_owned(),
        fiber: None,
    }
}

/// Runs one inverse and classifies how it ended.
async fn withdraw(disposer: Disposer) -> UndoOutcome {
    match disposer {
        Disposer::Whole(undo) => classify(contained(move || undo.undo()).await),
        Disposer::Stepwise(stepwise) => withdraw_steps(stepwise).await,
    }
}

/// Runs a stepwise inverse, checking for cancellation between steps.
///
/// A step that errors or panics stops its own sequence — the steps after it assume it
/// ran — but never the replay it belongs to.
async fn withdraw_steps(stepwise: StepwiseUndo) -> UndoOutcome {
    let (steps, cancel) = stepwise.into_parts();
    let total = steps.len();
    let mut completed = 0;
    for step in steps {
        if cancel.is_cancelled() {
            return UndoOutcome::Cancelled {
                completed,
                remaining: total - completed,
            };
        }
        match classify(contained(step).await) {
            UndoOutcome::Done => completed += 1,
            outcome => return outcome,
        }
    }
    UndoOutcome::Done
}

fn classify(result: Result<Result<(), KernelError>, String>) -> UndoOutcome {
    match result {
        Ok(Ok(())) => UndoOutcome::Done,
        Ok(Err(failure)) => UndoOutcome::Failed(failure),
        Err(panic) => UndoOutcome::Panicked(panic),
    }
}

#[cfg(test)]
mod tests {
    use super::{Disposer, EffectScope, Record, next_id};

    /// A tree deep enough that a recursive destructor would abort the process. Built
    /// here rather than through `register_child`, which walks the tree per call.
    ///
    /// Under miri the same walk is exercised at a depth that interprets in seconds
    /// rather than minutes; the stack-overflow teeth live in the native run.
    #[test]
    fn a_deeply_nested_tree_is_discarded_without_recursing() {
        let depth = if cfg!(miri) { 1_000 } else { 100_000 };
        let mut record = leaf();
        for _ in 0..depth {
            let mut parent = leaf();
            parent.children.push(record);
            record = parent;
        }

        let scope = EffectScope {
            roots: vec![record],
            replayed: false,
        };

        drop(scope);
    }

    fn leaf() -> Record {
        Record {
            id: next_id(),
            label: "nested".to_owned(),
            disposer: Disposer::sync(|| Ok(())),
            children: Vec::new(),
        }
    }
}
