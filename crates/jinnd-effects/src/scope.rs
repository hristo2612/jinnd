//! The live effect tree and its last-in-first-out teardown.

use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Waker};

use jinnd_api::{EffectDescriptor, EffectId, ErrorCode, KernelError};

use crate::disposer::Disposer;
use crate::report::{EffectReport, ReplayReport};
use crate::tree::{Record, describe, find, flatten, next_id, take, take_next};
use crate::withdrawal::Withdrawal;

/// One scope's live effect tree.
///
/// Effects are withdrawn only by [`EffectScope::replay`]. Dropping a scope discards
/// the inverses without running them: a scope is the record of what must be undone,
/// not a guard that undoes it, because withdrawal is async and a destructor cannot
/// await (R1).
pub struct EffectScope {
    pub(crate) roots: Vec<Record>,
    pub(crate) withdrawn: Vec<EffectReport>,
    pub(crate) replayed: bool,
}

impl EffectScope {
    /// An empty scope.
    #[must_use]
    pub fn new() -> Self {
        Self {
            roots: Vec::new(),
            withdrawn: Vec::new(),
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
    ///
    /// A teardown paused by a dropped replay is not empty: everything it never
    /// reached is still live, and still owed a withdrawal.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.roots.is_empty()
    }

    /// What this scope has withdrawn but not yet reported.
    ///
    /// Empty after a replay that ran to completion — its report carried those lines
    /// away. Non-empty only while a teardown is paused, which is exactly when there
    /// is no report to read them from; the next replay opens its report with them.
    #[must_use]
    pub fn withdrawn(&self) -> &[EffectReport] {
        &self.withdrawn
    }

    /// Withdraws every live effect, last registered first, and reports what each
    /// inverse did.
    ///
    /// Children are withdrawn before the effect they nested under, so a subtree
    /// cascades structurally. An inverse that fails or panics is recorded and the
    /// remaining inverses still run (R9, R11). Each record leaves the tree as its
    /// inverse starts, and every line is written to the scope as it is produced, so
    /// dropping this future mid-teardown loses neither: the untouched effects stay
    /// live and a later replay resumes from them, opening its report with what the
    /// interrupted one had already done. Only the inverse actually in flight cannot
    /// be resumed — it was consumed when it started — and it is reported
    /// [`Interrupted`](crate::UndoOutcome::Interrupted) rather than dropped.
    ///
    /// No lock is held while an inverse runs (R1); the scope is exclusive to this
    /// replay for its duration, which is what `&mut self` already says.
    ///
    /// Replaying twice withdraws nothing the second time.
    pub async fn replay(&mut self) -> ReplayReport {
        self.replayed = true;
        while let Some(record) = take_next(&mut self.roots) {
            Withdrawal::new(&mut self.withdrawn, record).await;
        }
        ReplayReport {
            effects: mem::take(&mut self.withdrawn),
        }
    }

    /// Detaches one live effect, with its whole subtree, for immediate
    /// withdrawal.
    ///
    /// The records leave this scope's tree at once — bookkeeping only, no
    /// inverse runs here — and the returned handle owes their withdrawal.
    /// Callers drive it with [`Detached::withdraw_now`] **after** releasing
    /// whatever guards this scope: an inverse can reach plugin code (a final
    /// listener handle's destructor, say), and no lock is held across plugin
    /// code (R1).
    ///
    /// `None` when `id` names no live effect here; detaching is idempotent.
    pub fn detach(&mut self, id: EffectId) -> Option<Detached> {
        let record = take(&mut self.roots, id)?;
        Some(Detached {
            records: flatten(vec![record]),
        })
    }

    pub(crate) fn record(
        &self,
        label: impl Into<String>,
        disposer: Disposer,
    ) -> Result<Record, KernelError> {
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
            drain: None,
            suspend: None,
            children: Vec::new(),
        })
    }
}

impl Default for EffectScope {
    fn default() -> Self {
        Self::new()
    }
}

/// One effect subtree detached from its scope, owing its inverses.
///
/// Like a scope, this is the record of what must be undone, not a guard that
/// undoes it: dropping it discards the inverses without running them.
#[must_use = "a detached effect's inverses run only through withdraw_now"]
pub struct Detached {
    /// Replay order: children before the effect they nested under; childless.
    records: Vec<Record>,
}

impl Detached {
    /// Withdraws every detached record as an ordinary future, children before
    /// the effect they nested under, and reports what each inverse did — the
    /// awaited counterpart of [`Detached::withdraw_now`] for callers whose
    /// inverses are allowed to wait (an in-flight forward effect landing, say).
    /// Failures and panics are contained and recorded exactly as a replay
    /// records them (R9, R11); no lock is held while an inverse runs (R1).
    pub async fn withdraw(self) -> ReplayReport {
        let mut effects = Vec::new();
        for record in self.records {
            Withdrawal::new(&mut effects, record).await;
        }
        ReplayReport { effects }
    }

    /// Withdraws every detached record now, children before the effect they
    /// nested under, and reports what each inverse did.
    ///
    /// This is the synchronous withdrawal point `unlisten`-shaped callers
    /// need: every inverse is driven in place and must finish without waiting.
    /// One that yields was consumed when it started and is reported
    /// [`Interrupted`](crate::UndoOutcome::Interrupted) — nothing here blocks
    /// (R1). Failures and panics are contained and recorded exactly as a
    /// replay records them (R9, R11).
    pub fn withdraw_now(self) -> ReplayReport {
        let mut effects = Vec::new();
        for record in self.records {
            let mut withdrawal = Withdrawal::new(&mut effects, record);
            let mut cx = Context::from_waker(Waker::noop());
            if Pin::new(&mut withdrawal).poll(&mut cx).is_pending() {
                // Dropping the in-flight withdrawal records it Interrupted.
                drop(withdrawal);
            }
        }
        ReplayReport { effects }
    }
}

impl std::fmt::Debug for Detached {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Detached")
            .field("records", &self.records.len())
            .finish()
    }
}

impl std::fmt::Debug for EffectScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EffectScope")
            .field("effects", &self.tree())
            .field("withdrawn", &self.withdrawn.len())
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
            withdrawn: Vec::new(),
            replayed: false,
        };

        drop(scope);
    }

    fn leaf() -> Record {
        Record {
            id: next_id(),
            label: "nested".to_owned(),
            disposer: Disposer::sync(|| Ok(())),
            drain: None,
            suspend: None,
            children: Vec::new(),
        }
    }
}
