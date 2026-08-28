//! The suspend replay (M2-K4; decision log 2026-08-28): suspend ≠ dispose.
//!
//! A fiber that stops while its profile entry persists — daemon shutdown, an
//! incarnation replacement — releases its KERNEL registrations and keeps its
//! WORLD mutations: withdrawal undoes the world, suspension releases the
//! kernel. The scope classifies by what each effect declared: a plain
//! effect is a registration whose inverse is its release, and runs; an
//! effect registered with a suspend path runs that path instead, its
//! inverse unrun. The order is the replay's — LIFO, children first — and
//! the report reads exactly like a withdrawal's (R5, R6, Law 3).

use std::mem;

use jinnd_api::{EffectId, KernelError};

use crate::contain::caught;
use crate::disposer::Disposer;
use crate::report::{EffectReport, ReplayReport, UndoOutcome};
use crate::scope::EffectScope;
use crate::tree::take_next;
use crate::withdrawal::Withdrawal;

impl EffectScope {
    /// Registers an effect with a suspend path: `undo` is the inverse a full
    /// withdrawal runs; `suspend` is what a [`EffectScope::suspend`] replay
    /// runs INSTEAD — the effect's release of kernel-held resources with its
    /// world mutation retained.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InactiveContext`](jinnd_api::ErrorCode::InactiveContext)
    /// once the scope has been replayed or suspended.
    pub fn register_suspendable(
        &mut self,
        label: impl Into<String>,
        undo: Disposer,
        suspend: Disposer,
    ) -> Result<EffectId, KernelError> {
        let mut record = self.record(label, undo)?;
        record.suspend = Some(suspend);
        let id = record.id;
        self.roots.push(record);
        Ok(id)
    }

    /// Suspends every live effect, last registered first: a suspendable
    /// effect runs its suspend path, every other effect runs its inverse.
    /// Everything [`EffectScope::replay`] guarantees holds here — exactly
    /// once, failure contained and recorded (R9, R11), a dropped replay
    /// pauses rather than discharges — and the scope is sealed afterwards:
    /// a suspended scope refuses new registrations exactly as a replayed one
    /// (a fiber that suspended never withdraws what it never recorded).
    pub async fn suspend(&mut self) -> ReplayReport {
        self.replayed = true;
        while let Some(mut record) = take_next(&mut self.roots) {
            if let Some(suspend) = record.suspend.take() {
                // The unrun inverse is plugin-authored: its destructor is
                // contained like every other plugin-owned drop (R11).
                let inverse = mem::replace(&mut record.disposer, suspend);
                if let Some(panic) = caught(|| drop(inverse)) {
                    self.withdrawn.push(EffectReport {
                        id: record.id,
                        label: record.label.clone(),
                        outcome: UndoOutcome::Panicked(panic),
                    });
                    continue;
                }
            }
            Withdrawal::new(&mut self.withdrawn, record).await;
        }
        ReplayReport {
            effects: mem::take(&mut self.withdrawn),
        }
    }
}
