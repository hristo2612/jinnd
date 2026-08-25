//! The drain phase of a full withdrawal (I2, paper Alg 5; M1-P6c).
//!
//! Some effects — a service provision above all — owe their dependents a
//! grace period: the dependents must be told the effect is going away and be
//! waited out BEFORE any inverse of the owning scope runs, so a dependent's
//! own teardown still observes the provider whole. The drain phase is that
//! wait, registered beside the inverse ([`EffectScope::register_draining`])
//! and driven by [`EffectScope::drain`] ahead of [`EffectScope::replay`].
//!
//! A scope replayed WITHOUT draining still withdraws completely: the inverse
//! of a draining effect repeats the drain idempotently. The phase exists for
//! ordering, never for correctness of the inverse itself.

use jinnd_api::{EffectId, KernelError};

use crate::disposer::Disposer;
use crate::report::EffectReport;
use crate::scope::EffectScope;
use crate::tree::drains;
use crate::withdrawal::withdraw;

impl EffectScope {
    /// Registers an effect with a drain phase: `drain` runs before ANY inverse
    /// of a full withdrawal of this scope; `undo` is the inverse proper and
    /// must not depend on the drain having run.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InactiveContext`](jinnd_api::ErrorCode::InactiveContext)
    /// once the scope has been replayed.
    pub fn register_draining(
        &mut self,
        label: impl Into<String>,
        drain: Disposer,
        undo: Disposer,
    ) -> Result<EffectId, KernelError> {
        let mut record = self.record(label, undo)?;
        record.drain = Some(drain);
        let id = record.id;
        self.roots.push(record);
        Ok(id)
    }

    /// Runs every pending drain phase to completion, in replay order, before
    /// any inverse: the supervisor calls this ahead of [`EffectScope::replay`]
    /// so a dying provider's dependents finish unloading — and may still call
    /// the dying service — while the provider's contribution is intact (I2).
    ///
    /// Failures and panics are contained (R11) and recorded: the failed
    /// phase's line opens the next replay's report, so an unclean drain is an
    /// unclean withdrawal, never a silent one. Dropping this future mid-drain
    /// loses no inverse — every taken drain's effect is still withdrawn, and
    /// repeated, by its own undo at replay.
    pub async fn drain(&mut self) {
        for (id, label, disposer) in drains(&mut self.roots) {
            let outcome = withdraw(disposer).await;
            if !outcome.is_done() {
                self.withdrawn.push(EffectReport { id, label, outcome });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};

    use crate::{Disposer, EffectScope};

    fn stamp(log: &Arc<AtomicU32>, mark: u32) -> Disposer {
        let log = Arc::clone(log);
        Disposer::sync(move || {
            let _ = log.compare_exchange(0, mark, Ordering::SeqCst, Ordering::SeqCst);
            Ok(())
        })
    }

    #[tokio::test]
    async fn drains_run_before_any_inverse_of_the_replay() {
        let mut scope = EffectScope::new();
        // Whoever writes first wins the cell: the drain must beat every undo.
        let first = Arc::new(AtomicU32::new(0));
        scope
            .register_draining("provision", stamp(&first, 1), Disposer::sync(|| Ok(())))
            .unwrap_or_else(|error| panic!("register: {error:?}"));
        scope
            .register("later effect", stamp(&first, 2))
            .unwrap_or_else(|error| panic!("register: {error:?}"));

        scope.drain().await;
        let report = scope.replay().await;
        assert!(report.is_clean());
        assert_eq!(
            first.load(Ordering::SeqCst),
            1,
            "the drain phase must run before any inverse (I2)"
        );
    }

    #[tokio::test]
    async fn a_failed_drain_makes_the_withdrawal_unclean() {
        let mut scope = EffectScope::new();
        scope
            .register_draining(
                "provision",
                Disposer::sync(|| {
                    Err(jinnd_api::KernelError {
                        code: jinnd_api::ErrorCode::EffectFailed,
                        message: "the drain refused".to_owned(),
                        fiber: None,
                    })
                }),
                Disposer::sync(|| Ok(())),
            )
            .unwrap_or_else(|error| panic!("register: {error:?}"));
        scope.drain().await;
        let report = scope.replay().await;
        assert!(
            !report.is_clean(),
            "a failed drain phase is an unclean withdrawal, never a silent one"
        );
    }

    #[tokio::test]
    async fn a_scope_replayed_without_draining_still_withdraws_every_inverse() {
        let mut scope = EffectScope::new();
        let ran = Arc::new(AtomicU32::new(0));
        let log = Arc::clone(&ran);
        scope
            .register_draining(
                "provision",
                Disposer::sync(|| Ok(())),
                Disposer::sync(move || {
                    log.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }),
            )
            .unwrap_or_else(|error| panic!("register: {error:?}"));
        let report = scope.replay().await;
        assert!(report.is_clean());
        assert_eq!(ran.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn draining_twice_runs_each_phase_once() {
        let mut scope = EffectScope::new();
        let ran = Arc::new(AtomicU32::new(0));
        let log = Arc::clone(&ran);
        scope
            .register_draining(
                "provision",
                Disposer::sync(move || {
                    log.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }),
                Disposer::sync(|| Ok(())),
            )
            .unwrap_or_else(|error| panic!("register: {error:?}"));
        scope.drain().await;
        scope.drain().await;
        assert_eq!(
            ran.load(Ordering::SeqCst),
            1,
            "a drain phase runs at most once"
        );
    }
}
