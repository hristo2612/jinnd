//! Forward effects (paper Definitions 51–52 and Algorithm 1; M1-P7).
//!
//! Until this module, the scope registered inverses only: the forward action had
//! already happened by the time the kernel saw the effect. A forward effect hands
//! the kernel the action itself, so atomicity is structural:
//!
//! * A **plain** effect installs **all or none**: its inverse exists exactly when
//!   its forward action landed, and a failed action installs nothing.
//! * A **stepwise** (iterator) effect yields its inverse at every step. A
//!   **target-staleness guard runs at every yield boundary** (L-Iter/L-Divert
//!   granularity): when the target went stale, the launched step still lands — a
//!   launched transition always lands (R1) — and then exactly the yielded prefix
//!   rolls back, last yield first. A step that fails rolls back the prefix the
//!   same way, immediately, and the original error is returned.
//!
//! Nothing here blocks and no lock exists to hold across the plugin-authored
//! actions (R1); every action and inverse is panic-contained (R11).

use jinnd_api::{ErrorCode, KernelError, KernelFuture, Undo};
use tokio_util::sync::CancellationToken;

use crate::contain::contained;
use crate::disposer::Disposer;
use crate::report::UndoOutcome;
use crate::undo::FutureUndo;
use crate::withdrawal::withdraw;

/// One forward action: runs when the kernel drives the effect, never at
/// registration (R9), and returns the inverse of what it did.
pub type ForwardAction = Box<dyn FnOnce() -> KernelFuture<'static, Box<dyn Undo>> + Send + 'static>;

/// A forward effect, per its atomicity contract.
pub enum ForwardEffect {
    /// All-or-none: the inverse installs exactly when the action lands.
    Plain(ForwardAction),
    /// Stepwise: each step yields its inverse; the staleness guard runs at
    /// every yield boundary and a divert rolls back exactly the yielded prefix.
    Steps(Vec<ForwardAction>),
}

/// How one forward effect ended.
pub enum ForwardEnd {
    /// Every action landed while the target stayed current: the disposer
    /// replays the yielded inverses last yield first.
    Installed(Disposer),
    /// The target went stale: the launched action landed, the yielded prefix
    /// was rolled back here, and nothing was installed.
    Diverted { unwound: Vec<UndoOutcome> },
    /// An action failed or panicked: the yielded prefix was rolled back here,
    /// immediately, and nothing was installed.
    Failed {
        error: KernelError,
        unwound: Vec<UndoOutcome>,
    },
}

/// Drives one forward effect to its end under `stale`.
///
/// The rollback a divert or failure owes happens inside this call, so by the
/// time it returns the effect is either installed or fully accounted for. The
/// installed disposer deliberately does **not** watch `stale`: the guard
/// belongs to the forward walk, and an installed inverse must replay in full
/// whenever the kernel withdraws it.
pub async fn advance(forward: ForwardEffect, stale: &CancellationToken) -> ForwardEnd {
    let steps = match forward {
        ForwardEffect::Plain(action) => return advance_plain(action, stale).await,
        ForwardEffect::Steps(steps) => steps,
    };
    let mut yielded: Vec<Box<dyn Undo>> = Vec::new();
    for step in steps {
        // The guard runs at the yield boundary, before the next step launches:
        // a step never launches against a target already known stale.
        if stale.is_cancelled() {
            return ForwardEnd::Diverted {
                unwound: unwind(yielded).await,
            };
        }
        match contained_undo(step).await {
            Ok(undo) => yielded.push(undo),
            Err(error) => {
                return ForwardEnd::Failed {
                    error,
                    unwound: unwind(yielded).await,
                };
            }
        }
        // And again after the launched step lands: a divert observed mid-step
        // lets the step finish, then rolls back exactly the yielded prefix.
        if stale.is_cancelled() {
            return ForwardEnd::Diverted {
                unwound: unwind(yielded).await,
            };
        }
    }
    ForwardEnd::Installed(installed(yielded))
}

/// A plain effect: the launched action always lands; staleness observed after
/// the landing runs the fresh inverse immediately instead of installing it.
async fn advance_plain(action: ForwardAction, stale: &CancellationToken) -> ForwardEnd {
    match contained_undo(action).await {
        Ok(undo) => {
            if stale.is_cancelled() {
                ForwardEnd::Diverted {
                    unwound: unwind(vec![undo]).await,
                }
            } else {
                ForwardEnd::Installed(Disposer::Whole(undo))
            }
        }
        Err(error) => ForwardEnd::Failed {
            error,
            unwound: Vec::new(),
        },
    }
}

/// Rolls back a yielded prefix, last yield first, each inverse contained.
async fn unwind(mut yielded: Vec<Box<dyn Undo>>) -> Vec<UndoOutcome> {
    let mut outcomes = Vec::with_capacity(yielded.len());
    while let Some(undo) = yielded.pop() {
        outcomes.push(withdraw(Disposer::Whole(undo)).await);
    }
    outcomes
}

/// The installed inverse: replays the yielded inverses last yield first, each
/// contained, stopping at the first inverse that does not complete — the
/// inverses after it assume it ran (the stepwise-withdrawal rule).
fn installed(yielded: Vec<Box<dyn Undo>>) -> Disposer {
    Disposer::Whole(Box::new(FutureUndo::new(move || async move {
        let mut yielded = yielded;
        while let Some(undo) = yielded.pop() {
            let outcome = withdraw(Disposer::Whole(undo)).await;
            if let Some(error) = failure(outcome) {
                return Err(error);
            }
        }
        Ok(())
    })))
}

/// Discharges one detached inverse in place — the seam `dispose`-shaped
/// callers drive an installed disposer through, outside every lock (R1).
pub async fn discharge(disposer: Disposer) -> UndoOutcome {
    withdraw(disposer).await
}

/// Launches one action with its panic contained (R11): a panicking action is a
/// failing action, answered with the rendered payload, never an unwind.
async fn contained_undo(action: ForwardAction) -> Result<Box<dyn Undo>, KernelError> {
    match contained(action).await {
        Ok(landed) => landed,
        Err(panic) => Err(KernelError {
            code: ErrorCode::EffectFailed,
            message: panic,
            fiber: None,
        }),
    }
}

fn failure(outcome: UndoOutcome) -> Option<KernelError> {
    match outcome {
        UndoOutcome::Done => None,
        UndoOutcome::Failed(error) => Some(error),
        stopped => Some(KernelError {
            code: ErrorCode::EffectFailed,
            message: format!("an installed inverse did not complete: {stopped:?}"),
            fiber: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use jinnd_api::{ErrorCode, KernelError, KernelFuture, Undo};
    use tokio_util::sync::CancellationToken;

    use super::{ForwardAction, ForwardEffect, ForwardEnd, advance, discharge};

    type Log = Arc<Mutex<Vec<u32>>>;

    fn log() -> Log {
        Arc::new(Mutex::new(Vec::new()))
    }

    fn read(log: &Log) -> Vec<u32> {
        log.lock().unwrap_or_else(|poison| poison.into_inner()).clone()
    }

    fn mark(log: &Log, value: u32) {
        log.lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(value);
    }

    struct MarkUndo(Log, u32);

    impl Undo for MarkUndo {
        fn undo(self: Box<Self>) -> KernelFuture<'static, ()> {
            mark(&self.0, self.1);
            Box::pin(async { Ok(()) })
        }
    }

    fn step_marking(log: &Log, forward: u32, inverse: u32) -> ForwardAction {
        let log = Arc::clone(log);
        Box::new(move || {
            mark(&log, forward);
            let undo: Box<dyn Undo> = Box::new(MarkUndo(log, inverse));
            Box::pin(async move { Ok(undo) })
        })
    }

    fn failing_step(message: &'static str) -> ForwardAction {
        Box::new(move || {
            Box::pin(async move {
                Err(KernelError {
                    code: ErrorCode::EffectFailed,
                    message: message.to_owned(),
                    fiber: None,
                })
            })
        })
    }

    #[tokio::test]
    async fn a_plain_action_that_lands_installs_exactly_its_inverse() {
        let log = log();
        let end = advance(
            ForwardEffect::Plain(step_marking(&log, 1, 2)),
            &CancellationToken::new(),
        )
        .await;
        assert_eq!(read(&log), vec![1], "the inverse must not run at install");
        let ForwardEnd::Installed(disposer) = end else {
            panic!("a landed plain action must install");
        };
        discharge(disposer).await;
        assert_eq!(read(&log), vec![1, 2]);
    }

    #[tokio::test]
    async fn a_failed_plain_action_installs_nothing_and_returns_the_original_error() {
        let end = advance(
            ForwardEffect::Plain(failing_step("the forward action refused")),
            &CancellationToken::new(),
        )
        .await;
        let ForwardEnd::Failed { error, unwound } = end else {
            panic!("a failed plain action must fail the effect");
        };
        assert_eq!(error.message, "the forward action refused");
        assert!(unwound.is_empty(), "nothing was yielded, nothing unwinds");
    }

    #[tokio::test]
    async fn a_stale_plain_action_lands_then_undoes_immediately() {
        let log = log();
        let stale = CancellationToken::new();
        stale.cancel();
        let end = advance(ForwardEffect::Plain(step_marking(&log, 1, 2)), &stale).await;
        assert!(matches!(end, ForwardEnd::Diverted { .. }));
        assert_eq!(
            read(&log),
            vec![1, 2],
            "the launched action lands, its undo follows immediately"
        );
    }

    #[tokio::test]
    async fn a_completed_iterator_replays_every_yield_last_first() {
        let log = log();
        let end = advance(
            ForwardEffect::Steps(vec![
                step_marking(&log, 1, 2),
                step_marking(&log, 3, 4),
                step_marking(&log, 5, 6),
            ]),
            &CancellationToken::new(),
        )
        .await;
        assert_eq!(read(&log), vec![1, 3, 5]);
        let ForwardEnd::Installed(disposer) = end else {
            panic!("a completed iterator must install");
        };
        discharge(disposer).await;
        assert_eq!(read(&log), vec![1, 3, 5, 6, 4, 2]);
    }

    #[tokio::test]
    async fn a_failing_step_unwinds_exactly_the_yielded_prefix_immediately() {
        let log = log();
        let end = advance(
            ForwardEffect::Steps(vec![
                step_marking(&log, 1, 2),
                failing_step("the second step refused"),
                step_marking(&log, 7, 8),
            ]),
            &CancellationToken::new(),
        )
        .await;
        let ForwardEnd::Failed { error, unwound } = end else {
            panic!("a failing step must fail the effect");
        };
        assert_eq!(error.message, "the second step refused");
        assert_eq!(unwound.len(), 1);
        assert_eq!(
            read(&log),
            vec![1, 2],
            "inverse 1 runs immediately; the third step never launches"
        );
    }

    #[tokio::test]
    async fn a_panicking_step_is_contained_and_unwinds_the_prefix() {
        let log = log();
        let panicking: ForwardAction = Box::new(|| panic!("the step panicked"));
        let end = advance(
            ForwardEffect::Steps(vec![step_marking(&log, 1, 2), panicking]),
            &CancellationToken::new(),
        )
        .await;
        let ForwardEnd::Failed { error, .. } = end else {
            panic!("a panicking step must fail the effect, never unwind out");
        };
        assert_eq!(error.code, ErrorCode::EffectFailed);
        assert_eq!(read(&log), vec![1, 2]);
    }

    #[tokio::test]
    async fn a_divert_at_the_yield_boundary_rolls_back_only_the_yielded_prefix() {
        let log = log();
        let stale = CancellationToken::new();
        let tripwire = Arc::clone(&log);
        let trip = stale.clone();
        // The first step lands and trips the guard as it does, so the divert is
        // observed at the yield boundary after the landing.
        let tripping: ForwardAction = Box::new(move || {
            mark(&tripwire, 1);
            trip.cancel();
            let undo: Box<dyn Undo> = Box::new(MarkUndo(tripwire, 2));
            Box::pin(async move { Ok(undo) })
        });
        let end = advance(
            ForwardEffect::Steps(vec![tripping, step_marking(&log, 7, 8)]),
            &stale,
        )
        .await;
        let ForwardEnd::Diverted { unwound } = end else {
            panic!("a stale target at the boundary must divert");
        };
        assert_eq!(unwound.len(), 1);
        assert_eq!(
            read(&log),
            vec![1, 2],
            "the launched step lands, only its inverse runs, the next never launches"
        );
    }

    #[tokio::test]
    async fn a_divert_before_the_first_yield_unwinds_nothing() {
        let stale = CancellationToken::new();
        stale.cancel();
        let log = log();
        let end = advance(ForwardEffect::Steps(vec![step_marking(&log, 1, 2)]), &stale).await;
        let ForwardEnd::Diverted { unwound } = end else {
            panic!("a pre-launch stale target must divert");
        };
        assert!(unwound.is_empty());
        assert_eq!(read(&log), Vec::<u32>::new(), "no step ever launches");
    }

    #[tokio::test]
    async fn an_installed_disposer_ignores_the_forward_guard() {
        // The guard belongs to the forward walk: cancelling it after install
        // must not stop the installed inverse from replaying in full.
        let log = log();
        let stale = CancellationToken::new();
        let end = advance(
            ForwardEffect::Steps(vec![step_marking(&log, 1, 2), step_marking(&log, 3, 4)]),
            &stale,
        )
        .await;
        let ForwardEnd::Installed(disposer) = end else {
            panic!("the iterator must install");
        };
        stale.cancel();
        let outcome = discharge(disposer).await;
        assert!(outcome.is_done(), "installed inverses replay in full");
        assert_eq!(read(&log), vec![1, 3, 4, 2]);
    }

    #[tokio::test]
    async fn an_installed_inverse_failure_stops_the_replay_at_that_inverse() {
        let log = log();
        let failing_undo: ForwardAction = Box::new(|| {
            struct RefusingUndo;
            impl Undo for RefusingUndo {
                fn undo(self: Box<Self>) -> KernelFuture<'static, ()> {
                    Box::pin(async {
                        Err(KernelError {
                            code: ErrorCode::EffectFailed,
                            message: "the inverse refused".to_owned(),
                            fiber: None,
                        })
                    })
                }
            }
            let undo: Box<dyn Undo> = Box::new(RefusingUndo);
            Box::pin(async move { Ok(undo) })
        });
        let counted = Arc::new(AtomicUsize::new(0));
        let survivor = Arc::clone(&counted);
        let counting: ForwardAction = Box::new(move || {
            struct CountUndo(Arc<AtomicUsize>);
            impl Undo for CountUndo {
                fn undo(self: Box<Self>) -> KernelFuture<'static, ()> {
                    self.0.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async { Ok(()) })
                }
            }
            let undo: Box<dyn Undo> = Box::new(CountUndo(survivor));
            Box::pin(async move { Ok(undo) })
        });
        let end = advance(
            ForwardEffect::Steps(vec![counting, failing_undo]),
            &CancellationToken::new(),
        )
        .await;
        let ForwardEnd::Installed(disposer) = end else {
            panic!("the iterator must install");
        };
        let outcome = discharge(disposer).await;
        assert!(!outcome.is_done(), "the failing inverse is reported");
        assert_eq!(
            counted.load(Ordering::SeqCst),
            0,
            "inverses after a failed one assume it ran and do not run"
        );
        let _ = read(&log);
    }
}
