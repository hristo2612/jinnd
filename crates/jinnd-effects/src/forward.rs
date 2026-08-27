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

use jinnd_api::{ErrorCode, KernelError, Undo};
// The forward-effect types are facade contract types (authorized M1-P7
// additive delta); this module implements their semantics.
pub use jinnd_api::{ForwardAction, ForwardEffect};
use tokio_util::sync::CancellationToken;

use crate::contain::contained;
use crate::disposer::Disposer;
use crate::report::UndoOutcome;
use crate::undo::FutureUndo;
use crate::withdrawal::withdraw;

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
