//! Driving one inverse so that its outcome is recorded whatever happens to the replay.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use jinnd_api::{EffectId, KernelError};

use crate::contain::{catching, caught, contained};
use crate::disposer::Disposer;
use crate::report::{EffectReport, UndoOutcome};
use crate::tree::Record;
use crate::undo::StepwiseUndo;

type Driver = Pin<Box<dyn Future<Output = UndoOutcome> + Send>>;

/// One effect's withdrawal, which always leaves a line in the log it was given.
///
/// Replay is an ordinary future, so it can be dropped between two inverses or in the
/// middle of one. This future owns the inverse it drives, which makes its own
/// destructor the single place where "the replay went away" is observable — so that
/// is where an interrupted withdrawal is recorded (R6), together with any panic the
/// inverse raises while being dropped (R11).
pub(crate) struct Withdrawal<'a> {
    driver: Option<Driver>,
    log: &'a mut Vec<EffectReport>,
    id: EffectId,
    label: Option<String>,
}

impl<'a> Withdrawal<'a> {
    /// Withdraws `record`, logging the outcome into `log`.
    pub(crate) fn new(log: &'a mut Vec<EffectReport>, record: Record) -> Self {
        Self {
            driver: Some(Box::pin(withdraw(record.disposer))),
            log,
            id: record.id,
            label: Some(record.label),
        }
    }

    /// Writes this effect's line. The label is consumed, so the line is written once.
    fn record(&mut self, outcome: UndoOutcome) {
        if let Some(label) = self.label.take() {
            self.log.push(EffectReport {
                id: self.id,
                label,
                outcome,
            });
        }
    }

    /// Drops the inverse, reporting a panic raised by its own destructor.
    fn release(&mut self) -> Option<String> {
        caught(|| drop(self.driver.take()))
    }
}

impl Future for Withdrawal<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // Polling after completion is a caller contract violation; staying pending is
        // the only answer that neither panics (R11) nor re-drives a spent inverse.
        let Some(driver) = this.driver.as_mut() else {
            return Poll::Pending;
        };
        // A sequence that stops early drops the steps it never ran while it returns,
        // and those closures are plugin-authored too: their destructors unwind out of
        // this poll, not out of an inverse (R11).
        let outcome = match catching(|| driver.as_mut().poll(cx)) {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(outcome)) => outcome,
            Err(panic) => UndoOutcome::Panicked(panic),
        };
        let outcome = match this.release() {
            Some(panic) => UndoOutcome::Panicked(panic),
            None => outcome,
        };
        this.record(outcome);
        Poll::Ready(())
    }
}

/// Records the withdrawal as interrupted if the replay was dropped mid-flight.
///
/// A no-op once [`Withdrawal::poll`] has written the line.
impl Drop for Withdrawal<'_> {
    fn drop(&mut self) {
        let panic = self.release();
        self.record(UndoOutcome::Interrupted { panic });
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
    let mut steps = steps.into_iter();
    let mut completed = 0;
    let mut outcome = UndoOutcome::Done;
    loop {
        // Cancellation is read before the next step is taken, never after: a step
        // pulled out of the sequence and then discarded would be discarded outside
        // the containment below.
        let remaining = steps.len();
        if remaining == 0 {
            break;
        }
        if cancel.is_cancelled() {
            outcome = UndoOutcome::Cancelled {
                completed,
                remaining,
            };
            break;
        }
        let Some(step) = steps.next() else { break };
        match classify(contained(step).await) {
            UndoOutcome::Done => completed += 1,
            stopped => {
                outcome = stopped;
                break;
            }
        }
    }

    // Discarding the steps that never ran runs their destructors, and those are
    // plugin-authored too. Contained here rather than left to unwind out of this
    // function: a panic escaping an async body poisons it, and a poisoned body's
    // remaining state is never dropped (R11).
    match caught(move || drop(steps)) {
        Some(panic) => UndoOutcome::Panicked(panic),
        None => outcome,
    }
}

fn classify(result: Result<Result<(), KernelError>, String>) -> UndoOutcome {
    match result {
        Ok(Ok(())) => UndoOutcome::Done,
        Ok(Err(failure)) => UndoOutcome::Failed(failure),
        Err(panic) => UndoOutcome::Panicked(panic),
    }
}
