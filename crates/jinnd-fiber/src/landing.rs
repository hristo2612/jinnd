//! The landing half of the inertia lock: drive one transition to completion
//! while absorbing every input that arrives (R1).

use std::future::Future;
use std::pin::pin;

use tokio_util::sync::CancellationToken;

use crate::readiness::ReadinessSignal;
use crate::shared::Shared;

/// Drives one transition to its landing while absorbing every input that arrives.
///
/// This is the inertia lock in code: `work` is never dropped, never raced and never
/// aborted. Inputs are folded into the steering cell as they arrive, and the only
/// thing they may do to the transition in flight is tell it, cooperatively, that its
/// target has already moved.
pub(crate) async fn land<F: Future>(
    work: F,
    signal: &mut dyn ReadinessSignal,
    shared: &Shared,
    cancel: &CancellationToken,
) -> F::Output {
    let mut work = pin!(work);
    loop {
        tokio::select! {
            output = &mut work => return output,
            () = absorb(&mut *signal, shared, cancel) => {}
        }
    }
}

/// Waits for one input to move, folds it in, and reports staleness to the activation.
///
/// The decision — fold, then raise on staleness — is
/// [`crate::steering::SteeringCell::absorb`], which the loom models drive; only
/// the waiting and the token are tokio's.
async fn absorb(signal: &mut dyn ReadinessSignal, shared: &Shared, cancel: &CancellationToken) {
    {
        tokio::select! {
            () = shared.wake.notified() => {}
            () = signal.changed() => {}
        }
    }
    if shared.steering.absorb(signal.epoch()) {
        cancel.cancel();
    }
}
