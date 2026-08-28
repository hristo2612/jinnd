//! The supervisor's contained free functions: one activation behind panic
//! containment (R11) and the failure an unclean withdrawal records. Split
//! from `supervisor.rs` by responsibility (R10 file hygiene).

use jinnd_api::{Epoch, ErrorCode, FiberId, KernelError};
use jinnd_effects::{EffectScope, ReplayReport};
use tokio_util::sync::CancellationToken;

use crate::body::{FiberBody, Setup};
use crate::contain::contained;

/// Runs one activation behind panic containment (R11).
pub(super) async fn activate<'a>(
    body: &'a dyn FiberBody,
    scope: &'a mut EffectScope,
    fiber: FiberId,
    epoch: &'a Epoch,
    cancel: CancellationToken,
) -> Result<(), KernelError> {
    let setup = Setup::new(fiber, epoch, scope, cancel);
    contained(fiber, move || body.activate(setup)).await
}

/// The failure an unclean withdrawal is recorded as.
pub(super) fn unclean(fiber: FiberId, report: &ReplayReport) -> KernelError {
    let residue: Vec<&str> = report
        .unclean()
        .map(|effect| effect.label.as_str())
        .collect();
    KernelError {
        code: ErrorCode::EffectFailed,
        message: format!(
            "these effects were not withdrawn cleanly: {}",
            residue.join(", ")
        ),
        fiber: Some(fiber),
    }
}
