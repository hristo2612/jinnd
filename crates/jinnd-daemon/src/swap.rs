//! Mode-1 hot-swap over the daemon's live roster (R8): the operator replaces
//! an artifact file (with its `.sha256` pin sidecar — Law 5: the operator
//! states the pin, the kernel verifies it), and the lifted batch machine
//! (`jinnd_wasm::swap_pinned`, M2-K1) replaces every live entry pinned to
//! the old hash — whole-batch rollback on any health-gate failure, old
//! instances still serving. What stays here is daemon policy: the artifact
//! file layout, the sidecar pin read, and the profile-cell no-op check.

use std::sync::Arc;

use jinnd_api::ErrorCode;
use jinnd_wasm::{LaneCore, SwapOutcome, swap_pinned};

use crate::support::{error, lock};

/// Swaps every live entry of `package` from its current artifact to the
/// bytes pinned by `pin`. A pin equal to the current hash is a no-op; a
/// committed batch retargets every package cell sharing the old artifact,
/// so future activations use the new one too (batch-by-hash, R8).
pub(crate) async fn swap_package(
    core: &Arc<LaneCore>,
    package: &str,
    bytes: Vec<u8>,
    pin: &str,
) -> Result<SwapOutcome, jinnd_api::KernelError> {
    let cell = lock(&core.packages).get(package).cloned().ok_or_else(|| {
        error(
            ErrorCode::InvalidProfile,
            format!("no registered wasm package {package:?}"),
        )
    })?;
    let old_hash = lock(&cell).hash().to_owned();
    if old_hash == pin {
        return Ok(SwapOutcome {
            swapped: Vec::new(),
            rolled_back: false,
        });
    }
    let fresh = core.host.load(bytes, pin, core.sink.as_ref())?;
    swap_pinned(core, &old_hash, fresh).await
}

impl crate::daemon::Daemon {
    /// Mode-1 hot-swap of one package from its artifact file + `.sha256`
    /// pin sidecar (R8; the operator states the pin, the kernel verifies).
    ///
    /// # Errors
    ///
    /// Unknown package, unreadable artifact or sidecar, refused pin. A
    /// failed health gate is NOT an error: the batch rolls back and the
    /// outcome says so.
    pub async fn swap(&self, package: &str) -> Result<SwapOutcome, jinnd_api::KernelError> {
        let name = crate::packages::basename(package);
        let file = self.paths.artifacts.join(format!("{name}.wasm"));
        let sidecar = self.paths.artifacts.join(format!("{name}.wasm.sha256"));
        let bytes = tokio::fs::read(&file)
            .await
            .map_err(|refused| error(ErrorCode::InvalidProfile, refused.to_string()))?;
        let pin = tokio::fs::read_to_string(&sidecar)
            .await
            .map_err(|refused| {
                error(
                    ErrorCode::InvalidProfile,
                    format!("no pin sidecar {} (Law 5): {refused}", sidecar.display()),
                )
            })?;
        let outcome = swap_package(&self.lane, package, bytes, pin.trim()).await?;
        self.sync_transitions();
        Ok(outcome)
    }
}
