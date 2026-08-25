//! Single-flight admission for loader operations (R1, M1-P6b).
//!
//! Loader operations execute plugin-facing callbacks — lane constructors,
//! restaters, caller-supplied builders — and R1 forbids holding any lock
//! across such code. Operations are therefore serialized by *admission*, not
//! by a lock guard: a one-permit semaphore keeps them single-flight, while
//! the loader's data stays behind its own short-lived state lock, never held
//! across a callback or an await. Deadlock through the gate is structurally
//! impossible: an acquisition from another task completes when the running
//! operation does, and a re-entrant acquisition from the running operation's
//! own task — a callback calling back into the loader — is refused honestly
//! instead of waiting on itself forever.
//!
//! The honest boundary: same-task re-entrancy is detected and refused; a
//! callback that *blocks its thread* on another task's loader call has left
//! R1's contract on its own side (a synchronous callback must not block), and
//! no admission scheme can tell that waiter from an innocent concurrent
//! caller.

use std::future::Future;

use jinnd_api::{ErrorCode, KernelError};
use tokio::sync::Semaphore;

use crate::state::error;

tokio::task_local! {
    /// Set on the operating task for the span of an admitted operation, so a
    /// same-task re-entrant admission is detected without any shared state.
    static ADMITTED: ();
}

/// The loader's single-flight admission gate.
pub(crate) struct Gate {
    admission: Semaphore,
}

impl Gate {
    pub(crate) fn new() -> Self {
        Self {
            admission: Semaphore::new(1),
        }
    }

    /// Runs `operation` admitted: single-flight against every other admitted
    /// operation, with no lock guard held while it runs (R1).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] when the calling task is already inside
    /// an admitted operation — a plugin-facing callback calling back into the
    /// loader is refused, never deadlocked. Otherwise whatever `operation`
    /// answers.
    pub(crate) async fn admit<T>(
        &self,
        operation: impl Future<Output = Result<T, KernelError>>,
    ) -> Result<T, KernelError> {
        if ADMITTED.try_with(|()| ()).is_ok() {
            return Err(error(
                ErrorCode::InvalidProfile,
                "operation refused: a loader operation's callback called back into the loader",
            ));
        }
        let _permit = match self.admission.acquire().await {
            Ok(permit) => permit,
            // Unreachable — the gate is never closed — but answered honestly.
            Err(_closed) => {
                return Err(error(
                    ErrorCode::InvalidProfile,
                    "the loader gate is closed",
                ));
            }
        };
        ADMITTED.scope((), operation).await
    }
}
