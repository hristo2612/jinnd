//! Flattening one guest call (split from `instance.rs` by responsibility,
//! R10): deadline, trap, and guest fault each become a contained kernel
//! error (R11); deadline and trap also end the instance.

use std::time::Duration;

use jinnd_api::{ErrorCode, KernelError};
use tokio::time::timeout;

use crate::bindings::lifecycle;

#[cfg(test)]
mod tests;

pub(crate) fn trapped(trap: &wasmtime::Error) -> KernelError {
    KernelError {
        code: ErrorCode::PluginFailed,
        message: format!("guest trapped: {trap:#}"),
        fiber: None,
    }
}

pub(crate) fn hung() -> KernelError {
    KernelError {
        code: ErrorCode::PluginFailed,
        message: "guest exceeded its call deadline".to_owned(),
        fiber: None,
    }
}

pub(crate) fn faulted(fault: lifecycle::GuestFault) -> KernelError {
    let lifecycle::GuestFault::Failed(message) = fault;
    KernelError {
        code: ErrorCode::PluginFailed,
        message,
        fiber: None,
    }
}

/// Flattens one guest call: deadline, trap, and guest fault each become a
/// contained kernel error; deadline and trap also end the instance.
pub(crate) enum Settled<T> {
    Ok(T),
    Fault(KernelError),
    Dead(KernelError),
}

pub(crate) async fn settle<T>(
    deadline: Duration,
    call: impl Future<Output = wasmtime::Result<Result<T, lifecycle::GuestFault>>>,
) -> Settled<T> {
    match timeout(deadline, call).await {
        Err(_) => Settled::Dead(hung()),
        Ok(Err(trap)) => Settled::Dead(trapped(&trap)),
        Ok(Ok(Err(fault))) => Settled::Fault(faulted(fault)),
        Ok(Ok(Ok(value))) => Settled::Ok(value),
    }
}
