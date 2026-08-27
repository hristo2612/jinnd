//! Boundary error classes and the async contract future (pre-work extraction,
//! M1-P8; zero semantic change).

use std::future::Future;
use std::pin::Pin;

use serde::{Deserialize, Serialize};

use crate::FiberId;

/// A sendable future returned by an asynchronous kernel contract.
pub type KernelFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T, KernelError>> + Send + 'a>>;

/// Stable error classes exposed by the kernel boundary.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum ErrorCode {
    InactiveContext,
    MissingDependency,
    DependencyCycle,
    PluginFailed,
    EffectFailed,
    ListenerFailed,
    InvalidProfile,
    /// A provision for an occupied (service, realm) slot from a different
    /// provider was refused: replacement is never silent (paper Def 23, R9).
    /// The same provider superseding its own generation — the hot-swap lane —
    /// is not a duplicate. (Authorized M1-P6c additive delta.)
    DuplicateProvision,
}

/// Structured error value. Plugin panics are converted before crossing this boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KernelError {
    pub code: ErrorCode,
    pub message: String,
    pub fiber: Option<FiberId>,
}
