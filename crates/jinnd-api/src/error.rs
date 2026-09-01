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
    /// A read, stat, or removal named a path that does not exist: the
    /// typed not-found of the base host-provider contracts (`jinn:fs`
    /// bundle `fs-error.not-found`), so a caller classifies absence by
    /// code, never by folding a generic error. (Authorized M2-K3 additive
    /// facade delta; harness finding 3.)
    NotFound,
    /// The operation is declared `irreversible` at the contract level, so
    /// no inverse exists and a revert unit containing it is rejected
    /// whole (Law 3, constitution 03 §51). Operator-surface only: it
    /// answers a revert, never a guest call. (Authorized M2-K14 additive
    /// facade delta; "revert failed" is not an answer — R3.)
    Irreversible,
    /// The peer on the other end of an outbound TLS call did not prove it
    /// is the authority the allowlist named: an unanchored issuer, a name
    /// the certificate does not cover, a certificate out of its dates. A
    /// THIRD answer beside the grant refusal and the transport failure,
    /// because it is a third next move — the allowlist refusal is the
    /// caller's profile to fix, the transport failure is worth retrying,
    /// and this one is neither. (Authorized M2-K15 additive facade delta;
    /// R3, and the M2-K9 precedent that four next moves are four cases.)
    Untrusted,
}

/// Structured error value. Plugin panics are converted before crossing this boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KernelError {
    pub code: ErrorCode,
    pub message: String,
    pub fiber: Option<FiberId>,
}
