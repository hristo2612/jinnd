//! Observable fiber lifecycle vocabulary (pre-work extraction, M1-P8; zero
//! semantic change).

use serde::{Deserialize, Serialize};

use crate::FiberId;

/// Observable lifecycle state of one fiber.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum FiberState {
    Pending,
    Loading,
    Active,
    Failed,
    Unloading,
    Disposed,
}

/// Why a fiber's desired activation changed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum TransitionCause {
    InitialLoad,
    DependencyChanged,
    ConfigChanged,
    ExplicitRestart,
    ExplicitDispose,
    ParentDisposed,
    /// The fiber's cell is stopped while its profile entry persists — daemon
    /// shutdown: kernel registrations release, world mutations are retained
    /// for the entry, nothing is withdrawn (M2-K4; decision log 2026-08-28;
    /// authorized suspend-vs-dispose facade delta).
    Suspend,
}

/// One committed transition recorded for observation and the ledger.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Transition {
    pub fiber: FiberId,
    pub from: FiberState,
    pub to: FiberState,
    pub cause: TransitionCause,
}
