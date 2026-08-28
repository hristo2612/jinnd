//! Shared daemon plumbing (split by responsibility, R10): the ledger sink
//! every broker crossing lands on, the tracked-fiber map behind the
//! transition bridge, and the error/lock helpers.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use jinnd_api::{ErrorCode, FiberId, KernelError, LedgerEventKind};
use jinnd_fiber::Fiber;
use jinnd_wasm::LedgerSink;

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

pub(crate) fn error(code: ErrorCode, message: String) -> KernelError {
    KernelError {
        code,
        message,
        fiber: None,
    }
}

/// Broker crossings land on the kernel ledger's ordered record lane (R6).
pub(crate) struct Sink(pub(crate) jinnd_ledger::Ledger);

impl LedgerSink for Sink {
    fn append(&self, kind: LedgerEventKind, fiber: Option<FiberId>) {
        self.0.record(kind, None, fiber);
    }
}

/// The daemon's tracked fibers: the transition-ledger bridge's feed (R6).
pub(crate) type SharedFibers = Arc<Mutex<HashMap<FiberId, Tracked>>>;

pub(crate) struct Tracked {
    pub(crate) fiber: Arc<Fiber>,
    /// Transitions already emitted to the ledger.
    pub(crate) recorded: usize,
}
