//! Typed events, dispatch modes, and listeners (pre-work extraction, M1-P8;
//! zero semantic change).

use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::{ContextId, KernelError, KernelFuture};

/// Dispatch semantics are part of an event's type-level contract. Serde is
/// for the ledger's `DispatchTrace` record (M2-K2): the mode is part of one
/// emit's typed audit line (R3, R6).
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum DispatchMode {
    Emit,
    Parallel,
    Serial,
    Bail,
    Waterfall,
}

/// A typed event and its declared dispatch mode.
///
/// The three provided methods are the payload's side of dispatch, all defaulted
/// so an event declares only what its mode uses (authorized M1-P5 additive
/// delta; R3, R12).
pub trait Event: Clone + Debug + Send + Sync + 'static {
    type Output: Debug + Send + Sync + 'static;

    const MODE: DispatchMode;

    /// Inverted routing (LAW §3): the payload selects its listeners by
    /// interrogating each listener's registration context. Listeners never
    /// filter the payload. Default: every listener is selected.
    fn selects(&self, listener: ContextId) -> bool {
        let _ = listener;
        true
    }

    /// Bail dispatch: whether a resolved output is decisive. The kernel awaits
    /// every listener result and asks only then — a pending async result is
    /// never treated as bailed (R9). Default: every resolved output is decisive.
    fn decisive(&self, output: &Self::Output) -> bool {
        let _ = output;
        true
    }

    /// Waterfall dispatch: fold one listener's output into the payload before
    /// the next listener sees it. Returns whether the walk continues; `false`
    /// declines the rest of the chain. Default: drop the output, continue.
    fn absorb(&mut self, output: Self::Output) -> bool {
        let _ = output;
        true
    }
}

/// Every listener outcome of one dispatch, per the event's declared mode.
///
/// R9 mechanically: a failing listener never aborts a collecting walk; its
/// contained failure is observed here, after every listener settled.
#[derive(Debug)]
pub struct DispatchReport<E: Event> {
    /// The payload after the walk. Waterfall reads its accumulator from here.
    pub event: E,
    /// Resolved outputs in registration order. Emit ignores outputs; bail
    /// carries the decisive output alone; waterfall folds outputs into `event`.
    pub outputs: Vec<E::Output>,
    /// Contained listener failures, in the order they were observed.
    pub failures: Vec<KernelError>,
}

/// One typed event listener.
pub trait EventListener<E: Event>: Send + Sync + 'static {
    fn call<'a>(&'a self, caller: ContextId, event: E) -> KernelFuture<'a, E::Output>;
}
