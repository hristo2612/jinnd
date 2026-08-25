//! Shared fixtures for the bus's behavioral suites. Each suite is its own test
//! crate, so unused items here are expected per-binary.

#![allow(dead_code)]

use std::future::Future;
use std::sync::{Arc, Mutex};

use jinnd_api::{
    ContextId, DispatchMode, ErrorCode, Event, EventListener, KernelError, KernelFuture,
};

pub const ROOT: ContextId = ContextId(0);

pub type Log = Arc<Mutex<Vec<&'static str>>>;

pub fn record(log: &Log, entry: &'static str) {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .push(entry);
}

pub fn recorded(log: &Log) -> Vec<&'static str> {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
}

pub fn boxed<T>(
    future: impl Future<Output = Result<T, KernelError>> + Send + 'static,
) -> KernelFuture<'static, T> {
    Box::pin(future)
}

pub fn failure<T>(message: &str) -> Result<T, KernelError> {
    Err(KernelError {
        code: ErrorCode::ListenerFailed,
        message: message.to_owned(),
        fiber: None,
    })
}

pub struct FnListener<F>(pub F);

impl<E, F> EventListener<E> for FnListener<F>
where
    E: Event,
    F: Fn(ContextId, E) -> KernelFuture<'static, E::Output> + Send + Sync + 'static,
{
    fn call<'a>(&'a self, caller: ContextId, event: E) -> KernelFuture<'a, E::Output> {
        (self.0)(caller, event)
    }
}

#[derive(Clone, Debug)]
pub struct Ping;

impl Event for Ping {
    type Output = ();

    const MODE: DispatchMode = DispatchMode::Emit;
}

#[derive(Clone, Debug)]
pub struct Routed {
    pub target: ContextId,
}

impl Event for Routed {
    type Output = ();

    const MODE: DispatchMode = DispatchMode::Emit;

    fn selects(&self, listener: ContextId) -> bool {
        listener == self.target
    }
}

#[derive(Clone, Debug)]
pub struct Unroutable;

impl Event for Unroutable {
    type Output = ();

    const MODE: DispatchMode = DispatchMode::Emit;

    fn selects(&self, _listener: ContextId) -> bool {
        panic!("routing panic")
    }
}

#[derive(Clone, Debug)]
pub struct Gather;

impl Event for Gather {
    type Output = u8;

    const MODE: DispatchMode = DispatchMode::Parallel;
}

#[derive(Clone, Debug)]
pub struct Ordered;

impl Event for Ordered {
    type Output = u8;

    const MODE: DispatchMode = DispatchMode::Serial;
}

#[derive(Clone, Debug)]
pub struct Probe;

impl Event for Probe {
    type Output = Option<u8>;

    const MODE: DispatchMode = DispatchMode::Bail;

    fn decisive(&self, output: &Self::Output) -> bool {
        output.is_some()
    }
}

/// Waterfall accumulator: `Add` folds and continues, `Take` folds and declines
/// the rest of the chain.
#[derive(Debug)]
pub enum Step {
    Add(i64),
    Take(i64),
}

#[derive(Clone, Debug)]
pub struct Fold {
    pub acc: i64,
}

impl Event for Fold {
    type Output = Step;

    const MODE: DispatchMode = DispatchMode::Waterfall;

    fn absorb(&mut self, output: Step) -> bool {
        match output {
            Step::Add(amount) => {
                self.acc += amount;
                true
            }
            Step::Take(value) => {
                self.acc = value;
                false
            }
        }
    }
}
