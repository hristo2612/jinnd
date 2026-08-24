//! Shared scaffolding for the effect-engine tests.

#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use jinnd_api::{EffectId, ErrorCode, KernelError};
use jinnd_effects::Disposer;

/// Records the order in which inverses ran.
#[derive(Clone, Debug, Default)]
pub struct Trace(Arc<Mutex<Vec<String>>>);

impl Trace {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&self, entry: &str) {
        let mut entries = self.0.lock().unwrap_or_else(|poison| poison.into_inner());
        entries.push(entry.to_owned());
    }

    pub fn entries(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }
}

/// A synchronous inverse that records `label` when it runs.
pub fn recorded(trace: &Trace, label: &'static str) -> Disposer {
    let trace = trace.clone();
    Disposer::sync(move || {
        trace.push(label);
        Ok(())
    })
}

/// An awaited inverse that records `label` when it runs.
pub fn recorded_async(trace: &Trace, label: &'static str) -> Disposer {
    let trace = trace.clone();
    Disposer::future(move || async move {
        tokio::task::yield_now().await;
        trace.push(label);
        Ok(())
    })
}

/// An inverse that records `label` and then returns an error.
pub fn failing(trace: &Trace, label: &'static str) -> Disposer {
    let trace = trace.clone();
    Disposer::sync(move || {
        trace.push(label);
        Err(error(label))
    })
}

/// An inverse that records `label` and then panics.
pub fn panicking(trace: &Trace, label: &'static str) -> Disposer {
    let trace = trace.clone();
    Disposer::sync(move || {
        trace.push(label);
        panic!("{label} could not be undone");
    })
}

/// The error a [`failing`] inverse returns.
pub fn error(label: &str) -> KernelError {
    KernelError {
        code: ErrorCode::EffectFailed,
        message: format!("{label} could not be undone"),
        fiber: None,
    }
}

/// Unwraps a registration that the test requires to succeed.
pub fn registered(result: Result<EffectId, KernelError>) -> EffectId {
    match result {
        Ok(id) => id,
        Err(error) => panic!("registration must succeed here: {error:?}"),
    }
}
