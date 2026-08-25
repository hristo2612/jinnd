//! Shared scaffolding for the effect-engine tests.

#![allow(dead_code)]

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

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

/// An inverse that never completes.
pub fn stuck() -> Disposer {
    Disposer::future(std::future::pending::<Result<(), KernelError>>)
}

/// An inverse whose future panics from its own destructor.
///
/// `ready` picks the boundary under test: an inverse that ran to completion and then
/// panicked while being dropped, or one still in flight when the replay was dropped.
pub struct PanicOnDrop {
    label: &'static str,
    ready: bool,
    trace: Trace,
}

impl Future for PanicOnDrop {
    type Output = Result<(), KernelError>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.ready {
            return Poll::Pending;
        }
        self.trace.push(self.label);
        Poll::Ready(Ok(()))
    }
}

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        panic!("{} left a panicking destructor", self.label);
    }
}

/// An inverse that runs to completion and then panics while its future is dropped.
pub fn panicking_destructor(trace: &Trace, label: &'static str) -> Disposer {
    let trace = trace.clone();
    Disposer::future(move || PanicOnDrop {
        label,
        ready: true,
        trace,
    })
}

/// An inverse that never completes and panics while its future is dropped.
pub fn stuck_panicking_destructor(trace: &Trace, label: &'static str) -> Disposer {
    let trace = trace.clone();
    Disposer::future(move || PanicOnDrop {
        label,
        ready: false,
        trace,
    })
}

/// Polls `future` once, requiring it to still be pending afterwards.
pub fn poll_pending<F: Future>(future: Pin<&mut F>) {
    let mut cx = Context::from_waker(Waker::noop());
    assert!(
        future.poll(&mut cx).is_pending(),
        "this future must still be pending here"
    );
}

/// A step whose closure panics from its own destructor if the step never runs.
pub fn step_with_panicking_destructor(label: &'static str) -> jinnd_effects::UndoStep {
    let guard = PanicOnDrop {
        label,
        ready: true,
        trace: Trace::new(),
    };
    jinnd_effects::step(move || {
        let _ = &guard;
        Ok(())
    })
}
