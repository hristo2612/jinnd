//! Shared scaffolding for the fiber-engine tests.

#![allow(dead_code)]

use std::any::TypeId;
use std::sync::{Arc, Mutex};

use jinnd_api::{
    DependencySnapshot, Epoch, ErrorCode, FiberId, FiberState, Generation, KernelError,
    KernelFuture, Realm, ServiceType, Transition,
};
use jinnd_effects::Disposer;
use jinnd_fiber::{Fiber, FiberBody, ReadinessSource, Setup};
use tokio::sync::{Semaphore, watch};

/// A dependency identity standing in for whatever the registry will publish.
#[must_use]
pub fn epoch(generation: u64) -> Epoch {
    Epoch {
        dependencies: vec![DependencySnapshot {
            service: ServiceType {
                type_id: TypeId::of::<()>(),
                name: "jinn.test/dependency",
            },
            provider: FiberId(u64::MAX),
            generation: Generation(generation),
            realm: Realm::Root,
        }],
    }
}

/// Records what activations and inverses did, in the order they did it.
#[derive(Clone, Debug, Default)]
pub struct Trace(Arc<Mutex<Vec<String>>>);

impl Trace {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&self, entry: impl Into<String>) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(entry.into());
    }

    #[must_use]
    pub fn entries(&self) -> Vec<String> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    #[must_use]
    pub fn count(&self, entry: &str) -> usize {
        self.entries().iter().filter(|line| *line == entry).count()
    }
}

/// A plugin body built from a closure, so each test states its own behaviour.
pub struct BodyFn<F>(F);

impl<F> FiberBody for BodyFn<F>
where
    F: for<'a> Fn(Setup<'a>) -> KernelFuture<'a, ()> + Send + Sync + 'static,
{
    fn activate<'a>(&'a self, setup: Setup<'a>) -> KernelFuture<'a, ()> {
        (self.0)(setup)
    }
}

/// Lifts `activate` into a shareable plugin body.
pub fn body<F>(activate: F) -> Arc<dyn FiberBody>
where
    F: for<'a> Fn(Setup<'a>) -> KernelFuture<'a, ()> + Send + Sync + 'static,
{
    Arc::new(BodyFn(activate))
}

/// A body that records one line per activation and registers one inverse.
pub fn recording(trace: &Trace, label: &'static str) -> Arc<dyn FiberBody> {
    let trace = trace.clone();
    body(move |mut setup| {
        let trace = trace.clone();
        Box::pin(async move {
            trace.push(format!("load:{label}"));
            setup.effect(label, undo(&trace, label))?;
            Ok(())
        })
    })
}

/// A body that blocks inside its activation until the test releases it.
///
/// `entered` counts activations that have started; `release` admits one of them.
#[derive(Clone, Debug)]
pub struct Gate {
    entered: watch::Sender<u32>,
    permits: Arc<Semaphore>,
}

impl Gate {
    #[must_use]
    pub fn new() -> Self {
        Self {
            entered: watch::Sender::new(0),
            permits: Arc::new(Semaphore::new(0)),
        }
    }

    /// Called from inside an activation: announce arrival, then wait to be admitted.
    pub async fn enter(&self) {
        self.entered.send_modify(|count| *count += 1);
        if let Ok(permit) = self.permits.acquire().await {
            permit.forget();
        }
    }

    /// Resolves once `count` activations have arrived at the gate.
    pub async fn entered(&self, count: u32) {
        let mut seen = self.entered.subscribe();
        while *seen.borrow_and_update() < count {
            if seen.changed().await.is_err() {
                return;
            }
        }
    }

    /// Admits one waiting activation.
    pub fn release(&self) {
        self.permits.add_permits(1);
    }
}

impl Default for Gate {
    fn default() -> Self {
        Self::new()
    }
}

/// The error a deliberately failing body returns.
#[must_use]
pub fn failure(message: &str) -> KernelError {
    KernelError {
        code: ErrorCode::PluginFailed,
        message: message.to_owned(),
        fiber: None,
    }
}

/// An inverse that records `label` when it runs.
#[must_use]
pub fn undo(trace: &Trace, label: &'static str) -> Disposer {
    let trace = trace.clone();
    Disposer::sync(move || {
        trace.push(format!("undo:{label}"));
        Ok(())
    })
}

/// Spawns a fiber whose dependencies are already satisfied.
#[must_use]
pub fn ready(body: Arc<dyn FiberBody>) -> (Fiber, ReadinessSource) {
    let source = ReadinessSource::new(Some(epoch(1)));
    let fiber = Fiber::spawn(body, source.signal());
    (fiber, source)
}

/// The `to` states of every recorded transition, in order.
#[must_use]
pub fn path(transitions: &[Transition]) -> Vec<FiberState> {
    transitions.iter().map(|entry| entry.to).collect()
}

/// A body that registers one inverse, then waits at `gate` before landing.
#[must_use]
pub fn gated(trace: &Trace, label: &'static str, gate: &Gate) -> Arc<dyn FiberBody> {
    let trace = trace.clone();
    let gate = gate.clone();
    body(move |mut setup| {
        let trace = trace.clone();
        let gate = gate.clone();
        Box::pin(async move {
            trace.push(format!("load:{label}"));
            setup.effect(label, undo(&trace, label))?;
            gate.enter().await;
            trace.push(format!("land:{label}"));
            Ok(())
        })
    })
}

/// An inverse that waits at `gate` before it records `label` and completes.
#[must_use]
pub fn gated_undo(trace: &Trace, label: &'static str, gate: &Gate) -> Disposer {
    let trace = trace.clone();
    let gate = gate.clone();
    Disposer::future(move || async move {
        gate.enter().await;
        trace.push(format!("undo:{label}"));
        Ok(())
    })
}
