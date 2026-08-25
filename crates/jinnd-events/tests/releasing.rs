//! Plugin-authored code at the walk's edges — payload clones and listener
//! destructors — stays contained (R11): its panic is recorded as that
//! listener's failure and never aborts the walk or unwinds out of it (R9).

#![cfg(not(feature = "loom"))]

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use jinnd_api::{ContextId, DispatchMode, ErrorCode, Event, EventListener, KernelFuture};
use jinnd_events::EventBus;
use support::{FnListener, Ping, ROOT, boxed};

/// A payload whose plugin-authored `Clone` panics while poisoned.
#[derive(Debug)]
struct FlakyClone {
    poisoned: bool,
}

impl Clone for FlakyClone {
    fn clone(&self) -> Self {
        if self.poisoned {
            panic!("the payload clone panicked")
        }
        Self { poisoned: false }
    }
}

impl Event for FlakyClone {
    type Output = ();

    const MODE: DispatchMode = DispatchMode::Parallel;
}

/// A listener whose plugin-authored destructor panics. Registered only as a
/// once-listener, so the final handle drops inside the claiming dispatch.
struct PoisonDrop(Arc<AtomicUsize>);

impl EventListener<Ping> for PoisonDrop {
    fn call<'a>(&'a self, _caller: ContextId, Ping: Ping) -> KernelFuture<'a, ()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        boxed(async { Ok(()) })
    }
}

impl Drop for PoisonDrop {
    fn drop(&mut self) {
        panic!("the listener destructor panicked")
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_parallel_payload_clone_panic_is_contained_and_recorded() {
    let bus = EventBus::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let listener_calls = Arc::clone(&calls);
    bus.listen(
        ROOT,
        FnListener(move |_, _event: FlakyClone| {
            listener_calls.fetch_add(1, Ordering::SeqCst);
            boxed(async { Ok(()) })
        }),
        false,
    );

    let report = bus.dispatch(ROOT, FlakyClone { poisoned: true }).await;

    assert_eq!(calls.load(Ordering::SeqCst), 0, "the clone never succeeded");
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].code, ErrorCode::ListenerFailed);
    assert!(report.outputs.is_empty());
}

#[tokio::test(flavor = "current_thread")]
async fn a_failed_clone_does_not_consume_the_once_registration() {
    let bus = EventBus::new();
    let calls = Arc::new(AtomicUsize::new(0));
    let listener_calls = Arc::clone(&calls);
    bus.listen(
        ROOT,
        FnListener(move |_, _event: FlakyClone| {
            listener_calls.fetch_add(1, Ordering::SeqCst);
            boxed(async { Ok(()) })
        }),
        true,
    );

    let poisoned = bus.dispatch(ROOT, FlakyClone { poisoned: true }).await;
    assert_eq!(poisoned.failures.len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let clean = bus.dispatch(ROOT, FlakyClone { poisoned: false }).await;
    assert!(clean.failures.is_empty(), "the registration survived");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    bus.dispatch(ROOT, FlakyClone { poisoned: false }).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1, "once means once");
}

#[tokio::test(flavor = "current_thread")]
async fn a_claimed_once_listeners_destructor_panic_is_contained() {
    let bus = EventBus::new();
    let calls = Arc::new(AtomicUsize::new(0));
    bus.listen(ROOT, PoisonDrop(Arc::clone(&calls)), true);

    let report = bus.dispatch(ROOT, Ping).await;

    assert_eq!(calls.load(Ordering::SeqCst), 1, "the call itself succeeded");
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].code, ErrorCode::ListenerFailed);

    let after = bus.dispatch(ROOT, Ping).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1, "the registration is spent");
    assert!(after.failures.is_empty());
}
