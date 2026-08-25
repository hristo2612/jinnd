//! M1-P6b regression suite: loader operations run their plugin-facing
//! callbacks with no lock guard held (R1). A callback that calls back into
//! the loader is refused honestly, never deadlocked, and concurrent
//! operations stay single-flight — no interleaved amendments.

mod common;

use std::pin::pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context as TaskContext, Poll, Waker};

use jinnd_api::{ErrorCode, KernelError, Profile};
use jinnd_loader::{Loader, PackageLane, SpawnRequest};
use jinnd_registry::Registry;

use common::{Grab, Moment, entry, fixture, id, moments, profile};

/// Polls `future` exactly once with an inert waker: the most a synchronous
/// plugin-facing callback can do to drive a re-entrant loader call.
fn poll_once<T>(
    future: impl Future<Output = Result<T, KernelError>>,
) -> Poll<Result<T, KernelError>> {
    let mut future = pin!(future);
    let mut task = TaskContext::from_waker(Waker::noop());
    future.as_mut().poll(&mut task)
}

/// One probe's honest description: what a re-entrant call observed.
fn describe<T>(poll: Poll<Result<T, KernelError>>) -> String {
    match poll {
        Poll::Ready(Ok(_)) => "succeeded".to_owned(),
        Poll::Ready(Err(error)) => format!("refused: {}", error.message),
        Poll::Pending => "pending: a re-entrant call would wait on its own operation".to_owned(),
    }
}

/// A loader with one lane whose spawn callback probes all three loader
/// operations re-entrantly and records what each observed.
fn probing_loader() -> (Arc<Loader>, Arc<Mutex<Vec<String>>>) {
    let tree = jinnd_context::ContextTree::new();
    let root = tree.root();
    let registry = Registry::new();
    let loader = Arc::new(Loader::new(root, registry, |_context| {}));
    let probes = Arc::new(Mutex::new(Vec::new()));
    let recorded = Arc::clone(&probes);
    let captured: Weak<Loader> = Arc::downgrade(&loader);
    loader
        .register_lane::<u32>(
            "test/reenter",
            PackageLane {
                injects: Vec::new(),
                provides: None,
                spawn: Box::new(move |request: SpawnRequest<'_>| {
                    let loader = captured.upgrade().grab();
                    let target = request.entry.clone();
                    let observed = vec![
                        describe(poll_once(loader.update_entry(&target, 9_u32))),
                        describe(poll_once(loader.dispose_entry::<u32>(&target))),
                        describe(poll_once(loader.reconcile(Profile::<u32> {
                            entries: Vec::new(),
                        }))),
                    ];
                    *recorded.lock().unwrap_or_else(|poison| poison.into_inner()) = observed;
                    Err(KernelError {
                        code: ErrorCode::PluginFailed,
                        message: "probe lane never spawns".to_owned(),
                        fiber: None,
                    })
                }),
            },
        )
        .grab();
    (loader, probes)
}

#[tokio::test]
async fn a_callback_reentering_the_loader_is_refused_honestly() {
    let (loader, probes) = probing_loader();
    let report = loader
        .reconcile(profile(vec![entry("a", "test/reenter", 1)]))
        .await
        .grab();
    // The probe lane refuses its own spawn after probing; contained per R11.
    assert_eq!(report.errors.len(), 1);
    let observed = probes
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone();
    assert_eq!(observed.len(), 3, "all three operations are probed");
    for probe in &observed {
        assert!(
            probe.starts_with("refused"),
            "a re-entrant loader call must be refused honestly, got: {probe}"
        );
    }
}

#[tokio::test]
async fn concurrent_updates_stay_single_flight_and_converge() {
    let (loader, _registry, log) = fixture();
    let loader = Arc::new(loader);
    loader
        .reconcile(profile(vec![entry("a", "test/count", 1)]))
        .await
        .grab();
    let tasks: Vec<_> = (2..=5_u32)
        .map(|config| {
            let loader = Arc::clone(&loader);
            tokio::spawn(async move { loader.update_entry(&id("a"), config).await })
        })
        .collect();
    for task in tasks {
        task.await.grab().grab();
    }
    // Serialized operations: every update fully landed, one activation each.
    assert_eq!(common::activations(&log, "a"), 5);
    assert!(loader.entry_faults().is_empty());
    // The two views agree: the last activation observed the committed config.
    let committed: Profile<u32> = loader.persisted().grab();
    let last_activated = moments(&log)
        .iter()
        .rev()
        .find_map(|moment| match moment {
            Moment::Activated(entry, config) if entry == "a" => Some(*config),
            _ => None,
        })
        .grab();
    assert_eq!(committed.entries[0].config, last_activated);
}
