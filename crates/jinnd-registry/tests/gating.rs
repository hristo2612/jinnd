//! Reactive availability and epoch gating, end to end against the fiber engine:
//! a consumer activates only when its providers are available, and any provider
//! change forces a full clean unload → reload (R1, R9; §3 "Epoch gating"; I2).

#![cfg(not(feature = "loom"))]

mod support;

use std::sync::{Arc, Mutex};

use jinnd_api::{FiberState, KernelFuture, ServiceContract};
use jinnd_context::ContextTree;
use jinnd_effects::{Disposer, EffectScope};
use jinnd_fiber::{Fiber, FiberBody, ReadinessSignal, Setup};
use jinnd_registry::{InjectedReadiness, Injection, Registry};
use support::{Counter, counter_service, provide_counter, provide_other};

/// A consumer body that records each activation's observed counter value, and
/// whose undo calls the (possibly dying) service one last time (I2).
struct Consumer {
    registry: Registry,
    context: jinnd_context::Context<()>,
    observed: Arc<Mutex<Vec<u8>>>,
    torn_down: Arc<Mutex<Vec<u8>>>,
}

impl FiberBody for Consumer {
    fn activate<'a>(&'a self, mut setup: Setup<'a>) -> KernelFuture<'a, ()> {
        Box::pin(async move {
            let (handle, guard) = self.registry.lease::<Counter, ()>(&self.context)?;
            record(&self.observed, handle.service.observe());
            let torn_down = Arc::clone(&self.torn_down);
            setup.effect(
                "consumer lease",
                Disposer::sync(move || {
                    // The dying provider must still answer during teardown (I2).
                    record(&torn_down, handle.service.observe());
                    drop(guard);
                    Ok(())
                }),
            )?;
            Ok(())
        })
    }
}

fn record(cell: &Mutex<Vec<u8>>, value: u8) {
    cell.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .push(value);
}

fn snapshot(cell: &Mutex<Vec<u8>>) -> Vec<u8> {
    cell.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
}

struct Rig {
    tree: ContextTree,
    registry: Registry,
    observed: Arc<Mutex<Vec<u8>>>,
    torn_down: Arc<Mutex<Vec<u8>>>,
    fiber: Fiber,
}

fn rig() -> Rig {
    let tree: ContextTree = ContextTree::new();
    let registry = Registry::new();
    let observed = Arc::new(Mutex::new(Vec::new()));
    let torn_down = Arc::new(Mutex::new(Vec::new()));
    let signal = registry.readiness(
        &tree.root(),
        Injection {
            services: vec![counter_service()],
        },
    );
    let fiber = Fiber::spawn(
        Arc::new(Consumer {
            registry: registry.clone(),
            context: tree.root(),
            observed: Arc::clone(&observed),
            torn_down: Arc::clone(&torn_down),
        }),
        signal,
    );
    Rig {
        tree,
        registry,
        observed,
        torn_down,
        fiber,
    }
}

/// Lets the availability watcher process the store edge it was just handed.
async fn breathe() {
    for _ in 0..4 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_consumer_waits_for_its_provider_and_activates_when_it_appears() {
    let rig = rig();
    let mut scope = EffectScope::new();

    rig.fiber.quiesce().await;
    assert_eq!(rig.fiber.state(), FiberState::Pending);
    assert_eq!(snapshot(&rig.observed), Vec::<u8>::new());

    provide_counter(&rig.registry, &mut scope, &rig.tree.root(), 4);
    breathe().await;
    rig.fiber.quiesce().await;
    assert_eq!(rig.fiber.state(), FiberState::Active);
    assert_eq!(snapshot(&rig.observed), vec![4]);
}

#[tokio::test(flavor = "current_thread")]
async fn a_provider_change_forces_a_full_clean_reload() {
    let rig = rig();
    let mut scope = EffectScope::new();

    provide_counter(&rig.registry, &mut scope, &rig.tree.root(), 1);
    breathe().await;
    rig.fiber.quiesce().await;
    assert_eq!(rig.fiber.state(), FiberState::Active);

    provide_counter(&rig.registry, &mut scope, &rig.tree.root(), 2);
    breathe().await;
    rig.fiber.quiesce().await;

    assert_eq!(rig.fiber.state(), FiberState::Active);
    assert_eq!(
        snapshot(&rig.observed),
        vec![1, 2],
        "the replacement generation must activate a fresh consumer, never swap silently (R9)"
    );
    assert_eq!(
        snapshot(&rig.torn_down),
        vec![1],
        "the first activation must fully unload, its undo still seeing generation one (I2)"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn withdrawing_the_provider_unloads_the_consumer_and_only_then_drains() {
    let rig = rig();
    let mut scope = EffectScope::new();

    provide_counter(&rig.registry, &mut scope, &rig.tree.root(), 6);
    breathe().await;
    rig.fiber.quiesce().await;
    assert_eq!(rig.fiber.state(), FiberState::Active);

    let report = scope.replay().await;
    assert!(
        report.is_clean(),
        "the drained withdrawal completes: {report:?}"
    );
    rig.fiber.quiesce().await;

    assert_eq!(rig.fiber.state(), FiberState::Pending);
    assert_eq!(
        snapshot(&rig.torn_down),
        vec![6],
        "the consumer's teardown ran, and could still call the dying service (I2)"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn an_unrelated_store_change_leaves_the_epoch_untouched() {
    let tree: ContextTree = ContextTree::new();
    let registry = Registry::new();
    let signal = registry.readiness(
        &tree.root(),
        Injection {
            services: vec![counter_service()],
        },
    );
    let mut scope = EffectScope::new();

    provide_counter(&registry, &mut scope, &tree.root(), 1);
    breathe().await;
    let ready = signal.epoch();
    assert!(ready.is_some(), "a provided dependency must be ready");

    provide_other(&registry, &mut scope, &tree.root(), 9);
    breathe().await;
    assert_eq!(
        signal.epoch(),
        ready,
        "an unrelated slot change must not move this consumer's epoch"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn epochs_carry_the_provider_generation() {
    let tree: ContextTree = ContextTree::new();
    let registry = Registry::new();
    let signal = registry.readiness(
        &tree.root(),
        Injection {
            services: vec![counter_service()],
        },
    );
    let mut scope = EffectScope::new();

    provide_counter(&registry, &mut scope, &tree.root(), 1);
    breathe().await;
    let first = signal.epoch();

    provide_counter(&registry, &mut scope, &tree.root(), 1);
    breathe().await;
    let second = signal.epoch();

    assert!(first.is_some() && second.is_some());
    assert_ne!(first, second, "a new generation is a new epoch (R9)");
}

/// The registry's signal is the seam the fiber engine consumes (the P3 trait).
#[test]
fn injected_readiness_is_a_readiness_signal() {
    fn accepts<S: ReadinessSignal>() {}
    accepts::<InjectedReadiness>();
}

#[tokio::test(flavor = "current_thread")]
async fn a_provider_not_yet_reported_active_withholds_readiness() {
    let tree: ContextTree = ContextTree::new();
    let registry = Registry::new();
    let mut scope = EffectScope::new();
    let signal = registry.readiness(
        &tree.root(),
        Injection {
            services: vec![counter_service()],
        },
    );

    let vitality = registry.vitality(false);
    support::provide_counter_guarded(&registry, &mut scope, &tree.root(), 3, &vitality);
    breathe().await;
    assert_eq!(
        signal.epoch(),
        None,
        "a provider whose supervisor has not reported it Active is not available (§3)"
    );

    vitality.report(true);
    breathe().await;
    assert!(
        signal.epoch().is_some(),
        "an Active, checked provider completes the injection"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn a_provider_reported_inactive_unloads_its_consumers() {
    let rig = rig();
    let mut scope = EffectScope::new();

    let vitality = rig.registry.vitality(true);
    support::provide_counter_guarded(&rig.registry, &mut scope, &rig.tree.root(), 5, &vitality);
    breathe().await;
    rig.fiber.quiesce().await;
    assert_eq!(rig.fiber.state(), FiberState::Active);

    vitality.report(false);
    breathe().await;
    rig.fiber.quiesce().await;
    assert_eq!(
        rig.fiber.state(),
        FiberState::Pending,
        "a provider leaving Active withdraws availability without withdrawing the slot"
    );
    assert_eq!(
        snapshot(&rig.torn_down),
        vec![5],
        "the consumer fully unloads, its teardown still answered by the value (I2)"
    );

    vitality.report(true);
    breathe().await;
    rig.fiber.quiesce().await;
    assert_eq!(rig.fiber.state(), FiberState::Active);
    assert_eq!(
        snapshot(&rig.observed),
        vec![5, 5],
        "recovery is a fresh activation against the same generation, never a silent resume (R9)"
    );
}
