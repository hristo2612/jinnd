mod support;

use std::sync::{Arc, Mutex};

use jinnd_api::{Activation, ContextId, FiberState, Kernel, KernelFuture, PluginContract};
use support::{SpecCase, StateAt, v02_deferred};

const SUBSYSTEM: support::Subsystem = support::Subsystem::Fiber;
const V02_DEFERRED_BOUND: &str = "SOURCE-OF-TRUTH R7 and constitution 01 Mechanical closure: v0.1 has no in-process plugin host or public transition-control and failure-injection contract";

#[derive(Clone, Debug)]
struct RecordingPlugin {
    activations: Arc<Mutex<Vec<&'static str>>>,
}

impl PluginContract for RecordingPlugin {
    type Config = &'static str;
    type Dependencies = ();

    const NAME: &'static str = "jinn.test/recording-plugin";

    fn activate<'a>(
        &'a self,
        _activation: Activation<'a, Self::Dependencies>,
        config: Self::Config,
    ) -> KernelFuture<'a, ()> {
        self.activations
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(config);
        Box::pin(async { Ok(()) })
    }
}

fn recorded(activations: &Mutex<Vec<&'static str>>) -> Vec<&'static str> {
    activations
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
}

/// TS origin: `packages/core/tests/fiber.spec.ts`, test `inertia lock 1`.
#[tokio::test(flavor = "current_thread")]
async fn inertia_lock_1_lands_each_started_transition_before_reconciling() {
    v02_deferred(
        &SpecCase {
            origin: "packages/core/tests/fiber.spec.ts",
            test_name: "inertia lock 1",
            setup: &[
                "provide dependency generation 1",
                "start consumer whose load and unload each take 1000ms",
            ],
            actions: &[
                "withdraw dependency at 400ms",
                "restore dependency after the load lands and unload begins",
            ],
            expected: &["consumer fully unloads, reloads once, and ends active"],
            states: &[
                StateAt {
                    millis: 400,
                    state: FiberState::Loading,
                },
                StateAt {
                    millis: 800,
                    state: FiberState::Loading,
                },
                StateAt {
                    millis: 1200,
                    state: FiberState::Unloading,
                },
                StateAt {
                    millis: 2200,
                    state: FiberState::Loading,
                },
                StateAt {
                    millis: 3200,
                    state: FiberState::Active,
                },
            ],
        },
        SUBSYSTEM,
        V02_DEFERRED_BOUND,
    )
    .await;
}

/// TS origin: `packages/core/tests/fiber.spec.ts`, test `inertia lock 2`.
#[tokio::test(flavor = "current_thread")]
async fn inertia_lock_2_coalesces_dependency_return_during_loading() {
    v02_deferred(
        &SpecCase {
            origin: "packages/core/tests/fiber.spec.ts",
            test_name: "inertia lock 2",
            setup: &[
                "provide dependency generation 1",
                "start consumer whose load and unload each take 1000ms",
            ],
            actions: &[
                "withdraw dependency at 400ms",
                "provide generation 2 at 800ms before the first load lands",
            ],
            expected: &["the launched load lands and the consumer is active at 1200ms"],
            states: &[
                StateAt {
                    millis: 400,
                    state: FiberState::Loading,
                },
                StateAt {
                    millis: 800,
                    state: FiberState::Loading,
                },
                StateAt {
                    millis: 1200,
                    state: FiberState::Active,
                },
            ],
        },
        SUBSYSTEM,
        V02_DEFERRED_BOUND,
    )
    .await;
}

/// TS origin: `packages/core/tests/fiber.spec.ts`, test `inertia lock 3`.
#[tokio::test(flavor = "current_thread")]
async fn inertia_lock_3_provider_disposal_drains_consumer() {
    v02_deferred(
        &SpecCase {
            origin: "packages/core/tests/fiber.spec.ts",
            test_name: "inertia lock 3",
            setup: &[
                "activate a typed provider",
                "start consumer whose load and unload each take 1000ms",
            ],
            actions: &["dispose provider concurrently with all scheduled transitions"],
            expected: &["provider disposal waits until consumer reaches pending"],
            states: &[
                StateAt {
                    millis: 400,
                    state: FiberState::Loading,
                },
                StateAt {
                    millis: 1000,
                    state: FiberState::Active,
                },
                StateAt {
                    millis: 2000,
                    state: FiberState::Pending,
                },
            ],
        },
        SUBSYSTEM,
        V02_DEFERRED_BOUND,
    )
    .await;
}

/// TS origin: `packages/core/tests/fiber.spec.ts`, test `plugin error`.
#[tokio::test(flavor = "current_thread")]
async fn plugin_failure_is_local_to_the_failed_fiber() {
    v02_deferred(
        &SpecCase {
            origin: "packages/core/tests/fiber.spec.ts",
            test_name: "plugin error",
            setup: &["start failing and healthy sibling fibers with the same plugin type"],
            actions: &["let both activation attempts reach quiescence, then emit an event"],
            expected: &[
                "failing sibling is failed",
                "healthy sibling is active and receives the event exactly once",
                "one error is recorded",
            ],
            states: &[],
        },
        SUBSYSTEM,
        V02_DEFERRED_BOUND,
    )
    .await;
}

/// TS origin: `packages/core/tests/fiber.spec.ts`, test `dispose error`.
#[tokio::test(flavor = "current_thread")]
async fn disposer_failure_is_contained_and_disposal_is_idempotent() {
    v02_deferred(
        &SpecCase {
            origin: "packages/core/tests/fiber.spec.ts",
            test_name: "dispose error",
            setup: &["activate a plugin whose disposer returns an error"],
            actions: &["dispose the fiber twice"],
            expected: &[
                "dispose resolves without propagating the plugin error",
                "disposer runs once and one error is recorded",
            ],
            states: &[],
        },
        SUBSYSTEM,
        V02_DEFERRED_BOUND,
    )
    .await;
}

/// TS origin: `packages/core/tests/fiber.spec.ts`, test `update config on wrapped fiber`.
#[tokio::test(flavor = "current_thread")]
async fn config_update_restarts_with_each_latest_config() {
    let activations = Arc::new(Mutex::new(Vec::new()));
    let plugin = RecordingPlugin {
        activations: Arc::clone(&activations),
    };
    let kernel = jinnd_adapter::kernel();

    let Ok(fiber) = kernel.spawn(ContextId(0), plugin, "hello").await else {
        panic!("the recording plugin must spawn")
    };
    let Ok(()) = kernel.wait_for_quiescence().await else {
        panic!("the initial activation must settle")
    };
    let Ok(()) = kernel.update::<RecordingPlugin>(fiber, "world").await else {
        panic!("the world config must settle")
    };
    let Ok(()) = kernel.update::<RecordingPlugin>(fiber, "!!!").await else {
        panic!("the final config must settle")
    };
    let Ok(()) = kernel.wait_for_quiescence().await else {
        panic!("all updates must reach quiescence")
    };

    assert_eq!(recorded(&activations), vec!["hello", "world", "!!!"]);
    assert_eq!(kernel.state(fiber), FiberState::Active);
}

/// TS origin: `packages/core/tests/fiber.spec.ts`, test `restart wrapped fiber`.
#[tokio::test(flavor = "current_thread")]
async fn explicit_restart_reactivates_once_and_ends_active() {
    let activations = Arc::new(Mutex::new(Vec::new()));
    let plugin = RecordingPlugin {
        activations: Arc::clone(&activations),
    };
    let kernel = jinnd_adapter::kernel();

    let Ok(fiber) = kernel.spawn(ContextId(0), plugin, "stable").await else {
        panic!("the recording plugin must spawn")
    };
    let Ok(()) = kernel.restart(fiber).await else {
        panic!("the explicit restart must complete")
    };
    let Ok(()) = kernel.wait_for_quiescence().await else {
        panic!("the restarted fiber must settle")
    };

    assert_eq!(recorded(&activations), vec!["stable", "stable"]);
    assert_eq!(kernel.state(fiber), FiberState::Active);
}

/// TS origin: `packages/core/tests/fiber.spec.ts`, test `update config while injected service reloads`.
#[tokio::test(flavor = "current_thread")]
async fn config_update_and_dependency_reload_use_one_coherent_snapshot() {
    v02_deferred(
        &SpecCase {
            origin: "packages/core/tests/fiber.spec.ts",
            test_name: "update config while injected service reloads",
            setup: &["activate provider value=1 and consumer mode=old"],
            actions: &[
                "update provider to value=2",
                "update consumer to mode=new before both settle",
            ],
            expected: &[
                "consumer observations are exactly (1, old), (2, new)",
                "no mixed dependency/config snapshot is observed",
                "consumer ends active",
            ],
            states: &[],
        },
        SUBSYSTEM,
        V02_DEFERRED_BOUND,
    )
    .await;
}
