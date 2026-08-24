mod support;

use jinnd_api::FiberState;
use support::{SpecCase, StateAt, pending};

/// TS origin: `packages/core/tests/fiber.spec.ts`, test `inertia lock 1`.
#[test]
fn inertia_lock_1_lands_each_started_transition_before_reconciling() {
    pending(&SpecCase {
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
    });
}

/// TS origin: `packages/core/tests/fiber.spec.ts`, test `inertia lock 2`.
#[test]
fn inertia_lock_2_coalesces_dependency_return_during_loading() {
    pending(&SpecCase {
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
    });
}

/// TS origin: `packages/core/tests/fiber.spec.ts`, test `inertia lock 3`.
#[test]
fn inertia_lock_3_provider_disposal_drains_consumer() {
    pending(&SpecCase {
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
    });
}

/// TS origin: `packages/core/tests/fiber.spec.ts`, test `plugin error`.
#[test]
fn plugin_failure_is_local_to_the_failed_fiber() {
    pending(&SpecCase {
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
    });
}

/// TS origin: `packages/core/tests/fiber.spec.ts`, test `dispose error`.
#[test]
fn disposer_failure_is_contained_and_disposal_is_idempotent() {
    pending(&SpecCase {
        origin: "packages/core/tests/fiber.spec.ts",
        test_name: "dispose error",
        setup: &["activate a plugin whose disposer returns an error"],
        actions: &["dispose the fiber twice"],
        expected: &[
            "dispose resolves without propagating the plugin error",
            "disposer runs once and one error is recorded",
        ],
        states: &[],
    });
}

/// TS origin: `packages/core/tests/fiber.spec.ts`, test `update config on wrapped fiber`.
#[test]
fn config_update_restarts_with_each_latest_config() {
    pending(&SpecCase {
        origin: "packages/core/tests/fiber.spec.ts",
        test_name: "update config on wrapped fiber",
        setup: &["activate with config message=hello"],
        actions: &["update to world and await", "update to !!! and await"],
        expected: &["activation configs are exactly hello, world, !!! in order"],
        states: &[],
    });
}

/// TS origin: `packages/core/tests/fiber.spec.ts`, test `restart wrapped fiber`.
#[test]
fn explicit_restart_reactivates_once_and_ends_active() {
    pending(&SpecCase {
        origin: "packages/core/tests/fiber.spec.ts",
        test_name: "restart wrapped fiber",
        setup: &["activate one fiber"],
        actions: &["request restart and await quiescence"],
        expected: &["plugin body ran exactly twice", "fiber is active"],
        states: &[],
    });
}

/// TS origin: `packages/core/tests/fiber.spec.ts`, test `update config while injected service reloads`.
#[test]
fn config_update_and_dependency_reload_use_one_coherent_snapshot() {
    pending(&SpecCase {
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
    });
}
