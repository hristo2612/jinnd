mod support;

use support::spec_case;

const SUBSYSTEM: support::Subsystem = support::Subsystem::Effects;
const FACADE_GAP_REASON: &str = "the facade registers an Undo directly and exposes no effect-disposal or async forward-effect API";

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `dispose by plugin`.
    dispose_by_plugin_is_visible_and_idempotent,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "dispose by plugin",
    setup: ["plugin registers one labeled effect"],
    actions: ["inspect effect tree", "dispose plugin twice"],
    expected: ["tree contains label test", "undo runs exactly once"]
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `dispose manually`.
    manual_dispose_is_visible_and_idempotent,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "dispose manually",
    setup: ["root registers one anonymous effect"],
    actions: ["invoke returned disposer twice"],
    expected: ["effect appears at root", "undo runs exactly once"]
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `yield dispose`.
    nested_effects_unwind_in_reverse_order,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "yield dispose",
    setup: ["register nested effects with listener children and undo markers 1, 2, 3"],
    actions: ["inspect nested labels", "dispose outer effect twice"],
    expected: ["effect tree preserves parent-child shape", "undo sequence is 3, 2, 1 exactly once"]
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `async return 1`.
    async_effect_return_registers_undo_after_forward_completes,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "async return 1",
    setup: ["start a 100ms asynchronous forward effect returning an undo"],
    actions: ["advance 100ms", "dispose effect"],
    expected: ["forward marker precedes undo marker", "sequence is 1, 2"]
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `async return 2`.
    disposing_in_flight_async_effect_waits_then_undoes,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "async return 2",
    setup: ["start a 100ms asynchronous forward effect returning an undo"],
    actions: ["request disposal immediately", "advance 100ms"],
    expected: ["forward is allowed to land", "its undo follows immediately", "sequence is 1, 2"]
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `async yield 1`.
    completed_async_iterator_unwinds_all_yielded_undos_lifo,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "async yield 1",
    setup: ["three 100ms iterator steps yield undo markers 2, 4, 6"],
    actions: ["advance 300ms", "dispose"],
    expected: ["forward sequence is 1, 3, 5", "final sequence is 1, 3, 5, 6, 4, 2"]
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `async yield 2 (aborted)`.
    abort_before_first_async_yield_lands_then_undoes_first_step,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "async yield 2 (aborted)",
    setup: ["three-step asynchronous iterator effect"],
    actions: ["request disposal at 50ms", "advance through all timers"],
    expected: ["first launched step lands", "only first yielded inverse runs", "sequence is 1, 2"]
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `async yield 3 (aborted)`.
    abort_after_first_yield_lands_next_step_then_unwinds,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "async yield 3 (aborted)",
    setup: ["three-step asynchronous iterator effect", "first step has yielded its inverse"],
    actions: ["request disposal at 100ms", "advance 200ms"],
    expected: ["launched second step lands", "inverse order is second then first", "sequence is 1, 3, 4, 2"]
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `async yield 4 (await dispose)`.
    awaiting_async_effect_returns_an_idempotent_disposer,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "async yield 4 (await dispose)",
    setup: ["three-step asynchronous iterator effect"],
    actions: ["await effect completion", "invoke returned disposer"],
    expected: ["forward sequence is 1, 3, 5", "undo sequence appends 6, 4, 2"]
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `return with error`.
    synchronous_effect_failure_registers_no_inverse,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "return with error",
    setup: ["forward effect fails before returning an inverse"],
    actions: ["register effect"],
    expected: ["registration returns the original error", "no inverse runs"]
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `yield with error`.
    synchronous_iterator_failure_unwinds_prior_yields,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "yield with error",
    setup: ["iterator yields inverse 1 then fails before inverse 2"],
    actions: ["register effect"],
    expected: ["registration returns the original error", "inverse 1 runs immediately"]
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `async return with error`.
    asynchronous_effect_failure_registers_no_inverse,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "async return with error",
    setup: ["asynchronous forward effect fails before returning an inverse"],
    actions: ["await effect registration"],
    expected: ["future returns the original error", "no inverse runs"]
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `async yield with error`.
    asynchronous_iterator_failure_unwinds_prior_yields,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "async yield with error",
    setup: ["asynchronous iterator yields inverse 1 then fails"],
    actions: ["await effect registration"],
    expected: ["future returns the original error", "inverse 1 runs immediately"]
}
