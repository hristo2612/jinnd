mod support;

use std::sync::{Arc, Mutex};

use jinnd_api::{
    Activation, ErrorCode, ForwardAction, ForwardEffect, Kernel, KernelError, KernelFuture,
    PluginContract, Undo,
};
use support::spec_case;

type Log = Arc<Mutex<Vec<u32>>>;

fn log() -> Log {
    Arc::new(Mutex::new(Vec::new()))
}

fn read(log: &Log) -> Vec<u32> {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
}

fn mark(log: &Log, value: u32) {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .push(value);
}

struct MarkUndo(Log, u32);

impl Undo for MarkUndo {
    fn undo(self: Box<Self>) -> KernelFuture<'static, ()> {
        mark(&self.0, self.1);
        Box::pin(async { Ok(()) })
    }
}

fn step(log: &Log, forward: u32, inverse: u32) -> ForwardAction {
    let log = Arc::clone(log);
    Box::new(move || {
        mark(&log, forward);
        let undo: Box<dyn Undo> = Box::new(MarkUndo(log, inverse));
        Box::pin(async move { Ok(undo) })
    })
}

fn failure(message: &'static str) -> ForwardAction {
    Box::new(move || {
        Box::pin(async move {
            Err(KernelError {
                code: ErrorCode::EffectFailed,
                message: message.to_owned(),
                fiber: None,
            })
        })
    })
}

fn expect_failure(result: Result<(), KernelError>, context: &str) -> KernelError {
    match result {
        Ok(()) => panic!("{context}: expected failure"),
        Err(error) => error,
    }
}

async fn wait_for_len(log: &Log, length: usize) {
    for _ in 0..100 {
        if read(log).len() >= length {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("effect did not reach log length {length}");
}

#[derive(Debug)]
struct EffectPlugin(Log);

impl PluginContract for EffectPlugin {
    type Config = ();
    type Dependencies = ();

    const NAME: &'static str = "jinn.test/dispose-plugin";

    fn activate<'a>(&'a self, activation: Activation<'a, ()>, _config: ()) -> KernelFuture<'a, ()> {
        let log = Arc::clone(&self.0);
        Box::pin(async move {
            activation
                .effects
                .register("test".to_owned(), Box::new(MarkUndo(log, 1)))?;
            Ok(())
        })
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `dispose by plugin`.
    dispose_by_plugin_is_visible_and_idempotent,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "dispose by plugin",
    setup: ["plugin registers one labeled effect"],
    actions: ["inspect effect tree", "dispose plugin twice"],
    expected: ["tree contains label test", "undo runs exactly once"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let log = log();
        let fiber = support::expect_ok(
            kernel.spawn(kernel.root_context(), EffectPlugin(Arc::clone(&log)), ()).await,
            "plugin should activate",
        );
        assert!(kernel.effect_tree(fiber).iter().any(|effect| effect.label == "test"));
        support::expect_ok(kernel.dispose(fiber).await, "plugin should dispose");
        support::expect_ok(kernel.dispose(fiber).await, "plugin disposal should be idempotent");
        assert_eq!(read(&log), vec![1]);
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `dispose manually`.
    manual_dispose_is_visible_and_idempotent,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "dispose manually",
    setup: ["root registers one anonymous effect"],
    actions: ["invoke returned disposer twice"],
    expected: ["effect appears at root", "undo runs exactly once"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let log = log();
        let effect = support::expect_ok(
            kernel.register_effect(
                kernel.root_context(),
                "manual".to_owned(),
                Box::new(MarkUndo(Arc::clone(&log), 1)),
            ),
            "effect should register",
        );
        assert!(kernel.effect_tree(jinnd_adapter::KERNEL_SCOPE).iter().any(|item| item.id == effect));
        support::expect_ok(kernel.dispose_effect(effect).await, "effect should dispose");
        support::expect_ok(kernel.dispose_effect(effect).await, "effect disposal should be idempotent");
        assert_eq!(read(&log), vec![1]);
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `yield dispose`.
    nested_effects_unwind_in_reverse_order,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "yield dispose",
    setup: ["register nested effects with listener children and undo markers 1, 2, 3"],
    actions: ["inspect nested labels", "dispose outer effect twice"],
    expected: ["effect tree preserves parent-child shape", "undo sequence is 3, 2, 1 exactly once"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let log = log();
        let parent = support::expect_ok(
            kernel.register_effect(
                kernel.root_context(),
                "parent".to_owned(),
                Box::new(MarkUndo(Arc::clone(&log), 1)),
            ),
            "parent effect should register",
        );
        let child = support::expect_ok(
            kernel.register_child_effect(
                parent,
                "child".to_owned(),
                Box::new(MarkUndo(Arc::clone(&log), 2)),
            ),
            "child effect should register",
        );
        support::expect_ok(
            kernel.register_child_effect(
                child,
                "grandchild".to_owned(),
                Box::new(MarkUndo(Arc::clone(&log), 3)),
            ),
            "grandchild effect should register",
        );

        let tree = kernel.effect_tree(jinnd_adapter::KERNEL_SCOPE);
        let root = tree
            .iter()
            .find(|effect| effect.id == parent)
            .unwrap_or_else(|| panic!("the parent must be live in the tree"));
        assert_eq!(root.children.len(), 1);
        assert_eq!(root.children[0].label, "child");
        assert_eq!(root.children[0].children[0].label, "grandchild");

        support::expect_ok(kernel.dispose_effect(parent).await, "parent should dispose");
        support::expect_ok(
            kernel.dispose_effect(parent).await,
            "parent disposal should be idempotent",
        );
        assert_eq!(read(&log), vec![3, 2, 1]);
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `async return 1`.
    async_effect_return_registers_undo_after_forward_completes,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "async return 1",
    setup: ["start a 100ms asynchronous forward effect returning an undo"],
    actions: ["advance 100ms", "dispose effect"],
    expected: ["forward marker precedes undo marker", "sequence is 1, 2"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let log = log();
        let effect = support::expect_ok(
            kernel.begin_effect(kernel.root_context(), "async return".to_owned(), ForwardEffect::Plain(step(&log, 1, 2))),
            "effect should begin",
        );
        support::expect_ok(kernel.effect_outcome(effect).await, "effect should land");
        support::expect_ok(kernel.dispose_effect(effect).await, "effect should dispose");
        assert_eq!(read(&log), vec![1, 2]);
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `async return 2`.
    disposing_in_flight_async_effect_waits_then_undoes,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "async return 2",
    setup: ["start a 100ms asynchronous forward effect returning an undo"],
    actions: ["request disposal immediately", "advance 100ms"],
    expected: ["forward is allowed to land", "its undo follows immediately", "sequence is 1, 2"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let log = log();
        let (release, gate) = tokio::sync::oneshot::channel::<()>();
        let gated_log = Arc::clone(&log);
        let action: ForwardAction = Box::new(move || Box::pin(async move {
            let _ = gate.await;
            mark(&gated_log, 1);
            let undo: Box<dyn Undo> = Box::new(MarkUndo(gated_log, 2));
            Ok(undo)
        }));
        let effect = support::expect_ok(
            kernel.begin_effect(kernel.root_context(), "in flight".to_owned(), ForwardEffect::Plain(action)),
            "effect should begin",
        );
        tokio::task::yield_now().await;
        let (disposed, released) = tokio::join!(kernel.dispose_effect(effect), async move { release.send(()) });
        support::expect_ok(disposed, "effect should dispose");
        released.unwrap_or_else(|()| panic!("launched action should receive release"));
        assert_eq!(read(&log), vec![1, 2]);
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `async yield 1`.
    completed_async_iterator_unwinds_all_yielded_undos_lifo,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "async yield 1",
    setup: ["three 100ms iterator steps yield undo markers 2, 4, 6"],
    actions: ["advance 300ms", "dispose"],
    expected: ["forward sequence is 1, 3, 5", "final sequence is 1, 3, 5, 6, 4, 2"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let log = log();
        let effect = support::expect_ok(
            kernel.begin_effect(kernel.root_context(), "iterator".to_owned(), ForwardEffect::Steps(vec![step(&log, 1, 2), step(&log, 3, 4), step(&log, 5, 6)])),
            "iterator should begin",
        );
        support::expect_ok(kernel.effect_outcome(effect).await, "iterator should land");
        assert_eq!(read(&log), vec![1, 3, 5]);
        support::expect_ok(kernel.dispose_effect(effect).await, "iterator should dispose");
        assert_eq!(read(&log), vec![1, 3, 5, 6, 4, 2]);
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `async yield 2 (aborted)`.
    abort_before_first_async_yield_lands_then_undoes_first_step,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "async yield 2 (aborted)",
    setup: ["three-step asynchronous iterator effect"],
    actions: ["request disposal at 50ms", "advance through all timers"],
    expected: ["first launched step lands", "only first yielded inverse runs", "sequence is 1, 2"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let log = log();
        let (release, gate) = tokio::sync::oneshot::channel::<()>();
        let gated_log = Arc::clone(&log);
        let first: ForwardAction = Box::new(move || Box::pin(async move {
            let _ = gate.await;
            mark(&gated_log, 1);
            let undo: Box<dyn Undo> = Box::new(MarkUndo(gated_log, 2));
            Ok(undo)
        }));
        let effect = support::expect_ok(
            kernel.begin_effect(kernel.root_context(), "iterator".to_owned(), ForwardEffect::Steps(vec![first, step(&log, 3, 4)])),
            "iterator should begin",
        );
        tokio::task::yield_now().await;
        let (disposed, released) = tokio::join!(kernel.dispose_effect(effect), async move { release.send(()) });
        support::expect_ok(disposed, "iterator should dispose");
        released.unwrap_or_else(|()| panic!("launched step should receive release"));
        assert_eq!(read(&log), vec![1, 2]);
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `async yield 3 (aborted)`.
    abort_after_first_yield_lands_next_step_then_unwinds,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "async yield 3 (aborted)",
    setup: ["three-step asynchronous iterator effect", "first step has yielded its inverse"],
    actions: ["request disposal at 100ms", "advance 200ms"],
    expected: ["launched second step lands", "inverse order is second then first", "sequence is 1, 3, 4, 2"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let log = log();
        let (release, gate) = tokio::sync::oneshot::channel::<()>();
        let gated_log = Arc::clone(&log);
        let second: ForwardAction = Box::new(move || Box::pin(async move {
            let _ = gate.await;
            mark(&gated_log, 3);
            let undo: Box<dyn Undo> = Box::new(MarkUndo(gated_log, 4));
            Ok(undo)
        }));
        let effect = support::expect_ok(
            kernel.begin_effect(kernel.root_context(), "iterator".to_owned(), ForwardEffect::Steps(vec![step(&log, 1, 2), second, step(&log, 5, 6)])),
            "iterator should begin",
        );
        wait_for_len(&log, 1).await;
        tokio::task::yield_now().await;
        let (disposed, released) = tokio::join!(kernel.dispose_effect(effect), async move { release.send(()) });
        support::expect_ok(disposed, "iterator should dispose");
        released.unwrap_or_else(|()| panic!("launched step should receive release"));
        assert_eq!(read(&log), vec![1, 3, 4, 2]);
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `async yield 4 (await dispose)`.
    awaiting_async_effect_returns_an_idempotent_disposer,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "async yield 4 (await dispose)",
    setup: ["three-step asynchronous iterator effect"],
    actions: ["await effect completion", "invoke returned disposer"],
    expected: ["forward sequence is 1, 3, 5", "undo sequence appends 6, 4, 2"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let log = log();
        let effect = support::expect_ok(
            kernel.begin_effect(kernel.root_context(), "iterator".to_owned(), ForwardEffect::Steps(vec![step(&log, 1, 2), step(&log, 3, 4), step(&log, 5, 6)])),
            "iterator should begin",
        );
        support::expect_ok(kernel.effect_outcome(effect).await, "iterator should land");
        support::expect_ok(kernel.dispose_effect(effect).await, "iterator should dispose");
        support::expect_ok(kernel.dispose_effect(effect).await, "disposer should be idempotent");
        assert_eq!(read(&log), vec![1, 3, 5, 6, 4, 2]);
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `return with error`.
    synchronous_effect_failure_registers_no_inverse,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "return with error",
    setup: ["forward effect fails before returning an inverse"],
    actions: ["register effect"],
    expected: ["registration returns the original error", "no inverse runs"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let effect = support::expect_ok(
            kernel.begin_effect(kernel.root_context(), "failure".to_owned(), ForwardEffect::Plain(failure("original"))),
            "effect should begin",
        );
        let error = expect_failure(kernel.effect_outcome(effect).await, "effect should fail");
        assert_eq!(error.message, "original");
        assert!(kernel.effect_tree(jinnd_adapter::KERNEL_SCOPE).iter().all(|item| item.id != effect));
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `yield with error`.
    synchronous_iterator_failure_unwinds_prior_yields,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "yield with error",
    setup: ["iterator yields inverse 1 then fails before inverse 2"],
    actions: ["register effect"],
    expected: ["registration returns the original error", "inverse 1 runs immediately"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let log = log();
        let effect = support::expect_ok(
            kernel.begin_effect(kernel.root_context(), "failure".to_owned(), ForwardEffect::Steps(vec![step(&log, 1, 2), failure("original")])),
            "iterator should begin",
        );
        let error = expect_failure(kernel.effect_outcome(effect).await, "iterator should fail");
        assert_eq!(error.message, "original");
        assert_eq!(read(&log), vec![1, 2]);
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `async return with error`.
    asynchronous_effect_failure_registers_no_inverse,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "async return with error",
    setup: ["asynchronous forward effect fails before returning an inverse"],
    actions: ["await effect registration"],
    expected: ["future returns the original error", "no inverse runs"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let action: ForwardAction = Box::new(|| Box::pin(async {
            tokio::task::yield_now().await;
            Err(KernelError { code: ErrorCode::EffectFailed, message: "original".to_owned(), fiber: None })
        }));
        let effect = support::expect_ok(
            kernel.begin_effect(kernel.root_context(), "failure".to_owned(), ForwardEffect::Plain(action)),
            "effect should begin",
        );
        let error = expect_failure(kernel.effect_outcome(effect).await, "effect should fail");
        assert_eq!(error.message, "original");
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/dispose.spec.ts`, test `async yield with error`.
    asynchronous_iterator_failure_unwinds_prior_yields,
    origin: "packages/core/tests/dispose.spec.ts",
    test: "async yield with error",
    setup: ["asynchronous iterator yields inverse 1 then fails"],
    actions: ["await effect registration"],
    expected: ["future returns the original error", "inverse 1 runs immediately"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let log = log();
        let action: ForwardAction = Box::new(|| Box::pin(async {
            tokio::task::yield_now().await;
            Err(KernelError { code: ErrorCode::EffectFailed, message: "original".to_owned(), fiber: None })
        }));
        let effect = support::expect_ok(
            kernel.begin_effect(kernel.root_context(), "failure".to_owned(), ForwardEffect::Steps(vec![step(&log, 1, 2), action])),
            "iterator should begin",
        );
        let error = expect_failure(kernel.effect_outcome(effect).await, "iterator should fail");
        assert_eq!(error.message, "original");
        assert_eq!(read(&log), vec![1, 2]);
    }
}
