mod support;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use jinnd_api::{ContextId, DispatchMode, Event, EventListener, Kernel, KernelFuture};
use support::{Listener, expect_ok, listener_error, ready, spec_case, v02_deferred_at};

#[derive(Clone, Debug)]
struct EmitEvent {
    target: ContextId,
}

impl Event for EmitEvent {
    type Output = ();

    const MODE: DispatchMode = DispatchMode::Emit;

    fn selects(&self, listener: ContextId) -> bool {
        listener == self.target
    }
}

#[derive(Clone, Debug)]
struct ParallelEvent {
    target: ContextId,
}

impl Event for ParallelEvent {
    type Output = u8;

    const MODE: DispatchMode = DispatchMode::Parallel;

    fn selects(&self, listener: ContextId) -> bool {
        listener == self.target
    }
}

#[derive(Clone, Debug)]
struct SerialEvent {
    target: ContextId,
}

impl Event for SerialEvent {
    type Output = ();

    const MODE: DispatchMode = DispatchMode::Serial;

    fn selects(&self, listener: ContextId) -> bool {
        listener == self.target
    }
}

#[derive(Clone, Debug)]
struct BailEvent {
    target: ContextId,
}

impl Event for BailEvent {
    type Output = Option<u8>;

    const MODE: DispatchMode = DispatchMode::Bail;

    fn selects(&self, listener: ContextId) -> bool {
        listener == self.target
    }

    fn decisive(&self, output: &Self::Output) -> bool {
        output.is_some()
    }
}

#[derive(Clone, Debug)]
struct WaterfallEvent {
    value: i32,
}

#[derive(Debug)]
struct WaterfallStep {
    delta: i32,
    continues: bool,
}

impl Event for WaterfallEvent {
    type Output = WaterfallStep;

    const MODE: DispatchMode = DispatchMode::Waterfall;

    fn absorb(&mut self, output: Self::Output) -> bool {
        self.value += output.delta;
        output.continues
    }
}

#[derive(Debug)]
struct ClonePanicEvent;

impl Clone for ClonePanicEvent {
    fn clone(&self) -> Self {
        panic!("plugin-authored payload clone panicked")
    }
}

impl Event for ClonePanicEvent {
    type Output = ();

    const MODE: DispatchMode = DispatchMode::Parallel;
}

#[derive(Debug)]
struct DropPanicListener;

impl EventListener<EmitEvent> for DropPanicListener {
    fn call<'a>(&'a self, _caller: ContextId, _event: EmitEvent) -> KernelFuture<'a, ()> {
        ready(Ok(()))
    }
}

impl Drop for DropPanicListener {
    fn drop(&mut self) {
        panic!("once-listener destructor panicked")
    }
}

fn record(log: &Mutex<Vec<u8>>, value: u8) {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .push(value);
}

fn recorded(log: &Mutex<Vec<u8>>) -> Vec<u8> {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
}

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.on()`.
    listener_effect_receives_until_disposed,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.on()",
    setup: ["register one typed listener as an effect"],
    actions: ["emit twice", "dispose listener", "emit again"],
    expected: ["listener call count is exactly two"],
    body: |case| {
        let kernel = jinnd_adapter::kernel();
        let root = kernel.root_context();
        let calls = Arc::new(AtomicUsize::new(0));
        let listener_calls = Arc::clone(&calls);
        let effect = expect_ok(
            kernel.listen(
                root,
                Listener(move |_caller, _event: EmitEvent| {
                    listener_calls.fetch_add(1, Ordering::SeqCst);
                    ready(Ok(()))
                }),
            ),
            "the listener should register",
        );
        assert!(
            kernel
                .effect_tree(jinnd_adapter::KERNEL_SCOPE)
                .iter()
                .any(|entry| entry.id == effect),
            "listener registration must be visible as its owning effect"
        );

        expect_ok(
            kernel.dispatch_report(root, EmitEvent { target: root }).await,
            "first emit should settle",
        );
        expect_ok(
            kernel.dispatch_report(root, EmitEvent { target: root }).await,
            "second emit should settle",
        );
        expect_ok(kernel.unlisten(effect), "listener disposal should succeed");
        expect_ok(
            kernel.dispatch_report(root, EmitEvent { target: root }).await,
            "post-disposal emit should settle",
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        if kernel
            .effect_tree(jinnd_adapter::KERNEL_SCOPE)
            .iter()
            .any(|entry| entry.id == effect)
        {
            v02_deferred_at(
                &case,
                "unlisten removes delivery but leaves the listener's live effect record",
            );
        }
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.once()`.
    once_listener_disposes_before_second_dispatch,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.once()",
    setup: ["register one once-listener"],
    actions: ["emit twice", "dispose returned effect", "emit again"],
    expected: ["listener call count is exactly one"],
    body: |case| {
        let kernel = jinnd_adapter::kernel();
        let root = kernel.root_context();
        let calls = Arc::new(AtomicUsize::new(0));
        let listener_calls = Arc::clone(&calls);
        let effect = expect_ok(
            kernel.listen_once(
                root,
                Listener(move |_caller, _event: EmitEvent| {
                    listener_calls.fetch_add(1, Ordering::SeqCst);
                    ready(Ok(()))
                }),
            ),
            "the once-listener should register",
        );

        expect_ok(
            kernel.dispatch_report(root, EmitEvent { target: root }).await,
            "first once dispatch should settle",
        );
        expect_ok(
            kernel.dispatch_report(root, EmitEvent { target: root }).await,
            "second once dispatch should settle",
        );
        expect_ok(
            kernel.unlisten(effect),
            "disposing the consumed effect should be idempotent",
        );
        expect_ok(
            kernel.dispatch_report(root, EmitEvent { target: root }).await,
            "post-disposal dispatch should settle",
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let panic_kernel = jinnd_adapter::kernel();
        let panic_root = panic_kernel.root_context();
        expect_ok(
            panic_kernel.listen_once(panic_root, DropPanicListener),
            "the destructor probe should register",
        );
        let dispatch = tokio::spawn(async move {
            panic_kernel
                .dispatch_report(panic_root, EmitEvent { target: panic_root })
                .await
        })
        .await;
        match dispatch {
            Err(error) if error.is_panic() => v02_deferred_at(
                &case,
                "a consumed once-listener's destructor panic escapes dispatch containment",
            ),
            Err(error) => panic!("the once-listener dispatch task was cancelled: {error:?}"),
            Ok(report) => {
                let report = expect_ok(report, "the destructor failure should be reported");
                if report.failures.len() != 1
                    || report.failures[0].code != jinnd_api::ErrorCode::ListenerFailed
                {
                    v02_deferred_at(
                        &case,
                        "a consumed once-listener's destructor panic is not reported as one contained listener failure",
                    );
                }
            }
        }
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.parallel()`.
    parallel_dispatch_filters_and_aggregates_all_listener_errors,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.parallel()",
    setup: ["register context-filtered listener", "register synchronous and delayed failing listeners"],
    actions: ["dispatch matching and nonmatching payloads", "dispatch to both failures"],
    expected: ["only matching context receives payload", "all listeners settle", "both errors are returned in one aggregate"],
    body: |case| {
        let kernel = jinnd_adapter::kernel();
        let root = kernel.root_context();
        let child = kernel.derive_context(root, Vec::new());
        let matching_calls = Arc::new(AtomicUsize::new(0));
        let listener_calls = Arc::clone(&matching_calls);
        expect_ok(
            kernel.listen(
                child,
                Listener(move |_caller, _event: ParallelEvent| {
                    listener_calls.fetch_add(1, Ordering::SeqCst);
                    ready(Ok(1))
                }),
            ),
            "the filtered listener should register",
        );
        expect_ok(
            kernel
                .dispatch_report(root, ParallelEvent { target: root })
                .await,
            "the nonmatching parallel dispatch should settle",
        );
        expect_ok(
            kernel
                .dispatch_report(root, ParallelEvent { target: child })
                .await,
            "the matching parallel dispatch should settle",
        );
        assert_eq!(matching_calls.load(Ordering::SeqCst), 1);

        let settled = Arc::new(AtomicBool::new(false));
        expect_ok(
            kernel.listen(
                root,
                Listener(move |_caller, _event: ParallelEvent| {
                    ready(Err(listener_error("synchronous")))
                }),
            ),
            "the synchronous failure should register",
        );
        let delayed_settled = Arc::clone(&settled);
        expect_ok(
            kernel.listen(
                root,
                Listener(move |_caller, _event: ParallelEvent| {
                    let delayed_settled = Arc::clone(&delayed_settled);
                    Box::pin(async move {
                        tokio::task::yield_now().await;
                        delayed_settled.store(true, Ordering::SeqCst);
                        Err(listener_error("asynchronous"))
                    }) as KernelFuture<'static, u8>
                }),
            ),
            "the asynchronous failure should register",
        );
        let report = expect_ok(
            kernel
                .dispatch_report(root, ParallelEvent { target: root })
                .await,
            "parallel failures should be aggregated",
        );
        assert!(settled.load(Ordering::SeqCst));
        assert_eq!(report.failures.len(), 2);
        assert!(report.failures.iter().any(|error| error.message == "synchronous"));
        assert!(report.failures.iter().any(|error| error.message == "asynchronous"));

        let panic_kernel = jinnd_adapter::kernel();
        let panic_root = panic_kernel.root_context();
        expect_ok(
            panic_kernel.listen(
                panic_root,
                Listener(move |_caller, _event: ClonePanicEvent| ready(Ok(()))),
            ),
            "the payload clone probe should register",
        );
        let dispatch = tokio::spawn(async move {
            panic_kernel
                .dispatch_report(panic_root, ClonePanicEvent)
                .await
        })
        .await;
        match dispatch {
            Err(error) if error.is_panic() => v02_deferred_at(
                &case,
                "parallel dispatch clones the plugin-authored payload outside containment",
            ),
            Err(error) => panic!("the parallel dispatch task was cancelled: {error:?}"),
            Ok(report) => {
                let report = expect_ok(report, "the payload clone failure should be reported");
                if report.failures.len() != 1
                    || report.failures[0].code != jinnd_api::ErrorCode::ListenerFailed
                {
                    v02_deferred_at(
                        &case,
                        "parallel dispatch does not report the payload clone panic as one contained listener failure",
                    );
                }
            }
        }
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.emit()`; R9 changes its error expectation.
    emit_filters_without_aborting_after_listener_error,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.emit() (R9 hazard-corrected)",
    setup: ["register matching listener followed by a failing listener and a trailing listener"],
    actions: ["emit matching and nonmatching typed payloads", "emit when middle listener fails"],
    expected: ["filtering follows payload-to-listener context routing", "trailing listener still runs", "failure is reported after dispatch"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let root = kernel.root_context();
        let child = kernel.derive_context(root, Vec::new());
        let log = Arc::new(Mutex::new(Vec::new()));
        for (context, value, fails) in [(root, 1, false), (root, 2, true), (root, 3, false)] {
            let listener_log = Arc::clone(&log);
            expect_ok(
                kernel.listen(
                    context,
                    Listener(move |_caller, _event: EmitEvent| {
                        record(&listener_log, value);
                        if fails {
                            ready(Err(listener_error("middle")))
                        } else {
                            ready(Ok(()))
                        }
                    }),
                ),
                "the emit listener should register",
            );
        }
        expect_ok(
            kernel.dispatch_report(root, EmitEvent { target: child }).await,
            "the nonmatching emit should settle",
        );
        assert!(recorded(&log).is_empty());

        let report = expect_ok(
            kernel.dispatch_report(root, EmitEvent { target: root }).await,
            "the matching emit should settle",
        );
        assert_eq!(recorded(&log), vec![1, 2, 3]);
        assert_eq!(report.outputs.len(), 0);
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].message, "middle");
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.serial()`.
    serial_dispatch_filters_orders_and_propagates_failure,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.serial()",
    setup: ["register ordered context-filtered listeners"],
    actions: ["serially dispatch matching and nonmatching payloads", "make one listener fail"],
    expected: ["matching listeners run in registration order", "dispatch returns listener error"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let root = kernel.root_context();
        let child = kernel.derive_context(root, Vec::new());
        let log = Arc::new(Mutex::new(Vec::new()));
        for (value, fails) in [(1, false), (2, true), (3, false)] {
            let listener_log = Arc::clone(&log);
            expect_ok(
                kernel.listen(
                    root,
                    Listener(move |_caller, _event: SerialEvent| {
                        record(&listener_log, value);
                        if fails {
                            ready(Err(listener_error("serial")))
                        } else {
                            ready(Ok(()))
                        }
                    }),
                ),
                "the serial listener should register",
            );
        }
        expect_ok(
            kernel.dispatch(root, SerialEvent { target: child }).await,
            "the nonmatching serial dispatch should settle",
        );
        assert!(recorded(&log).is_empty());

        let error = match kernel.dispatch(root, SerialEvent { target: root }).await {
            Ok(_) => panic!("the serial listener failure should reach the caller after the walk"),
            Err(error) => error,
        };
        assert_eq!(error.code, jinnd_api::ErrorCode::ListenerFailed);
        assert_eq!(error.message, "serial");
        assert_eq!(recorded(&log), vec![1, 2, 3]);
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.bail()`; async-result semantics corrected by R9.
    bail_returns_first_synchronous_value_only,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.bail() (R9 hazard-corrected)",
    setup: ["register filtered listeners returning none, a future, and a synchronous value"],
    actions: ["dispatch matching and nonmatching payloads"],
    expected: ["a future object is not a bail value", "first synchronous non-none value stops dispatch"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let root = kernel.root_context();
        let child = kernel.derive_context(root, Vec::new());
        let log = Arc::new(Mutex::new(Vec::new()));

        let first_log = Arc::clone(&log);
        expect_ok(
            kernel.listen(
                root,
                Listener(move |_caller, _event: BailEvent| {
                    let first_log = Arc::clone(&first_log);
                    Box::pin(async move {
                        tokio::task::yield_now().await;
                        record(&first_log, 1);
                        Ok(None)
                    }) as KernelFuture<'static, Option<u8>>
                }),
            ),
            "the pending non-value listener should register",
        );
        let second_log = Arc::clone(&log);
        expect_ok(
            kernel.listen(
                root,
                Listener(move |_caller, _event: BailEvent| {
                    record(&second_log, 2);
                    ready(Ok(Some(7)))
                }),
            ),
            "the decisive listener should register",
        );
        let third_log = Arc::clone(&log);
        expect_ok(
            kernel.listen(
                root,
                Listener(move |_caller, _event: BailEvent| {
                    record(&third_log, 3);
                    ready(Ok(Some(9)))
                }),
            ),
            "the trailing listener should register",
        );

        let nonmatching = expect_ok(
            kernel.dispatch_report(root, BailEvent { target: child }).await,
            "the nonmatching bail dispatch should settle",
        );
        assert!(nonmatching.outputs.is_empty());
        assert!(recorded(&log).is_empty());

        let report = expect_ok(
            kernel.dispatch_report(root, BailEvent { target: root }).await,
            "the matching bail dispatch should settle",
        );
        assert_eq!(report.outputs, vec![Some(7)]);
        assert_eq!(recorded(&log), vec![1, 2]);
        assert!(report.failures.is_empty());
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.waterfall()`.
    waterfall_composes_until_middleware_declines_next,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.waterfall()",
    setup: ["register two additive middleware listeners", "then add a listener that does not call next"],
    actions: ["dispatch waterfall before and after terminal middleware"],
    expected: ["first result is 4", "second result is 3", "listeners after terminal middleware do not run"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let root = kernel.root_context();
        for delta in [1, 1] {
            expect_ok(
                kernel.listen(
                    root,
                    Listener(move |_caller, _event: WaterfallEvent| {
                        ready(Ok(WaterfallStep {
                            delta,
                            continues: true,
                        }))
                    }),
                ),
                "the additive middleware should register",
            );
        }
        let first = expect_ok(
            kernel
                .dispatch_report(root, WaterfallEvent { value: 2 })
                .await,
            "the first waterfall should settle",
        );
        assert_eq!(first.event.value, 4);

        expect_ok(
            kernel.listen(
                root,
                Listener(move |_caller, _event: WaterfallEvent| {
                    ready(Ok(WaterfallStep {
                        delta: -1,
                        continues: false,
                    }))
                }),
            ),
            "the terminal middleware should register",
        );
        let trailing_calls = Arc::new(AtomicUsize::new(0));
        let listener_calls = Arc::clone(&trailing_calls);
        expect_ok(
            kernel.listen(
                root,
                Listener(move |_caller, _event: WaterfallEvent| {
                    listener_calls.fetch_add(1, Ordering::SeqCst);
                    ready(Ok(WaterfallStep {
                        delta: 100,
                        continues: true,
                    }))
                }),
            ),
            "the trailing middleware should register",
        );
        let second = expect_ok(
            kernel
                .dispatch_report(root, WaterfallEvent { value: 2 })
                .await,
            "the terminal waterfall should settle",
        );
        assert_eq!(second.event.value, 3);
        assert_eq!(trailing_calls.load(Ordering::SeqCst), 0);
    }
}
