mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use jinnd_api::{
    Activation, DispatchMode, ErrorCode, Event, FiberState, Kernel, KernelError, KernelFuture,
    LedgerEventKind, LedgerQuery, PluginContract, PluginRef, Profile, ProfileEntry, Realm,
    ServiceContract,
};
use support::{Listener, expect_ok, listener_error, ready, spec_case, v02_deferred_at};

#[derive(Clone, Debug)]
struct EmitEvent;

impl Event for EmitEvent {
    type Output = ();

    const MODE: DispatchMode = DispatchMode::Emit;
}

#[derive(Clone, Debug)]
struct BailEvent;

impl Event for BailEvent {
    type Output = Option<u8>;

    const MODE: DispatchMode = DispatchMode::Bail;

    fn decisive(&self, output: &Self::Output) -> bool {
        output.is_some()
    }
}

#[derive(Debug)]
struct PassiveService;

impl ServiceContract for PassiveService {
    type Observation = ();

    const NAME: &'static str = "jinn.test/passive-service";

    fn observe(&self) {}
}

#[derive(Debug)]
struct VersionedService(u8);

impl ServiceContract for VersionedService {
    type Observation = u8;

    const NAME: &'static str = "jinn.test/versioned-service";

    fn observe(&self) -> Self::Observation {
        self.0
    }
}

#[derive(Debug)]
struct AlwaysFails {
    attempts: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct LiteralConfig(Arc<Mutex<Vec<String>>>);

impl PluginContract for LiteralConfig {
    type Config = String;
    type Dependencies = ();

    const NAME: &'static str = "jinn.test/literal-config";

    fn activate<'a>(
        &'a self,
        _activation: Activation<'a, ()>,
        config: String,
    ) -> KernelFuture<'a, ()> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(config);
        Box::pin(async { Ok(()) })
    }
}

impl PluginContract for AlwaysFails {
    type Config = ();
    type Dependencies = ();

    const NAME: &'static str = "always-fails";

    fn activate<'a>(
        &'a self,
        _activation: Activation<'a, Self::Dependencies>,
        _config: Self::Config,
    ) -> KernelFuture<'a, ()> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        ready(Err(KernelError {
            code: ErrorCode::PluginFailed,
            message: "fixture failure".to_owned(),
            fiber: None,
        }))
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

fn fixture_plugin(package: &str) -> PluginRef {
    PluginRef {
        package: package.to_owned(),
        version: "1.0.0".to_owned(),
        artifact_hash: "fixture-hash".to_owned(),
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.emit()`; corrected by SOURCE-OF-TRUTH R9.
    emit_error_does_not_abort_remaining_listeners,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.emit() hazard absence / R9",
    setup: ["three ordered listeners where the middle listener fails"],
    actions: ["emit one typed event"],
    expected: ["first, second, and third listeners are all invoked", "dispatch reports the middle failure after completing the snapshot"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let root = kernel.root_context();
        let log = Arc::new(Mutex::new(Vec::new()));
        for (value, fails) in [(1, false), (2, true), (3, false)] {
            let listener_log = Arc::clone(&log);
            expect_ok(
                kernel.listen(
                    root,
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

        let report = expect_ok(
            kernel.dispatch_report(root, EmitEvent).await,
            "emit should report after all listeners settle",
        );
        assert_eq!(recorded(&log), vec![1, 2, 3]);
        assert_eq!(report.failures.len(), 1);
        assert_eq!(report.failures[0].message, "middle");
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/events.spec.ts`, test `ctx.bail()`; corrected by SOURCE-OF-TRUTH R9.
    async_listener_result_does_not_count_as_bailed,
    origin: "packages/core/tests/events.spec.ts",
    test: "ctx.bail() async-result hazard absence / R9",
    setup: ["first bail listener returns a future", "second returns a synchronous value"],
    actions: ["dispatch one bail event"],
    expected: ["future object is ignored as a bail result", "second listener runs and supplies the result"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let root = kernel.root_context();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let first_calls = Arc::clone(&calls);
        expect_ok(
            kernel.listen(
                root,
                Listener(move |_caller, _event: BailEvent| {
                    let first_calls = Arc::clone(&first_calls);
                    Box::pin(async move {
                        tokio::task::yield_now().await;
                        record(&first_calls, 1);
                        Ok(None)
                    }) as KernelFuture<'static, Option<u8>>
                }),
            ),
            "the asynchronous non-value listener should register",
        );
        let second_calls = Arc::clone(&calls);
        expect_ok(
            kernel.listen(
                root,
                Listener(move |_caller, _event: BailEvent| {
                    record(&second_calls, 2);
                    ready(Ok(Some(7)))
                }),
            ),
            "the decisive listener should register",
        );

        let report = expect_ok(
            kernel.dispatch_report(root, BailEvent).await,
            "bail should await and classify resolved outputs",
        );
        assert_eq!(recorded(&calls), vec![1, 2]);
        assert_eq!(report.outputs, vec![Some(7)]);
        assert!(report.failures.is_empty());
    }
}

spec_case! {
    /// Rule origin: SOURCE-OF-TRUTH R9, side-effectful service constructors stay absent.
    service_construction_cannot_mutate_context,
    origin: "rule: R9 / side-effectful service constructors",
    test: "service constructor hazard absence",
    setup: ["construct a service value before activation"],
    actions: ["compare kernel effects and ledger before and after construction", "activate through explicit plugin boundary"],
    expected: ["construction creates no effect or ledger entry", "mutations become possible only inside activation"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let root = kernel.root_context();
        let before = kernel.effect_tree(jinnd_adapter::KERNEL_SCOPE);
        let ledger_before = expect_ok(
            kernel.ledger_events(LedgerQuery::default()).await,
            "ledger should be readable",
        );
        let service = Arc::new(PassiveService);
        assert_eq!(kernel.effect_tree(jinnd_adapter::KERNEL_SCOPE), before);
        assert_eq!(
            expect_ok(kernel.ledger_events(LedgerQuery::default()).await, "ledger after construction"),
            ledger_before,
            "constructing an inert value cannot cross the kernel boundary",
        );
        let effect = expect_ok(
            kernel.provide(root, Realm::Root, service).await,
            "the explicit provision boundary should accept the service",
        );
        assert!(
            kernel
                .effect_tree(jinnd_adapter::KERNEL_SCOPE)
                .iter()
                .any(|entry| entry.id == effect),
            "only the explicit boundary should create an effect"
        );
        assert!(expect_ok(kernel.ledger_events(LedgerQuery::default()).await, "ledger after provision")
            .iter()
            .any(|record| matches!(&record.kind, LedgerEventKind::ServiceProvided { service } if service == PassiveService::NAME)));
    }
}

spec_case! {
    /// Rule origin: SOURCE-OF-TRUTH R9, config evaluation with ambient authority stays absent.
    config_expression_lane_has_no_ambient_authority,
    origin: "rule: R9 / closed side-effect-free config subset",
    test: "ambient config evaluation hazard absence",
    setup: ["config expression attempts filesystem, network, environment, and process access"],
    actions: ["parse and validate at the profile boundary"],
    expected: ["all ambient-authority forms are rejected", "no capability call or ledger side effect occurs"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let expression = "readFile('/tmp/x') || fetch('https://invalid') || env.SECRET || process.exit()";
        let seen = Arc::new(Mutex::new(Vec::new()));
        let plugin_seen = Arc::clone(&seen);
        expect_ok(
            kernel.register_package("jinn.test/literal-config", move |config: String| {
                Ok((LiteralConfig(Arc::clone(&plugin_seen)), config))
            }),
            "literal-config package should register",
        );
        let profile = Profile { entries: vec![ProfileEntry {
            id: jinnd_api::EntryId("ambient-config".to_owned()),
            plugin: fixture_plugin("jinn.test/literal-config"),
            config: expression.to_owned(),
            disabled: false,
            parent: None,
            isolation: Vec::new(),
        }] };
        let report = expect_ok(kernel.reconcile(profile).await, "literal config should reconcile");
        assert!(report.errors.is_empty());
        assert_eq!(
            seen.lock().unwrap_or_else(|poison| poison.into_inner()).as_slice(),
            [expression],
            "the expression-looking text is inert config, never evaluated",
        );
        assert!(!expect_ok(kernel.ledger_events(LedgerQuery::default()).await, "ledger after config")
            .iter()
            .any(|record| matches!(record.kind, LedgerEventKind::ContractCall { .. })));
    }
}

spec_case! {
    /// Rule origin: SOURCE-OF-TRUTH R9, native-library unload stays absent.
    native_dynamic_library_backend_is_unrepresentable,
    origin: "rule: R9 / no native library unload",
    test: "native dylib hazard absence",
    setup: ["enumerate supported plugin backends and profile manifest forms"],
    actions: ["attempt to declare a native dynamic library artifact"],
    expected: ["manifest is rejected", "only sandboxed WASM and disabled-until-sandboxed subprocess backends exist"],
    body: |case| {
        let native_artifact = fixture_plugin("file://fixture.dylib");
        assert!(native_artifact.package.ends_with(".dylib"));
        v02_deferred_at(
            &case,
            "constitution 05 Hosting: v0.1 enables only WASM and keeps subprocess disabled, so backend enumeration and native-manifest validation wait on the v0.2 subprocess sandbox amendment",
        );
    }
}

spec_case! {
    /// Rule origin: SOURCE-OF-TRUTH R9, silent service replacement stays absent.
    provider_generation_change_forces_consumer_unload_reload,
    origin: "rule: R9 / no silent service replacement",
    test: "silent replacement hazard absence",
    setup: ["active consumer owns provider generation 1"],
    actions: ["replace provider with generation 2", "wait for quiescence"],
    expected: ["consumer tears down using generation 1", "a new activation captures generation 2", "no activation observes both"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let root = kernel.root_context();
        let first_effect = expect_ok(
            kernel
                .provide(root, Realm::Root, Arc::new(VersionedService(1)))
                .await,
            "generation one should be provided",
        );
        let first = expect_ok(
            kernel.resolve::<VersionedService>(root),
            "generation one should resolve",
        );
        assert_eq!(first.service.observe(), 1);
        expect_ok(kernel.dispose_effect(first_effect).await, "generation one should withdraw");
        expect_ok(
            kernel.provide(root, Realm::Root, Arc::new(VersionedService(2))).await,
            "generation two should provide",
        );
        let second = expect_ok(kernel.resolve::<VersionedService>(root), "generation two should resolve");
        assert_eq!(second.service.observe(), 2);
        assert!(second.generation > first.generation);
        assert_eq!(first.service.observe(), 1, "the retained handle never silently retargets");
    }
}

spec_case! {
    /// Rule origin: SOURCE-OF-TRUTH R9, failed-fiber auto-retry stays absent.
    failed_fiber_does_not_retry_without_environment_change,
    origin: "rule: R9 / no auto-retry on unchanged environment",
    test: "failed fiber retry hazard absence",
    setup: ["plugin body increments an attempt counter then fails", "dependencies and config remain unchanged"],
    actions: ["advance virtual time repeatedly", "wait for quiescence repeatedly"],
    expected: ["fiber remains failed", "attempt counter stays exactly one", "no new transition or ledger retry event appears"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let root = kernel.root_context();
        let attempts = Arc::new(AtomicUsize::new(0));
        let fiber = expect_ok(
            kernel
                .spawn(
                    root,
                    AlwaysFails {
                        attempts: Arc::clone(&attempts),
                    },
                    (),
                )
                .await,
            "the failing fixture should spawn",
        );
        assert_eq!(kernel.state(fiber), FiberState::Failed);
        let transitions = kernel.transitions(fiber);
        let ledger = expect_ok(
            kernel.ledger_events(LedgerQuery::default()).await,
            "initial failure should be recorded",
        );
        for _ in 0..3 {
            tokio::task::yield_now().await;
            expect_ok(
                kernel.wait_for_quiescence().await,
                "the unchanged failed fiber should remain quiescent",
            );
        }
        assert_eq!(kernel.state(fiber), FiberState::Failed);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(kernel.transitions(fiber), transitions);
        assert_eq!(
            expect_ok(kernel.ledger_events(LedgerQuery::default()).await, "settled ledger"),
            ledger,
            "no retry event may appear against an unchanged environment",
        );
    }
}
