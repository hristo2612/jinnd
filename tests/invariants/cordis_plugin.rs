mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use jinnd_api::{
    Activation, EntryId, ErrorCode, Kernel, KernelFuture, LedgerEventKind, LedgerQuery,
    PluginContract, Undo, WasmArtifact, WasmLane,
};
use support::{expect_ok, spec_case};

const SUBSYSTEM: support::Subsystem = support::Subsystem::Fiber;
const V02_DEFERRED_BOUND: &str = "constitution 04 makes the host-owned profile the v0.1 composition authority; plugins receive no registry iteration, root disposal, or context-reflection control contract";

#[derive(Debug)]
struct ConfigPlugin {
    seen: Arc<Mutex<Vec<String>>>,
}

impl PluginContract for ConfigPlugin {
    type Config = String;
    type Dependencies = ();

    const NAME: &'static str = "jinn.test/config-plugin";

    fn activate<'a>(
        &'a self,
        _activation: Activation<'a, ()>,
        config: String,
    ) -> KernelFuture<'a, ()> {
        self.seen
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(config);
        Box::pin(async { Ok(()) })
    }
}

struct CountUndo(Arc<AtomicUsize>);

impl Undo for CountUndo {
    fn undo(self: Box<Self>) -> KernelFuture<'static, ()> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    }
}

#[derive(Debug)]
struct InitializingPlugin {
    initialized: Arc<AtomicUsize>,
    undone: Arc<AtomicUsize>,
}

impl PluginContract for InitializingPlugin {
    type Config = ();
    type Dependencies = ();

    const NAME: &'static str = "jinn.test/initializing-plugin";

    fn activate<'a>(&'a self, activation: Activation<'a, ()>, _config: ()) -> KernelFuture<'a, ()> {
        self.initialized.fetch_add(1, Ordering::SeqCst);
        let undone = Arc::clone(&self.undone);
        Box::pin(async move {
            activation.effects.register(
                "initialization undo".to_owned(),
                Box::new(CountUndo(undone)),
            )?;
            Ok(())
        })
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/plugin.spec.ts`, test `apply functional plugin`.
    functional_plugin_receives_typed_config_once,
    origin: "packages/core/tests/plugin.spec.ts",
    test: "apply functional plugin",
    setup: ["define a functional plugin contract and config foo=bar"],
    actions: ["spawn and await activation"],
    expected: ["plugin body runs exactly once with the supplied config"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let fiber = expect_ok(
            kernel.spawn(kernel.root_context(), ConfigPlugin { seen: Arc::clone(&seen) }, "foo=bar".to_owned()).await,
            "functional fixture should spawn",
        );
        assert_eq!(kernel.state(fiber), jinnd_api::FiberState::Active);
        assert_eq!(*seen.lock().unwrap_or_else(|poison| poison.into_inner()), vec!["foo=bar"]);
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/plugin.spec.ts`, test `apply object plugin`.
    object_plugin_receives_typed_config_once,
    origin: "packages/core/tests/plugin.spec.ts",
    test: "apply object plugin",
    setup: ["define a struct plugin contract and config bar=foo"],
    actions: ["spawn and await activation"],
    expected: ["plugin body runs exactly once with the supplied config"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let plugin = ConfigPlugin { seen: Arc::clone(&seen) };
        let fiber = expect_ok(
            kernel.spawn(kernel.root_context(), plugin, "bar=foo".to_owned()).await,
            "object fixture should spawn",
        );
        assert_eq!(kernel.state(fiber), jinnd_api::FiberState::Active);
        assert_eq!(*seen.lock().unwrap_or_else(|poison| poison.into_inner()), vec!["bar=foo"]);
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/plugin.spec.ts`, test `apply invalid plugin`; translated to the dynamic R3 lane.
    invalid_dynamic_plugin_contract_is_rejected_at_boundary,
    origin: "packages/core/tests/plugin.spec.ts",
    test: "apply invalid plugin (dynamic contract equivalent)",
    setup: ["construct dynamic manifests missing entrypoint or contract metadata"],
    actions: ["request spawn for each invalid manifest"],
    expected: ["every invalid manifest is rejected before a fiber is registered"],
    body: |_case| {
        const EMPTY_COMPONENT: &[u8] = &[
            0, 97, 115, 109, 13, 0, 1, 0, 1, 8, 0, 97, 115, 109, 1, 0, 0, 0, 2, 4, 1, 0,
            0, 0, 0, 47, 9, 112, 114, 111, 100, 117, 99, 101, 114, 115, 1, 12, 112, 114,
            111, 99, 101, 115, 115, 101, 100, 45, 98, 121, 1, 13, 119, 105, 116, 45, 99,
            111, 109, 112, 111, 110, 101, 110, 116, 7, 48, 46, 50, 51, 51, 46, 48,
        ];
        let kernel = jinnd_adapter::kernel();
        let invalid = [
            (
                "missing-contract",
                WasmArtifact {
                    bytes: EMPTY_COMPONENT.to_vec(),
                    expected_hash:
                        "2b6794829bd9876746a6ddb4b314fca30d215b33d71f6940b89c845dc1a040e5"
                            .to_owned(),
                },
            ),
            (
                "missing-entrypoint",
                WasmArtifact {
                    bytes: vec![0x00, 0x61],
                    expected_hash:
                        "022a6979e6dab7aa5ae4c3e5e45f7e977112a7e63593820dbec1ec738a24f93c"
                            .to_owned(),
                },
            ),
        ];
        for (name, artifact) in invalid {
            let error = match kernel.register_wasm_package(name, artifact, Vec::new()) {
                Ok(_) => panic!("an invalid dynamic plugin contract must be refused"),
                Err(error) => error,
            };
            assert_eq!(error.code, ErrorCode::InvalidProfile);
            assert_eq!(kernel.entry_fiber(&EntryId(name.to_owned())), None);
        }
        let refusals = expect_ok(
            kernel.ledger_events(LedgerQuery::default()).await,
            "artifact refusals should be readable",
        )
        .into_iter()
        .filter(|record| matches!(record.kind, LedgerEventKind::ArtifactRefused { .. }))
        .count();
        assert_eq!(refusals, 2, "both invalid contracts must be ledgered");
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/plugin.spec.ts`, test `inactive context`.
    inactive_context_rejects_new_plugins_effects_and_listeners,
    origin: "packages/core/tests/plugin.spec.ts",
    test: "inactive context",
    setup: ["activate a plugin and retain its activation context"],
    actions: ["dispose it", "attempt spawn, effect registration, and listener registration from retained context"],
    expected: ["all three operations return InactiveContext", "no child activates"]
}

spec_case! {
    /// TS origin: `packages/core/tests/plugin.spec.ts`, test `context inspect`.
    context_diagnostics_report_stable_plugin_identity,
    origin: "packages/core/tests/plugin.spec.ts",
    test: "context inspect",
    setup: ["spawn root, named function, named object, and named type plugins"],
    actions: ["inspect each activation context"],
    expected: ["root is reported as root", "each named plugin reports its declared stable name"]
}

spec_case! {
    /// TS origin: `packages/core/tests/plugin.spec.ts`, test `ctx.registry`.
    registry_iteration_exposes_each_live_fiber_once,
    origin: "packages/core/tests/plugin.spec.ts",
    test: "ctx.registry",
    setup: ["register multiple fibers"],
    actions: ["iterate keys, values, entries, and callback traversal"],
    expected: ["all views contain the same live fiber identities exactly once"]
}

spec_case! {
    /// TS origin: `packages/core/tests/plugin.spec.ts`, test `nested plugins`.
    parent_disposal_cascades_through_nested_plugin_effects,
    origin: "packages/core/tests/plugin.spec.ts",
    test: "nested plugins",
    setup: ["root listener plus three nested plugin listeners"],
    actions: ["emit", "dispose outer plugin twice", "emit after each disposal"],
    expected: ["first emit reaches four listeners", "cascade removes all three child fibers", "later emits reach only root", "repeat disposal is a no-op"]
}

spec_case! {
    /// TS origin: `packages/core/tests/plugin.spec.ts`, test `compare snapshot`.
    nested_plugin_removal_restores_hook_and_registry_snapshot,
    origin: "packages/core/tests/plugin.spec.ts",
    test: "compare snapshot",
    setup: ["capture root observation", "activate three nested listener plugins"],
    actions: ["remove outer registration", "reactivate same tree"],
    expected: ["removal equals pre-activation observation", "reactivation equals first active observation"]
}

spec_case! {
    /// TS origin: `packages/core/tests/plugin.spec.ts`, test `root dispose`.
    root_disposal_is_idempotent_and_cascades_to_children,
    origin: "packages/core/tests/plugin.spec.ts",
    test: "root dispose",
    setup: ["root owns one child fiber with one disposer"],
    actions: ["dispose root twice"],
    expected: ["root identity remains reserved", "child becomes disposed", "child disposer runs once", "root effect list is empty"]
}

spec_case! {
    /// TS origin: `packages/core/tests/plugin.spec.ts`, test `Service.init`.
    initialization_returned_undo_runs_on_disposal,
    origin: "packages/core/tests/plugin.spec.ts",
    test: "Service.init",
    setup: ["plugin initialization returns one undo"],
    actions: ["activate", "dispose"],
    expected: ["initialization runs once before active", "undo runs once during disposal"],
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        let initialized = Arc::new(AtomicUsize::new(0));
        let undone = Arc::new(AtomicUsize::new(0));
        let fiber = expect_ok(
            kernel.spawn(
                kernel.root_context(),
                InitializingPlugin {
                    initialized: Arc::clone(&initialized),
                    undone: Arc::clone(&undone),
                },
                (),
            ).await,
            "initializing fixture should spawn",
        );
        assert_eq!(initialized.load(Ordering::SeqCst), 1);
        assert_eq!(undone.load(Ordering::SeqCst), 0);
        expect_ok(kernel.dispose(fiber).await, "fixture should dispose");
        expect_ok(kernel.dispose(fiber).await, "repeat disposal should be inert");
        assert_eq!(undone.load(Ordering::SeqCst), 1);
    }
}
