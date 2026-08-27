mod support;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use jinnd_api::{
    EntryId, FiberId, FiberState, Kernel, LedgerEventKind, LedgerQuery, PluginRef, Profile,
    ProfileEntry, SwapReport, WasmArtifact, WasmLane,
};
use support::{expect_ok, spec_case};

const CLOCK_PACKAGE: &str = "demo/clock";
const CLOCK_CONTRACT: &str = "demo:clock";
const GREETER_PACKAGE: &str = "demo/greeter";
const GREETING_CONTRACT: &str = "demo:greeting";

struct Kit {
    clock_v1: WasmArtifact,
    clock_v2: WasmArtifact,
    greeter: WasmArtifact,
}

fn read_artifact(dir: &Path, name: &str) -> WasmArtifact {
    let path = dir.join(format!("{name}.wasm"));
    let hash_path = dir.join(format!("{name}.wasm.sha256"));
    WasmArtifact {
        bytes: std::fs::read(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display())),
        expected_hash: std::fs::read_to_string(&hash_path)
            .unwrap_or_else(|error| panic!("read {}: {error}", hash_path.display())),
    }
}

fn run_builder(arguments: &[&str]) {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let status = Command::new("cargo")
        .args(["run", "--quiet", "--manifest-path"])
        .arg(root.join("demo/builder/Cargo.toml"))
        .arg("--")
        .args(arguments)
        .status()
        .unwrap_or_else(|error| panic!("start demo builder: {error}"));
    assert!(status.success(), "the checked-in demo fixtures must build");
}

fn kit() -> &'static Kit {
    static KIT: OnceLock<Kit> = OnceLock::new();
    KIT.get_or_init(|| {
        let root =
            std::env::temp_dir().join(format!("jinnd-invariant-handles-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        run_builder(&["kit", root.to_string_lossy().as_ref()]);
        let artifacts = root.join("artifacts");
        let clock_v1 = read_artifact(&artifacts, "clock");
        let greeter = read_artifact(&artifacts, "greeter");
        run_builder(&["clock", "v2", artifacts.to_string_lossy().as_ref()]);
        let clock_v2 = read_artifact(&artifacts, "clock");
        std::fs::remove_dir_all(&root)
            .unwrap_or_else(|error| panic!("remove fixture output: {error}"));
        Kit {
            clock_v1,
            clock_v2,
            greeter,
        }
    })
}

fn entry(id: &str, package: &str, artifact: &WasmArtifact, config: &str) -> ProfileEntry<String> {
    ProfileEntry {
        id: EntryId(id.to_owned()),
        plugin: PluginRef {
            package: package.to_owned(),
            version: "0.0.1".to_owned(),
            artifact_hash: artifact.expected_hash.clone(),
        },
        config: config.to_owned(),
        disabled: false,
        parent: None,
        isolation: Vec::new(),
    }
}

async fn demo_kernel() -> impl Kernel + WasmLane {
    let kernel = jinnd_adapter::kernel();
    expect_ok(
        kernel.register_wasm_package(
            CLOCK_PACKAGE,
            kit().clock_v1.clone(),
            vec![CLOCK_CONTRACT.to_owned()],
        ),
        "clock package should register",
    );
    expect_ok(
        kernel.register_wasm_package(
            GREETER_PACKAGE,
            kit().greeter.clone(),
            vec![
                GREETING_CONTRACT.to_owned(),
                CLOCK_CONTRACT.to_owned(),
                "demo:announce".to_owned(),
            ],
        ),
        "greeter package should register",
    );
    let report = expect_ok(
        kernel
            .reconcile(Profile {
                entries: vec![
                    entry("clock", CLOCK_PACKAGE, &kit().clock_v1, ""),
                    entry("greeter", GREETER_PACKAGE, &kit().greeter, "base"),
                ],
            })
            .await,
        "demo profile should reconcile",
    );
    assert!(report.errors.is_empty(), "unexpected faults: {report:?}");
    expect_ok(
        kernel.wait_for_quiescence().await,
        "demo profile should quiesce",
    );
    kernel
}

fn fiber(kernel: &impl Kernel, id: &str) -> FiberId {
    kernel
        .entry_fiber(&EntryId(id.to_owned()))
        .unwrap_or_else(|| panic!("{id} should have a fiber"))
}

async fn records(kernel: &impl Kernel) -> Vec<jinnd_api::LedgerRecord> {
    expect_ok(
        kernel.ledger_events(LedgerQuery::default()).await,
        "ledger should be readable",
    )
}

async fn call(
    kernel: &(impl Kernel + WasmLane),
    contract: &str,
    operation: &str,
    payload: &[u8],
) -> Vec<u8> {
    kernel.broker_grant(contract);
    let handle = expect_ok(
        kernel.broker_resolve(contract),
        "granted contract should resolve",
    );
    expect_ok(
        kernel
            .broker_call(handle, operation, payload.to_vec())
            .await,
        "brokered call should succeed",
    )
}

spec_case! {
    /// TS origin: `packages/core/tests/shadow.spec.ts`, test `keeps caller metadata separate from the service shadow`; R4 handle equivalent.
    nested_service_handles_keep_caller_and_provider_scopes_distinct,
    origin: "packages/core/tests/shadow.spec.ts",
    test: "keeps caller metadata separate from the service shadow (R4 handle equivalent)",
    setup: ["outer service resolves inner through its activation snapshot"],
    actions: ["root caller invokes outer then outer invokes inner"],
    expected: ["inner call is charged to outer activation", "outer call stays charged to root"],
    body: |_case| {
        let kernel = demo_kernel().await;
        let greeting = call(&kernel, GREETING_CONTRACT, "greet", b"nested").await;
        assert!(String::from_utf8_lossy(&greeting).contains("nested"));
        let greeter = fiber(&kernel, "greeter");
        let calls: Vec<_> = records(&kernel)
            .await
            .into_iter()
            .filter(|record| matches!(record.kind, LedgerEventKind::ContractCall { .. }))
            .collect();
        assert!(calls.iter().any(|record| {
            record.fiber == Some(jinnd_adapter::KERNEL_SCOPE)
                && matches!(&record.kind, LedgerEventKind::ContractCall { contract, .. } if contract == GREETING_CONTRACT)
        }));
        assert!(calls.iter().any(|record| {
            record.fiber == Some(greeter)
                && matches!(&record.kind, LedgerEventKind::ContractCall { contract, .. } if contract == CLOCK_CONTRACT)
        }));
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/shadow.spec.ts`, test `exposes the caller without preserving shadow for noShadow services`; R4 handle equivalent.
    plain_capability_handle_carries_caller_without_proxy_state,
    origin: "packages/core/tests/shadow.spec.ts",
    test: "exposes the caller without preserving shadow for noShadow services (R4 handle equivalent)",
    setup: ["provide a plain typed capability and resolve it inside outer service"],
    actions: ["invoke the opaque broker handle"],
    expected: ["ledger attribution carries the caller", "no proxy or shadow state is exposed"],
    body: |_case| {
        let kernel = demo_kernel().await;
        let answer = call(&kernel, CLOCK_CONTRACT, "version", &[]).await;
        assert_eq!(answer, 1u64.to_le_bytes());
        assert!(records(&kernel).await.iter().any(|record| {
            record.fiber == Some(jinnd_adapter::KERNEL_SCOPE)
                && matches!(&record.kind, LedgerEventKind::ContractCall { contract, operation } if contract == CLOCK_CONTRACT && operation == "version")
        }));
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/shadow.spec.ts`, test `exposes the caller to callable services`; R4 plain-method equivalent.
    method_service_handle_exposes_explicit_caller_scope,
    origin: "packages/core/tests/shadow.spec.ts",
    test: "exposes the caller to callable services (R4 plain-method equivalent)",
    setup: ["resolve a method contract through the capability broker"],
    actions: ["invoke the method through its caller-owned handle"],
    expected: ["the contract call is attributed to the explicit caller scope"],
    body: |_case| {
        let kernel = demo_kernel().await;
        kernel.broker_grant(CLOCK_CONTRACT);
        let handle = expect_ok(kernel.broker_resolve(CLOCK_CONTRACT), "clock should resolve");
        expect_ok(kernel.broker_call(handle, "now", Vec::new()).await, "now should answer");
        assert!(records(&kernel).await.into_iter().any(|record| {
            record.fiber == Some(jinnd_adapter::KERNEL_SCOPE)
                && matches!(&record.kind, LedgerEventKind::ContractCall { contract, operation } if contract == CLOCK_CONTRACT && operation == "now")
        }));
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/shadow.spec.ts`, test `strips service shadow before creating plugins`; R4 handle equivalent.
    service_spawn_uses_explicit_caller_context_without_proxy_leak,
    origin: "packages/core/tests/shadow.spec.ts",
    test: "strips service shadow before creating plugins (R4 handle equivalent)",
    setup: ["loader activates provider and consumer behind the broker"],
    actions: ["invoke consumer from root and let it call its provider"],
    expected: ["both fibers stay active", "nested call crosses the broker without proxy metadata or error"],
    body: |_case| {
        let kernel = demo_kernel().await;
        let greeting = call(&kernel, GREETING_CONTRACT, "greet", b"spawned").await;
        assert!(String::from_utf8_lossy(&greeting).contains("spawned"));
        assert_eq!(kernel.state(fiber(&kernel, "clock")), FiberState::Active);
        assert_eq!(kernel.state(fiber(&kernel, "greeter")), FiberState::Active);
        assert!(!records(&kernel)
            .await
            .iter()
            .any(|record| matches!(record.kind, LedgerEventKind::ErrorRecorded { .. })));
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/invoke.spec.ts`, test `functional service`; translated to broker call precedence per R4.
    service_method_merges_base_intercept_extension_and_call_config,
    origin: "packages/core/tests/invoke.spec.ts",
    test: "functional service (R4 broker-call equivalent)",
    setup: ["greeter activation has base config base", "one caller-owned handle is retained"],
    actions: ["call without payload", "call with a per-call name payload"],
    expected: ["base config is used when no call override exists", "per-call payload right-biases base config without changing caller scope"],
    body: |_case| {
        let kernel = demo_kernel().await;
        kernel.broker_grant(GREETING_CONTRACT);
        let handle = expect_ok(kernel.broker_resolve(GREETING_CONTRACT), "greeter should resolve");
        let base = expect_ok(kernel.broker_call(handle, "greet", Vec::new()).await, "base greet");
        let call = expect_ok(kernel.broker_call(handle, "greet", b"call".to_vec()).await, "call override");
        assert!(String::from_utf8_lossy(&base).contains("hello, base"));
        assert!(String::from_utf8_lossy(&call).contains("hello, call"));
        let attributed = records(&kernel)
            .await
            .into_iter()
            .filter(|record| {
                record.fiber == Some(jinnd_adapter::KERNEL_SCOPE)
                    && matches!(&record.kind, LedgerEventKind::ContractCall { contract, .. } if contract == GREETING_CONTRACT)
            })
            .count();
        assert_eq!(attributed, 2);
    }
}

spec_case! {
    /// TS origin: `packages/core/tests/invoke.spec.ts`, test `uses the service shadow for callable extensions`; translated to generation-pinned handles per R4/R9.
    extended_service_handle_keeps_dependency_snapshot,
    origin: "packages/core/tests/invoke.spec.ts",
    test: "uses the service shadow for callable extensions (R4 generation-pinned handle equivalent)",
    setup: ["resolve clock generation one through a caller-owned handle"],
    actions: ["hot-swap the provider artifact", "call the retained handle then resolve a fresh handle"],
    expected: ["retained handle keeps its broker-owned dependency slot", "retained and fresh handles both observe generation two"],
    body: |_case| {
        let kernel = demo_kernel().await;
        kernel.broker_grant(CLOCK_CONTRACT);
        let retained = expect_ok(kernel.broker_resolve(CLOCK_CONTRACT), "clock v1 should resolve");
        let before = expect_ok(kernel.broker_call(retained, "version", Vec::new()).await, "v1 call");
        assert_eq!(before, 1u64.to_le_bytes());
        let SwapReport { rolled_back, .. } = expect_ok(
            kernel
                .swap_wasm_artifact(&kit().clock_v1.expected_hash, kit().clock_v2.clone())
                .await,
            "healthy clock swap should commit",
        );
        assert!(!rolled_back);
        let retained_after = expect_ok(
            kernel.broker_call(retained, "version", Vec::new()).await,
            "the retained handle should keep the handed-off slot",
        );
        assert_eq!(retained_after, 2u64.to_le_bytes());
        let fresh = expect_ok(kernel.broker_resolve(CLOCK_CONTRACT), "clock v2 should resolve");
        let after = expect_ok(kernel.broker_call(fresh, "version", Vec::new()).await, "v2 call");
        assert_eq!(after, 2u64.to_le_bytes());
    }
}
