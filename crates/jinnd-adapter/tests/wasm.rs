//! The wasm lane through the kernel facade (M1-P8): a profile entry naming a
//! wasm artifact activates the fixture behind the broker, mixed profiles
//! drive native and wasm entries together, the harness peer and the guest
//! cross ONE choke point recorded in ONE kernel ledger, and Mode-1 swap
//! replaces instances without restarting fibers.

#[path = "../../jinnd-wasm/tests/support/mod.rs"]
mod support;

use std::sync::Arc;

use jinnd_adapter::kernel;
use jinnd_api::{
    EntryId, ErrorCode, FiberState, Kernel, KernelError, LedgerEventKind, LedgerQuery, PluginRef,
    Profile, ProfileEntry, ServiceContract, SwapPhaseKind, WasmArtifact, WasmLane,
};

const PACKAGE: &str = "jinn.test/counter-plugin";
const COUNTER: &str = "jinn:test/counter";

fn artifact() -> WasmArtifact {
    let (bytes, expected_hash) = support::pinned_fixture();
    WasmArtifact {
        bytes,
        expected_hash,
    }
}

fn wasm_entry(id: &str, config: &str) -> ProfileEntry<String> {
    ProfileEntry {
        id: EntryId(id.to_owned()),
        plugin: PluginRef {
            package: PACKAGE.to_owned(),
            version: "0.0.1".to_owned(),
            artifact_hash: artifact().expected_hash,
        },
        config: config.to_owned(),
        disabled: false,
        parent: None,
        isolation: Vec::new(),
    }
}

struct Beacon;

impl ServiceContract for Beacon {
    type Observation = ();
    const NAME: &'static str = "jinn:test/beacon";
    fn observe(&self) {}
}

#[tokio::test]
async fn a_mixed_profile_drives_native_and_wasm_entries_through_one_kernel() {
    let k = kernel();
    k.register_wasm_package(PACKAGE, artifact(), vec![COUNTER.to_owned()])
        .unwrap_or_else(|error| panic!("register: {error:?}"));
    k.register_provider_package::<String, Beacon, _>("jinn.test/beacon", |_| Ok(Arc::new(Beacon)))
        .unwrap_or_else(|error| panic!("register beacon: {error:?}"));

    let mut native = wasm_entry("native-1", String::new().as_str());
    native.plugin.package = "jinn.test/beacon".to_owned();
    let report = k
        .reconcile(Profile {
            entries: vec![wasm_entry("wasm-1", "provider"), native],
        })
        .await
        .unwrap_or_else(|error| panic!("reconcile: {error:?}"));
    assert_eq!(report.errors, Vec::new());
    assert_eq!(report.created.len(), 2, "both lanes load from one document");

    let fiber = k
        .entry_fiber(&EntryId("wasm-1".to_owned()))
        .unwrap_or_else(|| panic!("the wasm entry has a fiber"));
    assert_eq!(k.state(fiber), FiberState::Active);

    // The harness peer calls the guest provider over the SAME broker.
    k.broker_grant(COUNTER);
    let handle = k
        .broker_resolve(COUNTER)
        .unwrap_or_else(|error| panic!("resolve: {error:?}"));
    let answer = k
        .broker_call(handle, "add", 5u64.to_le_bytes().to_vec())
        .await
        .unwrap_or_else(|error| panic!("call: {error:?}"));
    assert_eq!(answer, 5u64.to_le_bytes().to_vec());

    // Disposal withdraws exactly the wasm entry's contribution (I1).
    let report = k
        .reconcile(Profile::<String> {
            entries: Vec::new(),
        })
        .await
        .unwrap_or_else(|error| panic!("dispose reconcile: {error:?}"));
    assert_eq!(report.disposed.len(), 2);
    assert_eq!(k.state(fiber), FiberState::Disposed);
    let refused = k.broker_call(handle, "get", Vec::new()).await;
    assert_eq!(
        refused.err().map(|error: KernelError| error.code),
        Some(ErrorCode::MissingDependency),
        "the provider is gone with its fiber"
    );

    // One ledger tells the whole story (Law 2, R6) — through the facade.
    let kinds: Vec<LedgerEventKind> = k
        .ledger_events(LedgerQuery::default())
        .await
        .unwrap_or_else(|error| panic!("ledger: {error:?}"))
        .into_iter()
        .map(|record| record.kind)
        .collect();
    let has = |wanted: &LedgerEventKind| kinds.iter().any(|kind| kind == wanted);
    assert!(
        kinds
            .iter()
            .any(|kind| matches!(kind, LedgerEventKind::ArtifactLoaded { .. }))
    );
    assert!(has(&LedgerEventKind::ServiceProvided {
        service: COUNTER.to_owned()
    }));
    assert!(has(&LedgerEventKind::ContractResolved {
        contract: COUNTER.to_owned()
    }));
    assert!(has(&LedgerEventKind::ContractCall {
        contract: COUNTER.to_owned(),
        operation: "add".to_owned()
    }));
    assert!(has(&LedgerEventKind::ServiceWithdrawn {
        service: COUNTER.to_owned()
    }));
}

#[tokio::test]
async fn an_entry_grantless_package_is_refused_at_the_broker_not_trusted() {
    let k = kernel();
    k.register_wasm_package(PACKAGE, artifact(), Vec::new())
        .unwrap_or_else(|error| panic!("register: {error:?}"));
    let report = k
        .reconcile(Profile {
            entries: vec![wasm_entry("wasm-1", "ungranted")],
        })
        .await
        .unwrap_or_else(|error| panic!("reconcile: {error:?}"));
    assert_eq!(report.errors, Vec::new(), "the guest observed the refusal");
    let kinds: Vec<LedgerEventKind> = k
        .ledger_events(LedgerQuery::default())
        .await
        .unwrap_or_else(|error| panic!("ledger: {error:?}"))
        .into_iter()
        .map(|record| record.kind)
        .collect();
    assert!(
        kinds.iter().any(|kind| matches!(
            kind,
            LedgerEventKind::GrantRefused { contract, .. } if contract == "jinn:test/secret"
        )),
        "every denial is a ledger event (constitution 01)"
    );
}

#[tokio::test]
async fn a_wrong_hash_registration_is_refused_and_recorded_through_the_facade() {
    let k = kernel();
    let mut pinned = artifact();
    pinned.expected_hash = "deadbeef".to_owned();
    let refused = k.register_wasm_package(PACKAGE, pinned, Vec::new());
    assert_eq!(
        refused.err().map(|error| error.code),
        Some(ErrorCode::InvalidProfile)
    );
    let kinds: Vec<LedgerEventKind> = k
        .ledger_events(LedgerQuery::default())
        .await
        .unwrap_or_else(|error| panic!("ledger: {error:?}"))
        .into_iter()
        .map(|record| record.kind)
        .collect();
    assert!(
        kinds
            .iter()
            .any(|kind| matches!(kind, LedgerEventKind::ArtifactRefused { .. }))
    );
}

fn provider_artifact() -> WasmArtifact {
    let (bytes, expected_hash) = support::pinned_provider_fixture();
    WasmArtifact {
        bytes,
        expected_hash,
    }
}

/// The round-2 blocker-4 pin: the STAGED activation's outcome is committed
/// at swap commit — its provision goes live, and disposal withdraws exactly
/// the swapped instance's contribution, cleanly (undo tokens belong to the
/// instance that created them; the swap-target guest refuses foreign
/// tokens, so a mispaired replay would end the fiber Failed, not Disposed).
#[tokio::test]
async fn mode1_swap_commits_the_staged_contribution_and_disposes_it_exactly() {
    let k = kernel();
    k.register_wasm_package(PACKAGE, artifact(), vec![COUNTER.to_owned()])
        .unwrap_or_else(|error| panic!("register: {error:?}"));
    let report = k
        .reconcile(Profile {
            entries: vec![wasm_entry("wasm-1", "plain")],
        })
        .await
        .unwrap_or_else(|error| panic!("reconcile: {error:?}"));
    assert_eq!(report.errors, Vec::new());
    let fiber = k
        .entry_fiber(&EntryId("wasm-1".to_owned()))
        .unwrap_or_else(|| panic!("wasm-1 has a fiber"));

    // Before the swap, `plain` provides nothing: the contract is missing.
    k.broker_grant(COUNTER);
    let before = k
        .broker_resolve(COUNTER)
        .unwrap_or_else(|error| panic!("resolve: {error:?}"));
    assert_eq!(
        k.broker_call(before, "get", Vec::new())
            .await
            .err()
            .map(|error| error.code),
        Some(ErrorCode::MissingDependency)
    );

    // Swap to an artifact whose activation DOES provide the counter.
    let outcome = k
        .swap_wasm_artifact(&artifact().expected_hash, provider_artifact())
        .await
        .unwrap_or_else(|error| panic!("swap: {error:?}"));
    assert!(!outcome.rolled_back);
    assert_eq!(outcome.swapped, vec![EntryId("wasm-1".to_owned())]);

    // The staged outcome was COMMITTED: the provision is live and answers.
    let handle = k
        .broker_resolve(COUNTER)
        .unwrap_or_else(|error| panic!("resolve after swap: {error:?}"));
    let answer = k
        .broker_call(handle, "get", Vec::new())
        .await
        .unwrap_or_else(|error| panic!("the committed provision must answer: {error:?}"));
    assert_eq!(answer, 0u64.to_le_bytes().to_vec(), "state handed off");

    // Disposal withdraws exactly the swapped instance's contribution —
    // cleanly: the seat replays the NEW instance's own tokens (I1, R5).
    let report = k
        .reconcile(Profile::<String> {
            entries: Vec::new(),
        })
        .await
        .unwrap_or_else(|error| panic!("dispose reconcile: {error:?}"));
    assert_eq!(report.disposed.len(), 1);
    assert_eq!(
        k.state(fiber),
        FiberState::Disposed,
        "a mispaired undo token would have ended the fiber Failed"
    );
    let kinds: Vec<LedgerEventKind> = k
        .ledger_events(LedgerQuery::default())
        .await
        .unwrap_or_else(|error| panic!("ledger: {error:?}"))
        .into_iter()
        .map(|record| record.kind)
        .collect();
    assert!(kinds.contains(&LedgerEventKind::ServiceProvided {
        service: COUNTER.to_owned()
    }));
    assert!(kinds.contains(&LedgerEventKind::ServiceWithdrawn {
        service: COUNTER.to_owned()
    }));
    let after = k
        .broker_resolve(COUNTER)
        .unwrap_or_else(|error| panic!("resolve after dispose: {error:?}"));
    assert_eq!(
        k.broker_call(after, "get", Vec::new())
            .await
            .err()
            .map(|error| error.code),
        Some(ErrorCode::MissingDependency),
        "no trace of the swapped instance remains (I1)"
    );
}

/// Round-2 blocker-3 pin (R5/I1; COO round-3 ruling): a failed swap
/// discards EVERY staged instance — the failing one and the already-healthy
/// ones — by REPLAYING its staged effects in reverse, never by a raw
/// dispose. The grumpy-undo guest makes the replay observable: its inverse
/// fails loudly, and that contained failure must surface as a ledger
/// record (R6). A raw dispose leaves no such record.
#[tokio::test]
async fn a_failed_swap_replays_staged_effects_on_every_discarded_instance() {
    let k = kernel();
    k.register_wasm_package(PACKAGE, artifact(), Vec::new())
        .unwrap_or_else(|error| panic!("register: {error:?}"));
    let report = k
        .reconcile(Profile {
            entries: vec![
                wasm_entry("wasm-1", "grumpy-undo"),
                wasm_entry("wasm-2", "flaky-restore"),
            ],
        })
        .await
        .unwrap_or_else(|error| panic!("reconcile: {error:?}"));
    assert_eq!(report.errors, Vec::new());
    let fiber_one = k
        .entry_fiber(&EntryId("wasm-1".to_owned()))
        .unwrap_or_else(|| panic!("wasm-1 has a fiber"));
    let fiber_two = k
        .entry_fiber(&EntryId("wasm-2".to_owned()))
        .unwrap_or_else(|| panic!("wasm-2 has a fiber"));

    // wasm-1's staged instance activates healthy; wasm-2's staged instance
    // then refuses the handoff — the whole batch must roll back.
    let outcome = k
        .swap_wasm_artifact(&artifact().expected_hash, artifact())
        .await
        .unwrap_or_else(|error| panic!("swap: {error:?}"));
    assert!(outcome.rolled_back, "the flaky handoff fails the batch");
    assert_eq!(outcome.swapped, Vec::<EntryId>::new(), "zero commits");

    // Old instances stay warm; no fiber moved (R8).
    assert_eq!(k.state(fiber_one), FiberState::Active);
    assert_eq!(k.state(fiber_two), FiberState::Active);

    // The replay proof: discarding wasm-1's healthy staged instance ran its
    // staged inverse, whose loud failure is a recorded, contained error.
    let kinds: Vec<LedgerEventKind> = k
        .ledger_events(LedgerQuery::default())
        .await
        .unwrap_or_else(|error| panic!("ledger: {error:?}"))
        .into_iter()
        .map(|record| record.kind)
        .collect();
    assert!(
        kinds.iter().any(|kind| matches!(
            kind,
            LedgerEventKind::ErrorRecorded { error } if error.message.contains("grumpy undo ran")
        )),
        "the discard must REPLAY staged effects (a raw dispose never runs the inverse): {kinds:?}"
    );
}

#[tokio::test]
async fn mode1_swap_replaces_both_instances_of_one_artifact_without_restarting_fibers() {
    let k = kernel();
    k.register_wasm_package(PACKAGE, artifact(), Vec::new())
        .unwrap_or_else(|error| panic!("register: {error:?}"));
    let report = k
        .reconcile(Profile {
            entries: vec![wasm_entry("wasm-1", "plain"), wasm_entry("wasm-2", "plain")],
        })
        .await
        .unwrap_or_else(|error| panic!("reconcile: {error:?}"));
    assert_eq!(report.errors, Vec::new());
    let fiber_one = k
        .entry_fiber(&EntryId("wasm-1".to_owned()))
        .unwrap_or_else(|| panic!("wasm-1 has a fiber"));
    let fiber_two = k
        .entry_fiber(&EntryId("wasm-2".to_owned()))
        .unwrap_or_else(|| panic!("wasm-2 has a fiber"));
    let transitions_before = (
        k.transitions(fiber_one).len(),
        k.transitions(fiber_two).len(),
    );

    let outcome = k
        .swap_wasm_artifact(&artifact().expected_hash, artifact())
        .await
        .unwrap_or_else(|error| panic!("swap: {error:?}"));
    assert!(!outcome.rolled_back);
    assert_eq!(
        outcome.swapped,
        vec![EntryId("wasm-1".to_owned()), EntryId("wasm-2".to_owned())],
        "the batch is every entry sharing the artifact hash"
    );

    // Mode 1 is an instance swap, never a fiber reload (R8).
    assert_eq!(k.state(fiber_one), FiberState::Active);
    assert_eq!(k.state(fiber_two), FiberState::Active);
    assert_eq!(
        (
            k.transitions(fiber_one).len(),
            k.transitions(fiber_two).len()
        ),
        transitions_before,
        "no fiber moved"
    );

    let phases: Vec<SwapPhaseKind> = k
        .ledger_events(LedgerQuery::default())
        .await
        .unwrap_or_else(|error| panic!("ledger: {error:?}"))
        .into_iter()
        .filter_map(|record| match record.kind {
            LedgerEventKind::SwapPhase { phase, .. } => Some(phase),
            _ => None,
        })
        .collect();
    assert_eq!(
        phases,
        vec![
            SwapPhaseKind::Began,
            SwapPhaseKind::InstanceHealthy,
            SwapPhaseKind::InstanceHealthy,
            SwapPhaseKind::Committed
        ],
        "every phase is a ledger event"
    );
}
