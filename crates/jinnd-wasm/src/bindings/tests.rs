//! R12 pin: a guest-visible semantic change to the plugin world ships
//! with its version (M2-K4 round-2 ruling: the suspend/dispose
//! lifecycle is contract, so the world is 0.4.0 and the bundles under
//! `contracts/` mirror the classification).

const WORLD: &str = include_str!("../../../../wit/plugin.wit");
const FS_META: &str = include_str!("../../../../contracts/jinn-fs/metadata.toml");
const CLOCK_META: &str = include_str!("../../../../contracts/jinn-clock/metadata.toml");

#[test]
fn world_is_versioned_for_suspend_semantics() {
    // 0.7.0 (M2-K10): `kernel-error` gains the typed `cycle` refusal;
    // 0.6.0 (M2-K9) gave it the typed `restarting` refusal.
    assert!(WORLD.contains("package jinn:plugin@0.7.0;"));
    assert!(WORLD.contains("Suspend ≠ dispose"));
}

/// M2-K9 (R3/R12): the reply-expecting dispatch refusal is a CASE of the
/// world's own error carrying a RECORD, never prose a guest greps — and
/// `emit` states which modes it decides, so the contract, not the
/// implementation, is where a guest author learns the rule.
#[test]
fn the_world_carries_the_typed_dispatch_refusals() {
    const INTROSPECT: &str = include_str!("../../../../contracts/jinn-introspect/contract.wit");
    for case in [
        "restarting(refused-target),",
        "gone(refused-target),",
        "suspended(refused-target),",
    ] {
        assert!(WORLD.contains(case), "{case}");
    }
    assert!(WORLD.contains("record refused-target {"));
    assert!(WORLD.contains("incarnation: u64,"));
    assert!(WORLD.contains("REPLY-EXPECTING modes"));
    // The ask that replaces discovering a pending transition by stalling,
    // in the SAME vocabulary the refusal uses.
    assert!(INTROSPECT.contains("package jinn:introspect@0.3.0;"));
    assert!(INTROSPECT.contains("enum unserved {"));
    assert!(INTROSPECT.contains("unserved: option<unserved>,"));
}

/// M2-K10 (R3/R12): the wait-cycle refusal is a CASE of the world's own
/// error carrying a RECORD naming both ends, and the live wait behind it
/// is readable through `jinn:introspect` — one vocabulary, additively
/// versioned, so a guest author learns the rule from the contract.
#[test]
fn the_world_carries_the_typed_wait_cycle() {
    const INTROSPECT: &str = include_str!("../../../../contracts/jinn-introspect/contract.wit");
    assert!(WORLD.contains("cycle(wait-cycle),"));
    assert!(WORLD.contains("record wait-cycle {"));
    for field in ["on: string,", "waiter: string,", "target: string,"] {
        assert!(WORLD.contains(field), "{field}");
    }
    // Every mode is decided, `emit` included: the kernel awaits every
    // delivery end to end, so fire-and-forget is not an escape.
    assert!(WORLD.contains("refused in EVERY mode"));
    assert!(INTROSPECT.contains("waits: func() -> list<wait>;"));
    assert!(INTROSPECT.contains("record wait {"));
}

/// M2-K10: the typed cycle reaches the wire naming BOTH ENDS and the
/// waits between them. A caller reads identity off the record; the prose
/// lane exists only for the bundles whose error type cannot carry one.
#[test]
fn the_wait_cycle_names_both_ends_on_the_wire() {
    use jinnd_api::{EntryId, FiberId};

    use super::{types, wire_cycle};
    use crate::waits::{Cycle, WaitEdge};

    let cycle = Cycle {
        waiter: FiberId(2),
        waiter_entry: Some(EntryId("owner".to_owned())),
        target: FiberId(1),
        target_entry: Some(EntryId("provider".to_owned())),
        on: "jinn:test/settings.get".to_owned(),
        through: vec![WaitEdge {
            waiter: FiberId(1),
            target: FiberId(2),
            on: "jinn:test/notice".to_owned(),
        }],
    };
    match wire_cycle(&cycle) {
        types::KernelError::Cycle(wire) => {
            assert_eq!(wire.on, "jinn:test/settings.get");
            assert_eq!(wire.waiter, "owner");
            assert_eq!(wire.target, "provider");
            assert_eq!(wire.through, vec!["jinn:test/notice".to_owned()]);
        }
        other => panic!("{other:?}"),
    }
}

/// M2-K9: the four dispositions map to four DIFFERENT wire cases,
/// because they are four different next moves for the caller.
/// Folding a disposal into `restarting` would tell a caller to wait for a
/// replacement that is never coming — the defect this mapping exists to
/// prevent — so the mapping is pinned per disposition, not in aggregate.
#[test]
fn each_disposition_maps_to_its_own_wire_case() {
    use jinnd_api::{EntryId, Owed};

    use super::{types, wire_refusal};
    use crate::topics::Unserved;

    let unserved = |owed| Unserved {
        entry: EntryId("consumer".to_owned()),
        incarnation: 7,
        owed,
    };
    let expected = |wire: &types::RefusedTarget| {
        wire.entry == "consumer" && wire.incarnation == 7 && wire.topic == "t"
    };
    match wire_refusal("t", &unserved(Owed::Reload)) {
        types::KernelError::Restarting(target) => assert!(expected(&target), "{target:?}"),
        other => panic!("a reload is `restarting`: {other:?}"),
    }
    match wire_refusal("t", &unserved(Owed::Disposal)) {
        types::KernelError::Gone(target) => assert!(expected(&target), "{target:?}"),
        other => panic!("a disposal is terminal — never `restarting`: {other:?}"),
    }
    match wire_refusal("t", &unserved(Owed::Suspension)) {
        types::KernelError::Suspended(target) => assert!(expected(&target), "{target:?}"),
        other => panic!("a suspension awaits a resume, not a restart: {other:?}"),
    }
    match wire_refusal("t", &unserved(Owed::Stalled)) {
        types::KernelError::Stalled(target) => assert!(expected(&target), "{target:?}"),
        other => panic!("a stall promises nothing — never `restarting`: {other:?}"),
    }
}

/// M2-K8 (R3/R12): the `keystore` import answers its bundle's error on
/// its own wire, verbatim, and carries the bundle's four operations.
#[test]
fn keystore_import_mirrors_its_bundle() {
    const KEYSTORE: &str = include_str!("../../../../contracts/jinn-keystore/contract.wit");
    let declared = KEYSTORE
        .lines()
        .find(|line| line.trim_start().starts_with("variant keystore-error"))
        .unwrap_or_else(|| panic!("keystore-error declared in the bundle"))
        .trim();
    assert!(
        WORLD.contains(declared),
        "the world carries {declared} verbatim"
    );
    for operation in ["get:", "put:", "delete:", "%list:"] {
        assert!(KEYSTORE.contains(operation) && WORLD.contains(operation));
    }
}

#[test]
fn bundles_mirror_lifecycle_classification() {
    // Durable world mutations: retained across suspend/incarnations.
    assert!(FS_META.contains("suspend"));
    assert!(FS_META.contains("dispose"));
    // Kernel registrations: released on suspend, re-armed on activate.
    assert!(CLOCK_META.contains("suspend"));
}

/// M2-K6 round 4 (R3; the world mirrors its bundles): the `process` and
/// `net` imports answer the bundle-declared errors on their own wire —
/// `output-truncated` is a variant a guest matches, never a string.
#[test]
fn process_and_net_imports_return_their_bundles_errors() {
    const PROCESS: &str = include_str!("../../../../contracts/jinn-process/contract.wit");
    const NET: &str = include_str!("../../../../contracts/jinn-net/contract.wit");
    for (bundle, error) in [(PROCESS, "process-error"), (NET, "net-error")] {
        let declared = bundle
            .lines()
            .find(|line| line.trim_start().starts_with(&format!("variant {error}")))
            .unwrap_or_else(|| panic!("{error} declared in the bundle"))
            .trim();
        assert!(declared.contains("not-found"), "{declared}");
        assert!(
            WORLD.contains(declared),
            "the world carries {declared} verbatim"
        );
        assert!(WORLD.contains(&format!("result<list<u8>, {error}>")));
    }
    assert!(WORLD.contains("output-truncated }"));
}
