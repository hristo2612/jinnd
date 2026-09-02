//! R12 pin: a guest-visible semantic change to the plugin world ships
//! with its version (M2-K4 round-2 ruling: the suspend/dispose
//! lifecycle is contract, so the world is 0.4.0 and the bundles under
//! `contracts/` mirror the classification).
//!
//! Every contract of record is read through `jinnd_contract_lens`
//! (M2-K16): declarations come out of the toolchain's own parser, metadata
//! out of real TOML tables, and a statement made in prose is asserted
//! against COMMENT LINES ONLY. No test here holds the text, so the
//! `contains` that six shipped instances passed on cannot be written.

use jinnd_contract_lens::{bundle, world};

#[test]
fn world_is_versioned_for_suspend_semantics() {
    // 0.10.0 (M2-K15): `net-error` gains `untrusted`;
    // 0.9.0 (M2-K14): `net.request` is provided and reshaped;
    // 0.8.0 (M2-K13): the kernel becomes a publisher on this world's bus;
    // 0.7.0 (M2-K10) gave `kernel-error` the typed `cycle` refusal, and
    // 0.6.0 (M2-K9) the typed `restarting` one.
    let world = world();
    assert_eq!(world.wit().package_id(), "jinn:plugin@0.10.0");
    assert!(world.prose().states("Suspend ≠ dispose"));
}

/// M2-K9 (R3/R12): the reply-expecting dispatch refusal is a CASE of the
/// world's own error carrying a RECORD, never prose a guest greps — and
/// `emit` states which modes it decides, so the contract, not the
/// implementation, is where a guest author learns the rule.
#[test]
fn the_world_carries_the_typed_dispatch_refusals() {
    let world = world();
    let wit = world.wit();
    let types = wit.interface("types");
    let cases = types.variant_cases("kernel-error");
    for case in [
        "restarting(refused-target)",
        "gone(refused-target)",
        "suspended(refused-target)",
        "stalled(refused-target)",
    ] {
        assert!(cases.iter().any(|c| c == case), "{case} in {cases:?}");
    }
    assert_eq!(
        types.record_fields("refused-target"),
        ["entry: string", "incarnation: u64", "topic: string"]
    );
    assert!(world.prose().states("REPLY-EXPECTING modes"));
    // The ask that replaces discovering a pending transition by stalling,
    // in the SAME vocabulary the refusal uses.
    let introspect = bundle("jinn-introspect").wit().wit();
    assert_eq!(introspect.package_id(), "jinn:introspect@0.4.0");
    let composition = introspect.interface("composition");
    assert_eq!(
        composition.enum_cases("unserved"),
        ["restarting", "gone", "suspended", "stalled"]
    );
    let entry = composition.record_fields("entry");
    assert!(
        entry
            .iter()
            .any(|field| field == "unserved: option<unserved>"),
        "{entry:?}"
    );
}

/// M2-K10 (R3/R12): the wait-cycle refusal is a CASE of the world's own
/// error carrying a RECORD naming both ends, and the live wait behind it
/// is readable through `jinn:introspect` — one vocabulary, additively
/// versioned, so a guest author learns the rule from the contract.
#[test]
fn the_world_carries_the_typed_wait_cycle() {
    let world = world();
    let wit = world.wit();
    let types = wit.interface("types");
    let cases = types.variant_cases("kernel-error");
    assert!(cases.iter().any(|c| c == "cycle(wait-cycle)"), "{cases:?}");
    // The record BODY, parsed (M2-K14 round-2 sweep): a substring for
    // `on: string,` was satisfied by the unrelated `operation: string,` in
    // two other signatures, so it would have passed with this field renamed
    // or deleted — the same unfirable shape as the outbound assertion below.
    assert_eq!(
        types.record_fields("wait-cycle"),
        [
            "on: string",
            "waiter: string",
            "target: string",
            "through: list<string>"
        ]
    );
    // Every mode is decided, `emit` included: the kernel awaits every
    // delivery end to end, so fire-and-forget is not an escape.
    assert!(world.prose().states("refused in EVERY mode"));
    let introspect = bundle("jinn-introspect").wit().wit();
    let composition = introspect.interface("composition");
    assert_eq!(
        composition.signature("waits"),
        "waits: func() -> list<wait>"
    );
    assert_eq!(
        composition.record_fields("wait"),
        [
            "waiter: u64",
            "waiter-entry: option<string>",
            "target: u64",
            "target-entry: option<string>",
            "on: string"
        ]
    );
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
/// its own wire, verbatim, and carries the bundle's four operations —
/// each compared as a PARSED signature, bundle against world.
#[test]
fn keystore_import_mirrors_its_bundle() {
    let bundle = bundle("jinn-keystore").wit().wit();
    let world = world().wit();
    let (secrets, keystore) = (bundle.interface("secrets"), world.interface("keystore"));
    assert_eq!(
        secrets.variant_cases("keystore-error"),
        [
            "not-found",
            "denied(string)",
            "failed(string)",
            "invalid(string)"
        ]
    );
    assert_eq!(
        keystore.variant_cases("keystore-error"),
        secrets.variant_cases("keystore-error"),
        "the world carries keystore-error verbatim"
    );
    for operation in ["get", "put", "delete", "list"] {
        assert_eq!(
            keystore.signature(operation),
            secrets.signature(operation),
            "{operation}"
        );
    }
}

/// The bundles declare their lifecycle class as a KEY in a real table,
/// not as a word somewhere in a comment: durable world mutations are
/// retained across suspend/incarnations, kernel registrations are released
/// on suspend and re-armed on activate.
#[test]
fn bundles_mirror_lifecycle_classification() {
    let fs = bundle("jinn-fs").metadata().metadata();
    assert_eq!(
        fs.string_at("retention.lifecycle").as_deref(),
        Some("entry-scoped-survives-suspend-withdrawn-on-dispose")
    );
    let clock = bundle("jinn-clock").metadata().metadata();
    assert_eq!(
        clock.string_at("notes.lifecycle").as_deref(),
        Some("kernel-registration-released-on-suspend-rearmed-on-activate")
    );
}

/// M2-K6 round 4 (R3; the world mirrors its bundles): the `process` and
/// `net` imports answer the bundle-declared errors on their own wire —
/// `output-truncated` is a variant a guest matches, never a string.
#[test]
fn process_and_net_imports_return_their_bundles_errors() {
    let world = world().wit();
    for (name, iface, error) in [
        ("jinn-process", "process", "process-error"),
        ("jinn-net", "net", "net-error"),
    ] {
        let declared = bundle(name)
            .wit()
            .wit()
            .interface(iface)
            .variant_cases(error);
        assert!(declared.iter().any(|c| c == "not-found"), "{declared:?}");
        assert_eq!(
            world.interface(iface).variant_cases(error),
            declared,
            "the world carries {error} verbatim"
        );
    }
    let process = world.interface("process");
    assert_eq!(
        process.signature("run"),
        "run: func(command: string, args: list<string>) -> result<list<u8>, process-error>"
    );
    let cases = process.variant_cases("process-error");
    assert!(cases.iter().any(|c| c == "output-truncated"), "{cases:?}");
}

/// M2-K14 (R12, Law 3): the world and the bundle ship BOTH outbound
/// one-shots with their version, their shapes, and their irreversibility —
/// a guest author learns from the contract, never from the implementation.
///
/// Every function is asserted by its PARSED signature, because a
/// `contains("request: func(...)")` is satisfied by `send-request:
/// func(...)` as a plain substring — it would pass whichever name the
/// operation carried, and would not have noticed round 1 REPLACING the
/// 0.1.0 declaration instead of adding beside it. That is the vacuity class
/// this packet exists to prevent; it is not allowed in the test that guards
/// the packet.
#[test]
fn the_world_and_the_bundle_declare_both_outbound_one_shots() {
    let net_bundle = bundle("jinn-net");
    let net = net_bundle.wit().wit();
    let world = world().wit();
    // BOTH documents PARSED, and every declaration below asserted in each:
    // the bundle is the contract of record, the world carries it verbatim
    // (R12), and a drift between the two is exactly what has to fail.
    assert_eq!(net.package_id(), "jinn:net@0.3.0");
    for (document, wit) in [("the bundle", &net), ("the world", &world)] {
        let net = wit.interface("net");
        assert_eq!(
            net.record_fields("outbound-request"),
            [
                "method: string",
                "url: string",
                "headers: list<header>",
                "body: list<u8>"
            ],
            "{document}"
        );
        assert_eq!(
            net.record_fields("outbound-response"),
            ["status: u16", "headers: list<header>", "body: list<u8>"],
            "{document}"
        );
        // R12: the 0.1.0 declaration, PRESERVED — same name, same
        // parameters in the same order, same result. Round 1 of M2-K14
        // REPLACED it instead of adding beside it, and the substring that
        // was supposed to notice could not: `contains("request: func(")`
        // is satisfied by `send-request` alone.
        assert_eq!(
            net.signature("request"),
            "request: func(method: string, url: string, body: list<u8>) \
             -> result<list<u8>, net-error>",
            "{document}"
        );
        // 0.2.0: the whole-response edition, added BESIDE it.
        assert_eq!(
            net.signature("send-request"),
            "send-request: func(req: outbound-request) -> result<outbound-response, net-error>",
            "{document}"
        );
        // 0.3.0 (M2-K15): ONE case ADDED to `net-error`. The whole case
        // list in declaration order, each with its payload — so a case
        // gone missing, renamed, reordered, or re-typed fails here.
        assert_eq!(
            net.variant_cases("net-error"),
            [
                "not-found",
                "denied(string)",
                "failed(string)",
                "invalid(string)",
                "untrusted(string)"
            ],
            "{document}"
        );
    }
    // Law 3 admits exactly two categories, and BOTH doors are DECLARED —
    // with no inverse and no compensator to mistake for one. Read as real
    // keys in real tables: a legacy door whose effect class went undeclared
    // would be a Law 3 violation.
    let meta = net_bundle.metadata().metadata();
    for operation in ["request", "send-request"] {
        assert_eq!(
            meta.string_at(&format!("operations.{operation}.effect"))
                .as_deref(),
            Some("irreversible"),
            "{operation}"
        );
        assert!(
            !meta.has_key(&format!("operations.{operation}.inverse")),
            "{operation} declares no inverse"
        );
    }
    // The durable row's shape is stated where a guest author reads it, and
    // it LEADS with the effect id — the field that makes the irreversible
    // class survive a reopen (round-2 verifier Minor). Compared against the
    // facade's OWN field list; gate 2 sweeps every mention, this pins that
    // both documents state this one at all.
    let facade = jinnd_contract_lens::facade::ledger_event_kinds();
    let row = facade
        .get("NetRequested")
        .unwrap_or_else(|| panic!("the facade writes NetRequested"));
    assert_eq!(row.first().map(String::as_str), Some("effect"));
    let readme = net_bundle
        .readme()
        .unwrap_or_else(|| panic!("the net bundle ships a README"));
    for source in [net_bundle.metadata(), readme] {
        let stated: Vec<_> = source
            .rows()
            .into_iter()
            .filter(|mention| mention.kind == "NetRequested")
            .collect();
        assert!(!stated.is_empty(), "{} states the row shape", source.path());
        for mention in stated {
            assert_eq!(&mention.fields, row, "{}:{}", source.path(), mention.line);
        }
    }
}

/// M2-K13 (R3/R12/Law 2): the kernel's lifecycle PUBLISH is declared where
/// a plugin author reads it — the reserved topic and its two guest-visible
/// rules in the world, the delivered record and its three settled
/// semantics (ordering, loss, replay) in the bundle.
#[test]
fn the_world_and_the_bundle_declare_the_lifecycle_publish() {
    let world = world();
    assert!(
        world
            .prose()
            .states("KERNEL-RESERVED topic is REFUSED here in every mode")
    );
    assert!(world.prose().states("subscribed under `jinn:introspect`"));
    let introspect = bundle("jinn-introspect").wit();
    let transition = introspect
        .wit()
        .interface("composition")
        .record_fields("transition");
    assert_eq!(
        transition,
        [
            "entry: string",
            "fiber: u64",
            "incarnation: option<u64>",
            "from: string",
            "to: string",
            "ordinal: u64",
            "committed-by: u64"
        ]
    );
    // The three questions the packet had to settle, settled IN the
    // contract rather than in an implementation note.
    for stated in [
        "ORDERING AGAINST THE LEDGER",
        "BACK-PRESSURE",
        "LATE JOIN AND REPLAY",
        "AUTHORITY",
    ] {
        assert!(introspect.prose().states(stated), "{stated}");
    }
    // The authority demonstration's one failure, stated on the wire: the
    // cause is not delivered, so the grant is not widened.
    assert!(
        introspect
            .prose()
            .states("`cause` for a transition is DELIBERATELY ABSENT")
    );
    assert!(!transition.iter().any(|field| field.starts_with("cause:")));
}
