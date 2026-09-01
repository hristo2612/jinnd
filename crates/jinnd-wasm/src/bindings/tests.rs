//! R12 pin: a guest-visible semantic change to the plugin world ships
//! with its version (M2-K4 round-2 ruling: the suspend/dispose
//! lifecycle is contract, so the world is 0.4.0 and the bundles under
//! `contracts/` mirror the classification).

const WORLD: &str = include_str!("../../../../wit/plugin.wit");
const FS_META: &str = include_str!("../../../../contracts/jinn-fs/metadata.toml");
const CLOCK_META: &str = include_str!("../../../../contracts/jinn-clock/metadata.toml");

// ---------------------------------------------------------------------------
// Contract text is asserted by PARSING (M2-K15 round-2 ruling), never by
// substring. `WORLD.contains("package jinn:plugin@0.10.0;")` is satisfied by
// those bytes ANYWHERE — a doc comment above a declaration that says
// something else satisfies it — and no substring can see a variant case's
// PAYLOAD or a function's parameter TYPES at all. `wit_parser::Resolve` is
// the toolchain's own parser: it observes the DECLARATION.
// ---------------------------------------------------------------------------

/// Parse a standalone WIT document, answering the resolved graph and the
/// package it declares. A document the toolchain cannot read fails here,
/// naming the file — that is a broken contract of record whatever bytes it
/// happens to contain.
fn parse_wit(file: &str, text: &str) -> (wit_parser::Resolve, wit_parser::PackageId) {
    let mut resolve = wit_parser::Resolve::default();
    let package = resolve
        .push_str(file, text)
        .unwrap_or_else(|err| panic!("{file} parses as WIT: {err:#}"));
    (resolve, package)
}

/// The declared package identity, `namespace:name@version`, as the parser
/// read it rather than as the file spells it.
fn package_id(resolve: &wit_parser::Resolve, package: wit_parser::PackageId) -> String {
    resolve.packages[package].name.to_string()
}

/// One named interface out of a parsed package.
fn interface<'a>(
    resolve: &'a wit_parser::Resolve,
    package: wit_parser::PackageId,
    name: &str,
) -> &'a wit_parser::Interface {
    let id = *resolve.packages[package]
        .interfaces
        .get(name)
        .unwrap_or_else(|| panic!("the package declares interface {name}"));
    &resolve.interfaces[id]
}

/// A type as WIT spells it, for the forms these contracts use. An
/// unanticipated form PANICS rather than rendering to something comparable:
/// a shape nobody expected is a finding, not a pass.
fn render(resolve: &wit_parser::Resolve, ty: wit_parser::Type) -> String {
    use wit_parser::{Type, TypeDefKind};
    match ty {
        Type::Bool => "bool".into(),
        Type::U8 => "u8".into(),
        Type::U16 => "u16".into(),
        Type::U32 => "u32".into(),
        Type::U64 => "u64".into(),
        Type::S8 => "s8".into(),
        Type::S16 => "s16".into(),
        Type::S32 => "s32".into(),
        Type::S64 => "s64".into(),
        Type::F32 => "f32".into(),
        Type::F64 => "f64".into(),
        Type::Char => "char".into(),
        Type::String => "string".into(),
        Type::ErrorContext => "error-context".into(),
        Type::Id(id) => {
            let def = &resolve.types[id];
            if let Some(name) = &def.name {
                return name.clone();
            }
            match &def.kind {
                TypeDefKind::List(inner) => format!("list<{}>", render(resolve, *inner)),
                TypeDefKind::Option(inner) => format!("option<{}>", render(resolve, *inner)),
                TypeDefKind::Tuple(tuple) => {
                    let parts: Vec<_> = tuple.types.iter().map(|t| render(resolve, *t)).collect();
                    format!("tuple<{}>", parts.join(", "))
                }
                TypeDefKind::Result(result) => match (result.ok, result.err) {
                    (Some(ok), Some(err)) => {
                        format!("result<{}, {}>", render(resolve, ok), render(resolve, err))
                    }
                    (Some(ok), None) => format!("result<{}>", render(resolve, ok)),
                    (None, Some(err)) => format!("result<_, {}>", render(resolve, err)),
                    (None, None) => "result".into(),
                },
                other => panic!("unrendered WIT type form: {other:?}"),
            }
        }
    }
}

/// A function as `name: func(param: type, ...) -> result`, from the PARSED
/// signature — so a renamed parameter, a reordered one, or a changed type
/// all fail here, none of which a substring on the source line can see.
fn signature(resolve: &wit_parser::Resolve, iface: &wit_parser::Interface, name: &str) -> String {
    let func = iface
        .functions
        .get(name)
        .unwrap_or_else(|| panic!("the interface declares {name}"));
    let params: Vec<_> = func
        .params
        .iter()
        .map(|(param, ty)| format!("{param}: {}", render(resolve, *ty)))
        .collect();
    let result = func
        .result
        .map(|ty| format!(" -> {}", render(resolve, ty)))
        .unwrap_or_default();
    format!("{}: func({}){result}", func.name, params.join(", "))
}

/// A variant's cases as `name` / `name(payload)`, in declaration order.
fn variant_cases(
    resolve: &wit_parser::Resolve,
    iface: &wit_parser::Interface,
    name: &str,
) -> Vec<String> {
    let id = *iface
        .types
        .get(name)
        .unwrap_or_else(|| panic!("the interface declares {name}"));
    match &resolve.types[id].kind {
        wit_parser::TypeDefKind::Variant(variant) => variant
            .cases
            .iter()
            .map(|case| match case.ty {
                Some(ty) => format!("{}({})", case.name, render(resolve, ty)),
                None => case.name.clone(),
            })
            .collect(),
        other => panic!("{name} is a variant, not {other:?}"),
    }
}

/// A record's fields as `name: type`, in declaration order.
fn record_fields(
    resolve: &wit_parser::Resolve,
    iface: &wit_parser::Interface,
    name: &str,
) -> Vec<String> {
    let id = *iface
        .types
        .get(name)
        .unwrap_or_else(|| panic!("the interface declares {name}"));
    match &resolve.types[id].kind {
        wit_parser::TypeDefKind::Record(record) => record
            .fields
            .iter()
            .map(|field| format!("{}: {}", field.name, render(resolve, field.ty)))
            .collect(),
        other => panic!("{name} is a record, not {other:?}"),
    }
}

#[test]
fn world_is_versioned_for_suspend_semantics() {
    // 0.10.0 (M2-K15): `net-error` gains `untrusted`;
    // 0.9.0 (M2-K14): `net.request` is provided and reshaped;
    // 0.8.0 (M2-K13): the kernel becomes a publisher on this world's bus;
    // 0.7.0 (M2-K10) gave `kernel-error` the typed `cycle` refusal, and
    // 0.6.0 (M2-K9) the typed `restarting` one.
    let (resolve, package) = parse_wit("plugin.wit", WORLD);
    assert_eq!(package_id(&resolve, package), "jinn:plugin@0.10.0");
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
    assert!(INTROSPECT.contains("package jinn:introspect@0.4.0;"));
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
    // Scoped to the record BODY, not the whole world (M2-K14 round-2
    // sweep). `WORLD.contains("on: string,")` was satisfied by the
    // unrelated `operation: string,` in two other signatures, so it would
    // have passed with this field renamed or deleted — the same unfirable
    // shape as the outbound-declaration assertion below.
    let record = WORLD
        .split("record wait-cycle {")
        .nth(1)
        .and_then(|rest| rest.split('}').next())
        .unwrap_or_else(|| panic!("the world declares record wait-cycle"));
    for field in ["on: string,", "waiter: string,", "target: string,"] {
        assert!(record.contains(field), "wait-cycle declares {field}");
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
    }
    assert!(WORLD.contains("result<list<u8>, process-error>"));
    assert!(WORLD.contains("output-truncated }"));
}

/// M2-K14 (R12, Law 3): the world and the bundle ship BOTH outbound
/// one-shots with their version, their shapes, and their irreversibility —
/// a guest author learns from the contract, never from the implementation.
///
/// Every function assertion is anchored on the line start (`\n  `), because
/// an unanchored `contains("request: func(...)")` is satisfied by
/// `send-request: func(...)` as a plain substring — it would pass whichever
/// name the operation carried, and would not have noticed round 1
/// REPLACING the 0.1.0 declaration instead of adding beside it. That is the
/// vacuity class this packet exists to prevent; it is not allowed in the
/// test that guards the packet.
#[test]
fn the_world_and_the_bundle_declare_both_outbound_one_shots() {
    const NET: &str = include_str!("../../../../contracts/jinn-net/contract.wit");
    const META: &str = include_str!("../../../../contracts/jinn-net/metadata.toml");
    // BOTH documents PARSED, and every declaration below asserted in each:
    // the bundle is the contract of record, the world carries it verbatim
    // (R12), and a drift between the two is exactly what has to fail.
    let (net_resolve, net_package) = parse_wit("jinn-net.wit", NET);
    let (world_resolve, world_package) = parse_wit("plugin.wit", WORLD);
    assert_eq!(package_id(&net_resolve, net_package), "jinn:net@0.3.0");
    for (document, resolve, package) in [
        ("the bundle", &net_resolve, net_package),
        ("the world", &world_resolve, world_package),
    ] {
        let net = interface(resolve, package, "net");
        assert_eq!(
            record_fields(resolve, net, "outbound-request"),
            [
                "method: string",
                "url: string",
                "headers: list<header>",
                "body: list<u8>"
            ],
            "{document}"
        );
        assert_eq!(
            record_fields(resolve, net, "outbound-response"),
            ["status: u16", "headers: list<header>", "body: list<u8>"],
            "{document}"
        );
        // R12: the 0.1.0 declaration, PRESERVED — same name, same
        // parameters in the same order, same result. Round 1 of M2-K14
        // REPLACED it instead of adding beside it, and the substring that
        // was supposed to notice could not: `contains("request: func(")`
        // is satisfied by `send-request` alone.
        assert_eq!(
            signature(resolve, net, "request"),
            "request: func(method: string, url: string, body: list<u8>) \
             -> result<list<u8>, net-error>",
            "{document}"
        );
        // 0.2.0: the whole-response edition, added BESIDE it.
        assert_eq!(
            signature(resolve, net, "send-request"),
            "send-request: func(req: outbound-request) -> result<outbound-response, net-error>",
            "{document}"
        );
        // 0.3.0 (M2-K15): ONE case ADDED to `net-error`. The whole case
        // list in declaration order, each with its payload — so a case
        // gone missing, renamed, reordered, or re-typed fails here. No
        // substring observes a payload at all, and `contains("untrusted")`
        // is satisfied by the prose above the declaration.
        assert_eq!(
            variant_cases(resolve, net, "net-error"),
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
    // with no inverse and no compensator to mistake for one. A legacy door
    // whose effect class went undeclared would be a Law 3 violation.
    for (operation, next) in [
        ("[operations.request]", "[operations.send-request]"),
        ("[operations.send-request]", "[operations.listen]"),
    ] {
        let block = META
            .split(operation)
            .nth(1)
            .and_then(|rest| rest.split(next).next())
            .unwrap_or_else(|| panic!("the bundle declares {operation}"));
        assert!(
            block.contains(r#"effect      = "irreversible""#),
            "{operation}: {block}"
        );
        assert!(
            !block.lines().any(|line| line.starts_with("inverse")),
            "{operation} declares no inverse: {block}"
        );
    }
    // The durable row's shape is stated where a guest author reads it, and
    // it LEADS with the effect id — the field that makes the irreversible
    // class survive a reopen (round-2 verifier Minor).
    for source in [
        META,
        include_str!("../../../../contracts/jinn-net/README.md"),
    ] {
        assert!(
            source.contains("NetRequested { effect, method, host, path, status, request_bytes,"),
            "the bundle states the row shape the facade actually writes"
        );
    }
}

/// M2-K13 (R3/R12/Law 2): the kernel's lifecycle PUBLISH is declared where
/// a plugin author reads it — the reserved topic and its two guest-visible
/// rules in the world, the delivered record and its three settled
/// semantics (ordering, loss, replay) in the bundle.
#[test]
fn the_world_and_the_bundle_declare_the_lifecycle_publish() {
    const INTROSPECT: &str = include_str!("../../../../contracts/jinn-introspect/contract.wit");
    assert!(WORLD.contains("KERNEL-RESERVED topic is REFUSED here in every mode"));
    assert!(WORLD.contains("subscribed under `jinn:introspect`"));
    assert!(INTROSPECT.contains("record transition {"));
    for field in [
        "incarnation: option<u64>,",
        "ordinal: u64,",
        "committed-by: u64,",
    ] {
        assert!(INTROSPECT.contains(field), "{field}");
    }
    // The three questions the packet had to settle, settled IN the
    // contract rather than in an implementation note.
    for stated in [
        "ORDERING AGAINST THE LEDGER",
        "BACK-PRESSURE",
        "LATE JOIN AND REPLAY",
        "AUTHORITY",
    ] {
        assert!(INTROSPECT.contains(stated), "{stated}");
    }
    // The authority demonstration's one failure, stated on the wire: the
    // cause is not delivered, so the grant is not widened.
    assert!(INTROSPECT.contains("`cause` for a transition is DELIBERATELY ABSENT"));
    assert!(!INTROSPECT.contains("cause: string,"));
}
