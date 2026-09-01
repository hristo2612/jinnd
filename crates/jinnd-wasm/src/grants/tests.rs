//! Admission pins (M2-K2 round 3, M2-K3 round 2, M2-K6): fail-closed on
//! every wrong-typed, malformed, or undeclared scope; bare grants admit;
//! the process/net policies parse exactly their declared shapes.

use super::{
    EnvPolicy, Grant, GrantScope, NetScope, ProcessScope, ScopeValue, admission, authority,
};

fn only(grant: Grant) -> Result<(), String> {
    let (admitted, refusals) = admission(&[grant]);
    match (admitted.len(), refusals.first()) {
        (1, None) => Ok(()),
        (0, Some(refusal)) => Err(refusal.message.clone()),
        (admitted, _) => panic!("one grant, one verdict; admitted {admitted}"),
    }
}

fn scoped(contract: &str, scope: ScopeValue) -> Grant {
    Grant {
        contract: contract.to_owned(),
        scope: Some(scope),
        ops: None,
    }
}

fn attenuated(contract: &str, ops: ScopeValue) -> Grant {
    Grant {
        contract: contract.to_owned(),
        scope: None,
        ops: Some(ops),
    }
}

fn names(items: &[&str]) -> ScopeValue {
    ScopeValue::List(items.iter().map(|item| text(item)).collect())
}

fn refusal_of(verdict: Result<(), String>) -> String {
    match verdict {
        Err(message) => message,
        Ok(()) => panic!("the scope must refuse the grant"),
    }
}

fn text(value: &str) -> ScopeValue {
    ScopeValue::Path(value.to_owned())
}

fn map(fields: &[(&str, ScopeValue)]) -> ScopeValue {
    ScopeValue::Map(
        fields
            .iter()
            .map(|(key, value)| ((*key).to_owned(), value.clone()))
            .collect(),
    )
}

/// The verifier's round-2 probe, pinned (round-3 ruling): a numeric
/// scope on a contract whose declared scope type is `path-prefix` is
/// the wrong type — the grant refuses; it never widens into root-wide
/// authority.
#[test]
fn the_fs_scope_9_probe_refuses_as_wrong_type() {
    let message = refusal_of(only(scoped("jinn:fs", ScopeValue::Rate(9))));
    assert!(
        message.contains("jinn:fs") && message.contains("path-prefix"),
        "the refusal names the contract and its declared scope type: {message}"
    );
}

/// The mirror mismatch: a string scope on a `rate` contract refuses.
#[test]
fn a_string_scope_on_a_rate_contract_refuses_as_wrong_type() {
    let message = refusal_of(only(scoped("jinn:clock", text("fine"))));
    assert!(
        message.contains("jinn:clock") && message.contains("rate"),
        "the refusal names the contract and its declared scope type: {message}"
    );
}

/// A scope on a contract declaring NO scope type refuses.
#[test]
fn a_scope_on_a_contract_declaring_none_refuses() {
    let verdict = only(scoped("jinn:ledger", ScopeValue::Rate(5)));
    assert!(
        verdict.is_err(),
        "jinn:ledger declares no scope; a scoped grant cannot validate"
    );
}

/// A scope on a contract with no shipped bundle refuses — there is no
/// declared type to validate against (fail-closed, never benign-default).
#[test]
fn a_scope_on_an_undeclared_contract_refuses() {
    let verdict = only(scoped("jinn:test/counter", ScopeValue::Rate(5)));
    assert!(
        verdict.is_err(),
        "no declared scope type means no validation means no admission"
    );
}

/// M2-K3 round 2 (COO ruling): a well-typed `path-prefix` scope admits
/// — the fs provider enforces it per call on the resolved path — and
/// a traversing "scope" is no containment path: it refuses.
#[test]
fn a_well_typed_path_scope_admits_and_a_traversing_one_refuses() {
    let verdict = only(scoped("jinn:fs", text("/data")));
    assert!(
        verdict.is_ok(),
        "the path-prefix scope is enforceable: {verdict:?}"
    );
    let message = refusal_of(only(scoped("jinn:fs", text("../up"))));
    assert!(
        message.contains("containment"),
        "the refusal names the shape: {message}"
    );
}

/// A scope shape the decoder could not read refuses wherever it lands.
#[test]
fn a_malformed_scope_refuses_on_every_contract() {
    for contract in [
        "jinn:clock",
        "jinn:fs",
        "jinn:ledger",
        "jinn:process",
        "jinn:net",
        "demo:thing",
    ] {
        let verdict = only(scoped(contract, ScopeValue::Malformed("-5".into())));
        assert!(
            verdict.is_err(),
            "a malformed scope must refuse: {contract}"
        );
    }
}

/// The clock's rate floor admits.
#[test]
fn a_rate_scope_on_the_clock_admits() {
    let verdict = only(scoped("jinn:clock", ScopeValue::Rate(1000)));
    assert!(verdict.is_ok(), "the rate scope is v0.1's enforced scope");
}

/// Bare grants (no scope) admit for every contract — the contract's
/// root/default scope, exactly the pre-ruling semantics; for the process
/// and net bundles that default is the EMPTY policy (M2-K6 default deny).
#[test]
fn bare_grants_admit_unchanged_and_process_net_default_to_deny() {
    for contract in [
        "jinn:clock",
        "jinn:fs",
        "jinn:ledger",
        "jinn:test/counter",
        "jinn:process",
        "jinn:net",
        "jinn:profile",
        "jinn:introspect",
    ] {
        let grant = Grant {
            contract: contract.to_owned(),
            scope: None,
            ops: None,
        };
        assert!(
            only(grant.clone()).is_ok(),
            "a bare grant admits: {contract}"
        );
        match (contract, authority(&grant)) {
            ("jinn:process", GrantScope::Process(policy)) => {
                assert_eq!(policy, ProcessScope::default());
            }
            ("jinn:net", GrantScope::Net(policy)) => assert_eq!(policy, NetScope::default()),
            // M2-K7: a bare profile grant patches nothing (default deny).
            ("jinn:profile", GrantScope::Entries(ids)) => assert!(ids.is_empty()),
            ("jinn:process" | "jinn:net" | "jinn:profile", other) => {
                panic!("not a policy: {other:?}")
            }
            (_, scope) => assert_eq!(scope, GrantScope::Root),
        }
    }
}

/// M2-K7 (`jinn:profile`, harness #21): the `entry-ids` scope admits a
/// non-empty list of ids or `"*"` and refuses every other shape; the
/// authority admits exactly the named entries, `"*"` only when written.
#[test]
fn an_entry_ids_scope_parses_its_declared_shape_and_refuses_the_rest() {
    let named = scoped(
        "jinn:profile",
        ScopeValue::List(vec![text("scheduler"), text("status")]),
    );
    assert!(only(named.clone()).is_ok());
    let authority = authority(&named);
    assert!(authority.admits_entry("scheduler") && authority.admits_entry("status"));
    assert!(!authority.admits_entry("editor") && !authority.admits_entry("*"));
    let star = scoped("jinn:profile", ScopeValue::List(vec![text("*")]));
    assert!(super::authority(&star).admits_entry("anything"));
    assert!(
        !GrantScope::Root.admits_entry("scheduler"),
        "only an entry scope patches"
    );
    for (wrote, why) in [
        (text("scheduler"), "a bare string is not a list"),
        (ScopeValue::List(Vec::new()), "an empty list names nothing"),
        (
            ScopeValue::List(vec![ScopeValue::Rate(3)]),
            "an id is a string",
        ),
        (
            ScopeValue::List(vec![text("")]),
            "an empty id names nothing",
        ),
        (
            map(&[("ids", text("a"))]),
            "a map is not the declared shape",
        ),
    ] {
        let refusal = refusal_of(only(scoped("jinn:profile", wrote)));
        assert!(
            refusal.contains("grant refused: jinn:profile"),
            "{why}: {refusal}"
        );
    }
    let refusal = refusal_of(only(scoped(
        "jinn:introspect",
        ScopeValue::List(vec![text("x")]),
    )));
    assert!(refusal.contains("declares no scope type"), "{refusal}");
}

/// One judgment call, split verdicts: mixed lists partition exactly.
#[test]
fn admission_partitions_a_mixed_grant_list() {
    let (admitted, refusals) = admission(&[
        Grant {
            contract: "jinn:test/counter".to_owned(),
            scope: None,
            ops: None,
        },
        scoped("jinn:fs", ScopeValue::Rate(9)),
        scoped("jinn:clock", ScopeValue::Rate(1000)),
    ]);
    assert_eq!(
        admitted
            .iter()
            .map(|grant| grant.contract.as_str())
            .collect::<Vec<_>>(),
        vec!["jinn:test/counter", "jinn:clock"],
    );
    assert_eq!(refusals.len(), 1, "exactly the fs probe refused");
}

/// M2-K6 red-first: one refusal per scope-type mismatch for both
/// bundles — a path on `jinn:process`, a rate on `jinn:net` — each naming
/// the declared type; and a policy on the fs contract is the mirror.
#[test]
fn a_wrong_typed_scope_refuses_each_new_bundle() {
    let message = refusal_of(only(scoped("jinn:process", text("/bin"))));
    assert!(
        message.contains("jinn:process") && message.contains("process-policy"),
        "{message}"
    );
    let message = refusal_of(only(scoped("jinn:net", ScopeValue::Rate(9))));
    assert!(
        message.contains("jinn:net") && message.contains("net-policy"),
        "{message}"
    );
    let message = refusal_of(only(scoped(
        "jinn:fs",
        map(&[("exec", ScopeValue::List(vec![]))]),
    )));
    assert!(message.contains("path-prefix"), "{message}");
}

/// A well-typed process policy admits and becomes exactly its authority;
/// a relative exec prefix, an unknown field, and an unreadable env policy
/// each refuse.
#[test]
fn a_process_policy_parses_its_declared_shape_and_refuses_the_rest() {
    let grant = scoped(
        "jinn:process",
        map(&[
            (
                "exec",
                ScopeValue::List(vec![text("/bin/cat"), text("/usr/bin")]),
            ),
            ("env", ScopeValue::List(vec![text("PATH")])),
        ]),
    );
    assert!(only(grant.clone()).is_ok());
    assert_eq!(
        authority(&grant),
        GrantScope::Process(ProcessScope {
            exec: vec!["/bin/cat".into(), "/usr/bin".into()],
            env: EnvPolicy::Allow(vec!["PATH".into()]),
        })
    );
    let inherit_none = scoped("jinn:process", map(&[("env", text("inherit-none"))]));
    assert_eq!(
        authority(&inherit_none),
        GrantScope::Process(ProcessScope::default())
    );
    for (bad, needle) in [
        (
            map(&[("exec", ScopeValue::List(vec![text("bin/cat")]))]),
            "absolute",
        ),
        (
            map(&[("exec", ScopeValue::List(vec![text("/bin/../sh")]))]),
            "absolute",
        ),
        (map(&[("exec", text("/bin"))]), "list"),
        (map(&[("env", text("inherit-all"))]), "env policy"),
        (map(&[("cwd", text("/"))]), "no field"),
    ] {
        let message = refusal_of(only(scoped("jinn:process", bad)));
        assert!(message.contains(needle), "{message}");
    }
}

/// A well-typed net policy admits and becomes exactly its authority; an
/// inverted or oversized range, a non-list, and an unknown field refuse.
#[test]
fn a_net_policy_parses_its_declared_shape_and_refuses_the_rest() {
    let grant = scoped(
        "jinn:net",
        map(&[
            (
                "bind",
                ScopeValue::List(vec![ScopeValue::Rate(7800), ScopeValue::Rate(7899)]),
            ),
            ("outbound", ScopeValue::List(vec![text("example.invalid")])),
        ]),
    );
    assert!(only(grant.clone()).is_ok());
    assert_eq!(
        authority(&grant),
        GrantScope::Net(NetScope {
            bind: vec![(7800, 7899)],
            outbound: vec!["example.invalid".into()],
        })
    );
    for (bad, needle) in [
        (
            map(&[(
                "bind",
                ScopeValue::List(vec![ScopeValue::Rate(9), ScopeValue::Rate(8)]),
            )]),
            "range",
        ),
        (
            map(&[(
                "bind",
                ScopeValue::List(vec![ScopeValue::Rate(1), ScopeValue::Rate(70_000)]),
            )]),
            "range",
        ),
        (map(&[("bind", ScopeValue::Rate(80))]), "range"),
        (map(&[("outbound", text("host"))]), "list"),
        (map(&[("tls", ScopeValue::Rate(1))]), "no field"),
    ] {
        let message = refusal_of(only(scoped("jinn:net", bad)));
        assert!(message.contains(needle), "{message}");
    }
}

/// M2-K8 (harness #24): an operation-class attenuation names only
/// operations the contract bundle declares — read-only fs and keystore
/// grants admit; an unknown operation, a non-list, an empty list, and an
/// attenuation on a contract with no bundle each refuse (fail-closed).
#[test]
fn an_ops_attenuation_admits_declared_operations_only() {
    assert!(only(attenuated("jinn:fs", names(&["read", "list", "meta"]))).is_ok());
    assert!(only(attenuated("jinn:keystore", names(&["get", "list"]))).is_ok());
    assert!(only(attenuated("jinn:profile", names(&["entry", "document"]))).is_ok());
    let message = refusal_of(only(attenuated("jinn:fs", names(&["read", "format"]))));
    assert!(
        message.contains("jinn:fs") && message.contains("format"),
        "the refusal names the unknown operation: {message}"
    );
    assert!(only(attenuated("jinn:fs", text("read"))).is_err());
    assert!(only(attenuated("jinn:fs", names(&[]))).is_err());
    assert!(only(attenuated("jinn:test/counter", names(&["get"]))).is_err());
    assert_eq!(
        super::ops::attenuation(&attenuated("jinn:fs", names(&["read", "read"]))),
        Some(vec!["read".to_owned()]),
        "the admitted attenuation is the deduplicated operation set"
    );
    assert_eq!(
        super::ops::attenuation(&scoped("jinn:fs", text("/log"))),
        None
    );
}

/// M2-K14: an authority's normal form, and the allowlist match built on it
/// — EQUALITY, never a prefix, a suffix, or a host that swallows every
/// port (Law 1, the M2-K8 hull ruling read for hosts).
#[test]
fn an_outbound_entry_admits_its_own_authority_and_nothing_beside_it() {
    use super::policy::normalize_authority;
    for (written, normal) in [
        ("example.com", "example.com:80"),
        ("EXAMPLE.com:8080", "example.com:8080"),
        ("127.0.0.1:7799", "127.0.0.1:7799"),
        ("[::1]:7799", "[::1]:7799"),
        ("[::1]", "[::1]:80"),
    ] {
        assert_eq!(
            normalize_authority(written).map(|(normal, _, _)| normal),
            Some(normal.to_owned()),
            "{written}"
        );
    }
    // Not a readable authority: userinfo (a credential is refused, never
    // edited out), an empty host, a port that is not a u16, a path.
    for bad in ["", "user:pw@host", ":80", "host:notaport", "host/path"] {
        assert!(normalize_authority(bad).is_none(), "{bad}");
    }
    let scope = NetScope {
        bind: Vec::new(),
        outbound: vec!["example.com".to_owned(), "127.0.0.1:7799".to_owned()],
    };
    assert!(scope.admits_authority("example.com:80"));
    assert!(scope.admits_authority("127.0.0.1:7799"));
    for beside in [
        "example.com:443",
        "example.com:7799",
        "sub.example.com:80",
        "127.0.0.1:7800",
        "127.0.0.2:7799",
        "localhost:7799",
    ] {
        assert!(!scope.admits_authority(beside), "{beside}");
    }
    assert!(
        !NetScope::default().admits_authority("example.com:80"),
        "a bare grant reaches nothing"
    );
}

/// Grants of one contract COMPOSE their outbound allowlists (round-2
/// ruling 2): the union, never a widened wildcard.
#[test]
fn outbound_allowlists_compose_as_their_union() {
    let mut held = GrantScope::Net(NetScope {
        bind: Vec::new(),
        outbound: vec!["a.example:80".to_owned()],
    });
    held.union(GrantScope::Net(NetScope {
        bind: Vec::new(),
        outbound: vec!["b.example:80".to_owned()],
    }));
    let GrantScope::Net(scope) = &held else {
        panic!("net scope")
    };
    assert!(scope.admits_authority("a.example:80"));
    assert!(scope.admits_authority("b.example:80"));
    assert!(!scope.admits_authority("c.example:80"));
}
