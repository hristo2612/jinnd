//! Grant-scope admission — the ONE point where profile grants become broker
//! authority (M2-K2 round-3 ruling; Law 1, constitution 01 §Grants). The
//! judgment is FAIL-CLOSED: every scoped grant validates against its
//! contract bundle's declared scope type (contracts/*/metadata.toml
//! `[scope]`), and a scope that is malformed or of the wrong type REFUSES
//! the grant with a recorded per-entry error —
//! never dropped silently, never narrowed, and above all never widened into
//! an unscoped grant. Authority handling has no benign-default path (R9).

use jinnd_api::{ErrorCode, KernelError};

use crate::alarms::CLOCK_CONTRACT;
use crate::broker_state::refusal;
use crate::hostfs::FS_CONTRACT;

/// One scope value as the profile document wrote it (constitution 04
/// §Format): the decoder preserves the written shape — including one it
/// cannot read — so the admission judgment, not the decoder, refuses it on
/// the record (R3: typed, never a silent `filter_map`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScopeValue {
    /// A non-negative integer — the shape of the `rate` scope type.
    Rate(u64),
    /// A string — the shape of the `path-prefix` scope type.
    Path(String),
    /// Any other written shape, carried verbatim for the refusal record.
    Malformed(String),
}

impl std::fmt::Display for ScopeValue {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rate(floor) => write!(out, "{floor}"),
            Self::Path(path) => write!(out, "{path:?}"),
            Self::Malformed(wrote) => write!(out, "{wrote}"),
        }
    }
}

/// One granted contract with its optional scope (constitution 01 §Grants: a
/// grant is (identity, contract, version range, optional scope); 04 §Format:
/// a profile grant entry is a bare contract name or `{ contract, scope }`).
/// A bare grant holds the contract's root/default scope; a scoped grant
/// holds authority only if [`admission`] validates the scope against the
/// contract's declared scope type.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Grant {
    pub contract: String,
    pub scope: Option<ScopeValue>,
}

/// One entry's seat configuration: `grants` are the contracts the profile
/// side grants the instance (constitution 01: requests are not grants),
/// `faults` are entries the decoder could not read as grants at all —
/// carried so admission refuses them ON THE RECORD, never a silent drop
/// (round-3 ruling) — and `payload` is the opaque payload handed to the
/// guest's `activate` (R9: data, never behavior).
pub struct SeatSpec {
    pub grants: Vec<Grant>,
    pub faults: Vec<String>,
    pub payload: Vec<u8>,
}

/// The scope type a v0.1 contract bundle declares (`[scope]` in
/// contracts/*/metadata.toml; `scope-type = "none"` where operations declare
/// it inline). This table mirrors the shipped bundles verbatim — a contract
/// absent here declared nothing, and a scoped grant on it cannot validate.
enum Declared {
    /// `rate`: a minimum-period floor in milliseconds (jinn:clock).
    Rate,
    /// `path-prefix`: a containment path (jinn:fs).
    PathPrefix,
    /// The bundle declares no scope (jinn:ledger).
    NoScope,
    /// No shipped bundle declares a scope type for this contract.
    Undeclared,
}

fn declared(contract: &str) -> Declared {
    match contract {
        CLOCK_CONTRACT => Declared::Rate,
        FS_CONTRACT => Declared::PathPrefix,
        "jinn:ledger" => Declared::NoScope,
        _ => Declared::Undeclared,
    }
}

fn refused(message: String) -> KernelError {
    refusal(ErrorCode::EffectFailed, message)
}

/// THE fail-closed judgment on one grant (round-3 ruling). A bare grant
/// admits (the contract's root/default scope — unchanged v0.1 semantics).
/// A scoped grant admits only when its scope validates against the
/// contract's declared scope type — the clock's rate floor (M2-K2) and the
/// fs's path prefix (M2-K3 round 2, which RETIRED the K2-era "path scopes
/// are v0.1-unenforceable → refuse" branch: the provider now enforces them
/// per call on the resolved path); everything else refuses with an error
/// naming exactly why.
///
/// # Errors
///
/// A typed refusal for a malformed, wrong-type, or undeclared scope.
fn admit(grant: &Grant) -> Result<(), KernelError> {
    let Some(scope) = &grant.scope else {
        return Ok(());
    };
    let contract = &grant.contract;
    match (declared(contract), scope) {
        (Declared::Rate, ScopeValue::Rate(_)) => Ok(()),
        (Declared::Rate, wrote) => Err(refused(format!(
            "grant refused: {contract} declares scope type rate; scope {wrote} is not a rate"
        ))),
        // A containment path: rooted at the provider's root, normal
        // components only — a traversing scope is not a prefix.
        (Declared::PathPrefix, ScopeValue::Path(path)) => crate::hostfs::scope::lexical(path)
            .map(|_| ())
            .map_err(|_| {
                refused(format!(
                    "grant refused: {contract} scope {path:?} is not a containment path"
                ))
            }),
        (Declared::PathPrefix, wrote) => Err(refused(format!(
            "grant refused: {contract} declares scope type path-prefix; \
             scope {wrote} is not a path"
        ))),
        (Declared::NoScope, wrote) => Err(refused(format!(
            "grant refused: {contract} declares no scope type; scope {wrote} cannot validate"
        ))),
        (Declared::Undeclared, wrote) => Err(refused(format!(
            "grant refused: no contract bundle declares a scope type for \
             {contract}; scope {wrote} cannot validate"
        ))),
    }
}

/// Splits a seat's grants into the admitted and the refusals to record —
/// pure judgment, so the lane's admission loop stays the single place the
/// refusals land on the ledger with the entry's attribution.
pub(crate) fn admission(grants: &[Grant]) -> (Vec<Grant>, Vec<KernelError>) {
    let mut admitted = Vec::new();
    let mut refusals = Vec::new();
    for grant in grants {
        match admit(grant) {
            Ok(()) => admitted.push(grant.clone()),
            Err(error) => refusals.push(error),
        }
    }
    (admitted, refusals)
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use super::{Grant, ScopeValue, admission};

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
        }
    }

    fn refusal_of(verdict: Result<(), String>) -> String {
        match verdict {
            Err(message) => message,
            Ok(()) => panic!("the scope must refuse the grant"),
        }
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
        let message = refusal_of(only(scoped("jinn:clock", ScopeValue::Path("fine".into()))));
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
        let verdict = only(scoped("jinn:fs", ScopeValue::Path("/data".into())));
        assert!(
            verdict.is_ok(),
            "the path-prefix scope is enforceable: {verdict:?}"
        );
        let message = refusal_of(only(scoped("jinn:fs", ScopeValue::Path("../up".into()))));
        assert!(
            message.contains("containment"),
            "the refusal names the shape: {message}"
        );
    }

    /// A scope shape the decoder could not read refuses wherever it lands.
    #[test]
    fn a_malformed_scope_refuses_on_every_contract() {
        for contract in ["jinn:clock", "jinn:fs", "jinn:ledger", "demo:thing"] {
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
    /// root/default scope, exactly the pre-ruling semantics.
    #[test]
    fn bare_grants_admit_unchanged() {
        for contract in ["jinn:clock", "jinn:fs", "jinn:ledger", "jinn:test/counter"] {
            let verdict = only(Grant {
                contract: contract.to_owned(),
                scope: None,
            });
            assert!(verdict.is_ok(), "a bare grant admits: {contract}");
        }
    }

    /// One judgment call, split verdicts: mixed lists partition exactly.
    #[test]
    fn admission_partitions_a_mixed_grant_list() {
        let (admitted, refusals) = admission(&[
            Grant {
                contract: "jinn:test/counter".to_owned(),
                scope: None,
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
}
