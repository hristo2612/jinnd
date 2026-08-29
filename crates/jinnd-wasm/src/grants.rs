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
use crate::hostcaps::{NET_CONTRACT, PROCESS_CONTRACT};
use crate::hostfs::FS_CONTRACT;
use crate::hostkeystore::KEYSTORE_CONTRACT;

mod policy;
#[cfg(all(test, not(feature = "loom")))]
mod tests;

pub use policy::{EnvPolicy, GrantScope, NetScope, ProcessScope};

/// The read-only composition contract's name (M2-K7, harness #19).
pub const INTROSPECT_CONTRACT: &str = "jinn:introspect";
/// The profile-patch contract's name (M2-K7, harness #21).
pub const PROFILE_CONTRACT: &str = "jinn:profile";

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
    /// A list of written values (M2-K6: policy fields).
    List(Vec<ScopeValue>),
    /// A keyed document (M2-K6: the `process-policy` / `net-policy` shape).
    Map(Vec<(String, ScopeValue)>),
    /// Any other written shape, carried verbatim for the refusal record.
    Malformed(String),
}

impl std::fmt::Display for ScopeValue {
    fn fmt(&self, out: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rate(floor) => write!(out, "{floor}"),
            Self::Path(path) => write!(out, "{path:?}"),
            Self::List(items) => {
                write!(out, "[")?;
                for (index, item) in items.iter().enumerate() {
                    write!(out, "{}{item}", if index == 0 { "" } else { ", " })?;
                }
                write!(out, "]")
            }
            Self::Map(fields) => {
                write!(out, "{{")?;
                for (index, (key, value)) in fields.iter().enumerate() {
                    write!(out, "{}{key}: {value}", if index == 0 { " " } else { ", " })?;
                }
                write!(out, " }}")
            }
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
    /// The operation-class attenuation (M2-K8, harness #24): the written
    /// `ops` list, or `None` for every operation the bundle declares.
    pub ops: Option<ScopeValue>,
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
    /// `process-policy`: exec allowlist + env policy (jinn:process, M2-K6).
    ProcessPolicy,
    /// `net-policy`: bind range + outbound allowlist (jinn:net, M2-K6).
    NetPolicy,
    /// `entry-ids`: the profile entries a patch may target (jinn:profile,
    /// M2-K7); `*` only when written.
    EntryIds,
    /// The bundle declares no scope (jinn:ledger, jinn:introspect).
    NoScope,
    /// No shipped bundle declares a scope type for this contract.
    Undeclared,
}

fn declared(contract: &str) -> Declared {
    match contract {
        CLOCK_CONTRACT => Declared::Rate,
        FS_CONTRACT => Declared::PathPrefix,
        PROCESS_CONTRACT => Declared::ProcessPolicy,
        NET_CONTRACT => Declared::NetPolicy,
        PROFILE_CONTRACT => Declared::EntryIds,
        "jinn:ledger" | INTROSPECT_CONTRACT => Declared::NoScope,
        _ => Declared::Undeclared,
    }
}

fn refused(message: String) -> KernelError {
    refusal(ErrorCode::EffectFailed, message)
}

/// The operations each shipped bundle declares (contracts/*/metadata.toml
/// `[operations.*]`): the closed vocabulary an `ops` attenuation may name.
fn declared_ops(contract: &str) -> Option<&'static [&'static str]> {
    Some(match contract {
        FS_CONTRACT => &["read", "list", "meta", "write", "append", "remove"],
        KEYSTORE_CONTRACT => &["get", "put", "delete", "list"],
        PROFILE_CONTRACT => &["patch-entry", "entry", "document"],
        CLOCK_CONTRACT => &["now", "alarm-at", "alarm-every"],
        PROCESS_CONTRACT => &[
            "run",
            "spawn",
            "write-stdin",
            "close-stdin",
            "read",
            "wait",
            "kill",
        ],
        NET_CONTRACT => &["request", "listen", "accept", "read", "write", "close"],
        "jinn:ledger" => &["read-range", "last-seq"],
        INTROSPECT_CONTRACT => &["entries", "readiness"],
        _ => return None,
    })
}

/// Validates one written `ops` attenuation against the bundle's declared
/// operations (M2-K8; fail-closed): a non-empty list of declared names.
fn admit_ops(contract: &str, wrote: &ScopeValue) -> Result<Vec<String>, KernelError> {
    let Some(declared) = declared_ops(contract) else {
        return Err(refused(format!(
            "grant refused: no contract bundle declares operations for {contract}; \
             ops {wrote} cannot validate"
        )));
    };
    let ScopeValue::List(items) = wrote else {
        return Err(refused(format!(
            "grant refused: {contract} ops must be a list of operation names, wrote {wrote}"
        )));
    };
    let mut ops: Vec<String> = Vec::new();
    for item in items {
        match item {
            ScopeValue::Path(name) if declared.contains(&name.as_str()) => {
                if !ops.contains(name) {
                    ops.push(name.clone());
                }
            }
            other => {
                return Err(refused(format!(
                    "grant refused: {contract} declares no operation {other}"
                )));
            }
        }
    }
    if ops.is_empty() {
        return Err(refused(format!(
            "grant refused: {contract} ops names no operation (drop the grant instead)"
        )));
    }
    Ok(ops)
}

/// THE fail-closed judgment on one grant (round-3 ruling). A bare grant
/// admits (the contract's root/default scope — for the process and net
/// bundles that is the EMPTY policy, M2-K6). A scoped grant admits only
/// when its scope validates against the contract's declared scope type —
/// the clock's rate floor (M2-K2), the fs's path prefix (M2-K3 round 2),
/// the process and net policies (M2-K6); everything else refuses with an
/// error naming exactly why.
///
/// # Errors
///
/// A typed refusal for a malformed, wrong-type, or undeclared scope.
fn admit(grant: &Grant) -> Result<(), KernelError> {
    if let Some(ops) = &grant.ops {
        admit_ops(&grant.contract, ops)?;
    }
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
        (Declared::ProcessPolicy, wrote) => policy::process_scope(contract, wrote).map(|_| ()),
        (Declared::NetPolicy, wrote) => policy::net_scope(contract, wrote).map(|_| ()),
        (Declared::EntryIds, wrote) => policy::entry_scope(contract, wrote).map(|_| ()),
        (Declared::NoScope, wrote) => Err(refused(format!(
            "grant refused: {contract} declares no scope type; scope {wrote} cannot validate"
        ))),
        (Declared::Undeclared, wrote) => Err(refused(format!(
            "grant refused: no contract bundle declares a scope type for \
             {contract}; scope {wrote} cannot validate"
        ))),
    }
}

/// The broker authority one ADMITTED grant holds: the parsed policy for
/// the process/net bundles (empty for a bare grant — default deny), the
/// path subtree for fs, the root scope otherwise. Only admitted grants
/// reach here, so a policy parse cannot fail; a shape that somehow did is
/// the empty policy, never root.
#[must_use]
pub(crate) fn authority(grant: &Grant) -> GrantScope {
    let contract = grant.contract.as_str();
    match (declared(contract), &grant.scope) {
        (Declared::ProcessPolicy, scope) => GrantScope::Process(
            scope
                .as_ref()
                .and_then(|wrote| policy::process_scope(contract, wrote).ok())
                .unwrap_or_default(),
        ),
        (Declared::NetPolicy, scope) => GrantScope::Net(
            scope
                .as_ref()
                .and_then(|wrote| policy::net_scope(contract, wrote).ok())
                .unwrap_or_default(),
        ),
        // A bare profile grant patches NOTHING (default deny, M2-K2 law).
        (Declared::EntryIds, scope) => GrantScope::Entries(
            scope
                .as_ref()
                .and_then(|wrote| policy::entry_scope(contract, wrote).ok())
                .unwrap_or_default(),
        ),
        (_, Some(ScopeValue::Path(path))) => GrantScope::Paths(vec![path.clone()]),
        _ => GrantScope::Root,
    }
}

/// The operation class one ADMITTED grant is attenuated to (M2-K8): the
/// deduplicated declared names, or `None` for every operation.
#[must_use]
pub(crate) fn attenuation(grant: &Grant) -> Option<Vec<String>> {
    grant
        .ops
        .as_ref()
        .and_then(|wrote| admit_ops(&grant.contract, wrote).ok())
}

/// The fail-closed verdicts a grant list would draw at activation, for a
/// surface validating a config BEFORE it commits (M2-K7 `jinn:profile`:
/// a patch whose grants would refuse is refused whole, never half-applied).
#[must_use]
pub fn grant_refusals(grants: &[Grant]) -> Vec<KernelError> {
    admission(grants).1
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
