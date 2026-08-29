//! The operation-class attenuation (M2-K8, harness #24; split from
//! `grants.rs` by responsibility, R10): the closed vocabulary each
//! shipped bundle declares (contracts/*/metadata.toml `[operations.*]`),
//! the fail-closed admission of a written `ops` list against it, and the
//! attenuation an admitted grant becomes at the broker.

use jinnd_api::KernelError;

use super::{Grant, ScopeValue, refused};
use crate::alarms::CLOCK_CONTRACT;
use crate::broker::Broker;
use crate::grants::{INTROSPECT_CONTRACT, PROFILE_CONTRACT};
use crate::hostcaps::{NET_CONTRACT, PROCESS_CONTRACT};
use crate::hostfs::FS_CONTRACT;
use crate::hostkeystore::KEYSTORE_CONTRACT;
use crate::peer::PeerId;

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
pub(crate) fn admit_ops(contract: &str, wrote: &ScopeValue) -> Result<Vec<String>, KernelError> {
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

/// The operation class one ADMITTED grant is attenuated to (M2-K8): the
/// deduplicated declared names, or `None` for every operation.
#[must_use]
pub(crate) fn attenuation(grant: &Grant) -> Option<Vec<String>> {
    grant
        .ops
        .as_ref()
        .and_then(|wrote| admit_ops(&grant.contract, wrote).ok())
}

/// Applies one admitted grant's operation class at the broker (M2-K8):
/// an attenuation narrows the peer's class for the contract; an
/// unattenuated grant of the same contract lifts it (union, as path
/// scopes accumulate).
pub(crate) fn attenuate(broker: &Broker, peer: PeerId, grant: &Grant) {
    match attenuation(grant) {
        Some(ops) => broker.grant_ops(peer, &grant.contract, ops),
        None => broker.lift_ops(peer, &grant.contract),
    }
}
