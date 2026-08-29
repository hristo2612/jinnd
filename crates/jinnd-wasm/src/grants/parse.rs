//! Reading the bundles' declared scope shapes off a profile document.
//! FAIL-CLOSED (M2-K2 law): a shape the bundle did not declare refuses with
//! an error naming exactly why — never narrowed, never widened, never
//! silently dropped (R3, R9, Law 1). The authority these shapes become,
//! and how grants of one contract compose, live in `policy`.

use std::path::{Component, Path};

use jinnd_api::KernelError;

use super::policy::{EnvPolicy, NetScope, ProcessScope};
use super::{ScopeValue, refused};

fn absolute_prefix(path: &str) -> bool {
    let path = Path::new(path);
    path.is_absolute()
        && path
            .components()
            .all(|part| matches!(part, Component::RootDir | Component::Normal(_)))
}

fn field<'a>(fields: &'a [(String, ScopeValue)], name: &str) -> Option<&'a ScopeValue> {
    fields
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value)
}

fn strings(contract: &str, what: &str, value: &ScopeValue) -> Result<Vec<String>, KernelError> {
    let ScopeValue::List(items) = value else {
        return Err(refused(format!(
            "grant refused: {contract} scope {what} must be a list of strings, wrote {value}"
        )));
    };
    items
        .iter()
        .map(|item| match item {
            ScopeValue::Path(text) => Ok(text.clone()),
            other => Err(refused(format!(
                "grant refused: {contract} scope {what} entry is not a string: {other}"
            ))),
        })
        .collect()
}

/// Parses one `process-policy` scope (`{ exec, env }`).
///
/// # Errors
///
/// A typed refusal naming the offending field.
pub(super) fn process_scope(
    contract: &str,
    scope: &ScopeValue,
) -> Result<ProcessScope, KernelError> {
    let ScopeValue::Map(fields) = scope else {
        return Err(refused(format!(
            "grant refused: {contract} declares scope type process-policy; scope {scope} is not a policy"
        )));
    };
    let exec = match field(fields, "exec") {
        None => Vec::new(),
        Some(value) => strings(contract, "exec", value)?,
    };
    if let Some(bad) = exec.iter().find(|prefix| !absolute_prefix(prefix)) {
        return Err(refused(format!(
            "grant refused: {contract} exec prefix {bad:?} is not an absolute normal path"
        )));
    }
    let env = match field(fields, "env") {
        None => EnvPolicy::InheritNone,
        Some(ScopeValue::Path(policy)) if policy == "inherit-none" => EnvPolicy::InheritNone,
        Some(value @ ScopeValue::List(_)) => EnvPolicy::Allow(strings(contract, "env", value)?),
        Some(other) => {
            return Err(refused(format!(
                "grant refused: {contract} env policy must be \"inherit-none\" or a name list, wrote {other}"
            )));
        }
    };
    for (key, _) in fields {
        if key != "exec" && key != "env" {
            return Err(refused(format!(
                "grant refused: {contract} scope has no field {key:?}"
            )));
        }
    }
    Ok(ProcessScope { exec, env })
}

/// Parses one `entry-ids` scope: a non-empty list of entry ids, or `"*"`.
///
/// # Errors
///
/// A typed refusal for any other shape (a bare string is not a list).
pub(super) fn entry_scope(contract: &str, scope: &ScopeValue) -> Result<Vec<String>, KernelError> {
    let ids = strings(contract, "entry-ids", scope)?;
    if ids.is_empty() || ids.iter().any(String::is_empty) {
        return Err(refused(format!(
            "grant refused: {contract} scope must name at least one entry id (or \"*\"), wrote {scope}"
        )));
    }
    Ok(ids)
}

/// Parses one `key-prefix` scope (M2-K8): a non-empty list of non-empty
/// key-name prefixes, or one prefix string.
///
/// # Errors
///
/// A typed refusal for any other shape.
pub(super) fn key_scope(contract: &str, scope: &ScopeValue) -> Result<Vec<String>, KernelError> {
    let prefixes = match scope {
        ScopeValue::Path(prefix) => vec![prefix.clone()],
        other => strings(contract, "key-prefix", other)?,
    };
    if prefixes.is_empty() || prefixes.iter().any(String::is_empty) {
        return Err(refused(format!(
            "grant refused: {contract} scope must name at least one non-empty key prefix, wrote {scope}"
        )));
    }
    Ok(prefixes)
}

/// Parses one `net-policy` scope (`{ bind, outbound }`).
///
/// # Errors
///
/// A typed refusal naming the offending field.
pub(super) fn net_scope(contract: &str, scope: &ScopeValue) -> Result<NetScope, KernelError> {
    let ScopeValue::Map(fields) = scope else {
        return Err(refused(format!(
            "grant refused: {contract} declares scope type net-policy; scope {scope} is not a policy"
        )));
    };
    let bind = match field(fields, "bind") {
        None => Vec::new(),
        Some(ScopeValue::List(range)) => match range.as_slice() {
            [ScopeValue::Rate(low), ScopeValue::Rate(high)] if low <= high && *high <= 65_535 => {
                vec![(*low as u16, *high as u16)]
            }
            _ => {
                return Err(refused(format!(
                    "grant refused: {contract} bind range must be [low, high] within 0..=65535, wrote {}",
                    ScopeValue::List(range.clone())
                )));
            }
        },
        Some(other) => {
            return Err(refused(format!(
                "grant refused: {contract} bind must be a [low, high] port range, wrote {other}"
            )));
        }
    };
    let outbound = match field(fields, "outbound") {
        None => Vec::new(),
        Some(value) => strings(contract, "outbound", value)?,
    };
    for (key, _) in fields {
        if key != "bind" && key != "outbound" {
            return Err(refused(format!(
                "grant refused: {contract} scope has no field {key:?}"
            )));
        }
    }
    Ok(NetScope { bind, outbound })
}
