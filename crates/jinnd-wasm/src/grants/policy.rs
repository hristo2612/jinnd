//! The typed scope policies of the `jinn:process` and `jinn:net` bundles
//! (M2-K6; contracts/*/metadata.toml `[scope]`), and the broker-side
//! authority one admitted grant becomes. Parsing is FAIL-CLOSED (M2-K2
//! law): a shape the bundle did not declare refuses with an error naming
//! exactly why; a bare grant holds the EMPTY policy — default deny, never
//! a widened authority (R9, Law 1).

use std::path::{Component, Path};

use jinnd_api::KernelError;

use super::{ScopeValue, refused};

/// What a child may inherit from the daemon's environment (contract
/// bundle `jinn-process` §scope): nothing, or exactly the named variables.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum EnvPolicy {
    #[default]
    InheritNone,
    Allow(Vec<String>),
}

/// One `process-policy` scope: absolute executable prefixes, enforced on
/// the fully resolved path per call, plus the env policy.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ProcessScope {
    pub exec: Vec<String>,
    pub env: EnvPolicy,
}

/// One `net-policy` scope: the inclusive loopback bind port ranges and the
/// outbound host allowlist (carried for the edition that consumes it).
///
/// `bind` is a SET of ranges, normalized (sorted, overlapping and adjacent
/// ranges coalesced) so equal sets compare equal and composition stays
/// order-independent. It is never the numeric HULL of what was granted: a
/// hull over `[1000,1000]` and `[2000,2000]` would admit port 1500 that no
/// grant conferred (Law 1, M2-K8 round-3 ruling). Empty admits no bind.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NetScope {
    pub bind: Vec<(u16, u16)>,
    pub outbound: Vec<String>,
}

impl NetScope {
    /// Whether the granted set admits binding `port` — true iff SOME range
    /// contains it (fail-closed: the empty set admits nothing).
    #[must_use]
    pub fn admits_port(&self, port: u16) -> bool {
        self.bind
            .iter()
            .any(|(low, high)| (*low..=*high).contains(&port))
    }
}

/// Sorts and coalesces `ranges` in place: overlapping and ADJACENT ranges
/// merge (`[10,20]` with `[21,30]` is one `[10,30]`), a gap of one port
/// stays two. The normal form is what makes union commutative.
fn coalesce(ranges: &mut Vec<(u16, u16)>) {
    ranges.sort_unstable();
    let mut normal: Vec<(u16, u16)> = Vec::with_capacity(ranges.len());
    for (low, high) in ranges.drain(..) {
        match normal.last_mut() {
            Some((_, held)) if low <= held.saturating_add(1) => *held = (*held).max(high),
            _ => normal.push((low, high)),
        }
    }
    *ranges = normal;
}

/// The authority one admitted grant holds at the broker (R4: the caller's
/// scope travels with every call).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GrantScope {
    /// The contract's root/default scope (a bare grant on fs/clock/...).
    Root,
    /// `path-prefix` subtrees (`jinn:fs`); accumulate across grants.
    Paths(Vec<String>),
    Process(ProcessScope),
    Net(NetScope),
    /// `entry-ids` (`jinn:profile`, M2-K7): the entries a patch may
    /// target; `"*"` means every entry, only when written. Empty (a bare
    /// grant) patches nothing.
    Entries(Vec<String>),
    /// `key-prefix` (`jinn:keystore`, M2-K8): the key-name prefixes the
    /// grant admits. Empty (a bare grant) admits no key.
    Keys(Vec<String>),
}

fn extend_unique(held: &mut Vec<String>, more: Vec<String>) {
    for item in more {
        if !held.contains(&item) {
            held.push(item);
        }
    }
}

impl GrantScope {
    /// Composes a second grant of the same contract into this authority
    /// (round-2 ruling 2, Law 1): commutative, so grant order never
    /// changes what a peer holds. Root absorbs; list scopes (paths, keys,
    /// entries, exec/env/outbound allowlists) union; bind ranges union as
    /// a normalized SET, never a hull (round-3 ruling, Law 1).
    /// Scopes of different kinds cannot both be admitted for one contract
    /// (the bundle declares one scope type), so a mismatch keeps the held
    /// authority rather than inventing one.
    pub(crate) fn union(&mut self, other: Self) {
        match (&mut *self, other) {
            (Self::Root, _) => {}
            (_, Self::Root) => *self = Self::Root,
            (Self::Paths(held), Self::Paths(more))
            | (Self::Entries(held), Self::Entries(more))
            | (Self::Keys(held), Self::Keys(more)) => extend_unique(held, more),
            (Self::Process(held), Self::Process(more)) => {
                extend_unique(&mut held.exec, more.exec);
                match (&mut held.env, more.env) {
                    (EnvPolicy::Allow(names), EnvPolicy::Allow(more)) => extend_unique(names, more),
                    (env @ EnvPolicy::InheritNone, more) => *env = more,
                    (EnvPolicy::Allow(_), EnvPolicy::InheritNone) => {}
                }
            }
            (Self::Net(held), Self::Net(more)) => {
                extend_unique(&mut held.outbound, more.outbound);
                held.bind.extend(more.bind);
                coalesce(&mut held.bind);
            }
            _ => {}
        }
    }

    /// Whether an `entry-ids` scope admits patching `entry` (fail-closed:
    /// any other scope shape admits nothing).
    #[must_use]
    pub fn admits_entry(&self, entry: &str) -> bool {
        match self {
            Self::Entries(ids) => ids.iter().any(|id| id == "*" || id == entry),
            _ => false,
        }
    }

    /// Whether a `key-prefix` scope admits `key` (fail-closed: any other
    /// scope shape, and the empty allowlist, admit nothing).
    #[must_use]
    pub fn admits_key(&self, key: &str) -> bool {
        match self {
            Self::Keys(prefixes) => prefixes.iter().any(|prefix| key.starts_with(prefix)),
            _ => false,
        }
    }
}

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
