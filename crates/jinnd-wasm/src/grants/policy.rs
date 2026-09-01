//! The typed scope policies of the `jinn:process` and `jinn:net` bundles
//! (M2-K6; contracts/*/metadata.toml `[scope]`), the broker-side authority
//! one admitted grant becomes, and how several grants of one contract to
//! one entry COMPOSE. A bare grant holds the EMPTY policy — default deny,
//! never a widened authority (R9, Law 1). Reading these shapes off a
//! profile document is `parse`'s job.

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

    /// Whether the allowlist admits an outbound call to `authority`, given
    /// already in the normal form [`normalize_authority`] answers (M2-K14).
    ///
    /// An entry admits the target iff their normal forms are EQUAL: no
    /// wildcard, no suffix match, and no "the host alone means every port"
    /// — granting `example.com` confers `example.com:80` and nothing else,
    /// exactly as two disjoint bind ranges never confer the port between
    /// them (Law 1, the M2-K8 round-3 ruling read for hosts). Fail-closed:
    /// the empty allowlist, and an entry that is not a readable authority,
    /// admit nothing.
    #[must_use]
    pub fn admits_authority(&self, authority: &str) -> bool {
        self.outbound
            .iter()
            .filter_map(|entry| normalize_authority(entry))
            .any(|(normal, _, _)| normal == authority)
    }
}

/// One authority — `host`, `host:port`, or `[v6]:port` — in normal form:
/// `(normal, host, port)`, host lowercased and `port` defaulted to 80 (the
/// only scheme v0.2 speaks). `None` for anything that is not a readable
/// authority: an empty host, userinfo (`user:pw@host` — a credential in a
/// URL is refused, never quietly edited out), a port that is not a u16, or
/// an unterminated v6 literal.
#[must_use]
pub fn normalize_authority(text: &str) -> Option<(String, String, u16)> {
    if text.is_empty() || text.contains('@') || text.contains('/') {
        return None;
    }
    let (host, port) = if let Some(rest) = text.strip_prefix('[') {
        let (inside, tail) = rest.split_once(']')?;
        (inside.to_owned(), tail.strip_prefix(':').map(str::to_owned))
    } else {
        match text.split_once(':') {
            Some((host, port)) => (host.to_owned(), Some(port.to_owned())),
            None => (text.to_owned(), None),
        }
    };
    let port = match port {
        None => 80,
        Some(port) => port.parse().ok()?,
    };
    if host.is_empty() {
        return None;
    }
    let host = host.to_lowercase();
    let normal = if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    Some((normal, host, port))
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
