//! Gate 1 (M2-K16): every `namespace:name@version` reference in the
//! contracts of record names the version that package DECLARES. Instance
//! one was `wit/plugin.wit` line 1 saying `@0.7.0` above a `package`
//! line saying `@0.8.0` — both well-formed, no gate able to see it.
//!
//! The rule is total on purpose: the `ns:name@version` spelling is
//! reserved for the CURRENT identity of a package, everywhere under `wit/`
//! and `contracts/`. A historical note spells its version the way the
//! world's own changelog already does — `0.3.0 (M2-K4)` — without the
//! package prefix, so a reader (and this gate) can tell a claim about what
//! the contract IS from a note about what it was.

use std::collections::BTreeMap;

/// One `namespace:name@version` token, with the 1-based line it sits on.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Reference {
    pub line: usize,
    pub package: String,
    pub version: String,
}

/// A reference whose version is not the one its package declares.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Disagreement {
    pub path: String,
    pub line: usize,
    pub package: String,
    pub found: String,
    pub declared: String,
}

impl std::fmt::Display for Disagreement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}: {}@{} but the package declares @{}",
            self.path, self.line, self.package, self.found, self.declared
        )
    }
}

/// Every `ns:name@X.Y.Z` token in `text`, in order.
pub fn references(text: &str) -> Vec<Reference> {
    let mut found = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let bytes = line.as_bytes();
        for (at, byte) in bytes.iter().enumerate() {
            if *byte != b'@' {
                continue;
            }
            let Some(package) = package_before(line, at) else {
                continue;
            };
            let Some(version) = version_after(line, at + 1) else {
                continue;
            };
            found.push(Reference {
                line: index + 1,
                package,
                version,
            });
        }
    }
    found
}

/// The `ns:name` ending at `end`, if the bytes before it spell one.
fn package_before(line: &str, end: usize) -> Option<String> {
    let bytes = line.as_bytes();
    let mut start = end;
    while start > 0
        && (bytes[start - 1].is_ascii_lowercase()
            || bytes[start - 1].is_ascii_digit()
            || bytes[start - 1] == b'-'
            || bytes[start - 1] == b':')
    {
        start -= 1;
    }
    let candidate = &line[start..end];
    let (namespace, name) = candidate.split_once(':')?;
    let is_ident = |part: &str| {
        part.chars().next().is_some_and(|c| c.is_ascii_lowercase())
            && part
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    };
    (is_ident(namespace) && is_ident(name)).then(|| candidate.to_owned())
}

/// The `X.Y.Z` starting at `start`, if the bytes there spell one.
fn version_after(line: &str, start: usize) -> Option<String> {
    let bytes = line.as_bytes();
    let mut end = start;
    while end < bytes.len() && (bytes[end].is_ascii_digit() || bytes[end] == b'.') {
        end += 1;
    }
    let candidate = &line[start..end];
    let parts: Vec<&str> = candidate.split('.').collect();
    let numeric = parts.len() == 3
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit()));
    numeric.then(|| candidate.to_owned())
}

/// Every reference in `text` to a DECLARED package whose version is not
/// the declared one. A reference to a package nobody declares is not a
/// disagreement — it is outside this gate's knowledge, and is reported
/// by the sweep as such rather than silently accepted.
pub fn disagreements(
    path: &str,
    text: &str,
    declared: &BTreeMap<String, String>,
) -> Vec<Disagreement> {
    references(text)
        .into_iter()
        .filter_map(|reference| {
            let expected = declared.get(&reference.package)?;
            (expected != &reference.version).then(|| Disagreement {
                path: path.to_owned(),
                line: reference.line,
                package: reference.package,
                found: reference.version,
                declared: expected.clone(),
            })
        })
        .collect()
}
