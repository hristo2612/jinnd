//! Gate 1 (M2-K16): every `namespace:name@version` reference in the
//! contracts of record names the version that package DECLARES. Instance
//! one was `wit/plugin.wit` line 1 saying `@0.7.0` above a `package`
//! line saying `@0.8.0` — both well-formed, no gate able to see it.
//!
//! References live in comments and READMEs, which no format parses, so
//! this module is the ONE reader of that convention (round-2 ruling 1).
//! Its grammar, and it FAILS CLOSED:
//!
//! ```text
//! reference := package "@" version
//! package   := ident ":" ident          ident   := [a-z][a-z0-9-]*
//! version   := digits "." digits "." digits, followed by none of [A-Za-z0-9-]
//! ```
//!
//! Every `@` whose left side reads as `package`, OR whose right side opens
//! with a digit, is a CANDIDATE. A candidate that does not read as a whole
//! `reference` is [`Candidate::Unreadable`], and the sweep reports it at
//! its line — never skips it. The `a:b@` spelling is therefore reserved
//! for the CURRENT identity of a package, everywhere under `wit/` and
//! `contracts/` (a `user:secret@host` in prose is a candidate by design);
//! a historical note spells its version bare — `0.3.0 (M2-K4)` — the way
//! the world's own changelog does, so a reader can tell a claim about what
//! the contract IS from a note about what it was.

use std::collections::BTreeMap;

/// One `namespace:name@version` reference, with the 1-based line it sits on.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Reference {
    pub line: usize,
    pub package: String,
    pub version: String,
}

/// One `@` token the reader judged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Candidate {
    Read(Reference),
    /// Looked like a reference; did not read as one. Reported, never skipped.
    Unreadable {
        line: usize,
        token: String,
    },
}

/// One reported disagreement of gate 1.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Disagreement {
    /// A reference whose version is not the one its package declares.
    Version {
        path: String,
        line: usize,
        package: String,
        found: String,
        declared: String,
    },
    /// A reference to a package nothing under `wit/` or `contracts/` declares.
    Undeclared {
        path: String,
        line: usize,
        package: String,
        version: String,
    },
    /// A candidate the grammar above could not read.
    Unreadable {
        path: String,
        line: usize,
        token: String,
    },
}

impl std::fmt::Display for Disagreement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Disagreement::Version {
                path,
                line,
                package,
                found,
                declared,
            } => write!(
                f,
                "{path}:{line}: {package}@{found} but the package declares @{declared}"
            ),
            Disagreement::Undeclared {
                path,
                line,
                package,
                version,
            } => write!(
                f,
                "{path}:{line}: {package}@{version} names no declared package"
            ),
            Disagreement::Unreadable { path, line, token } => {
                write!(
                    f,
                    "{path}:{line}: `{token}` looks like a reference but does not read as one"
                )
            }
        }
    }
}

/// Every candidate in `text`, in order, each read or reported.
pub fn candidates(text: &str) -> Vec<Candidate> {
    let mut found = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let bytes = line.as_bytes();
        for at in 0..bytes.len() {
            if bytes[at] != b'@' {
                continue;
            }
            let (start, package) = package_before(line, at);
            let version = version_after(line, at + 1);
            let opens_numeric = bytes.get(at + 1).is_some_and(u8::is_ascii_digit);
            if package.is_none() && !opens_numeric {
                continue;
            }
            found.push(match (package, version) {
                (Some(package), Some(version)) => Candidate::Read(Reference {
                    line: index + 1,
                    package,
                    version,
                }),
                _ => Candidate::Unreadable {
                    line: index + 1,
                    token: token_from(line, start).to_owned(),
                },
            });
        }
    }
    found
}

/// Where the run of package characters before `end` starts, and the
/// `ns:name` it spells if it spells one.
fn package_before(line: &str, end: usize) -> (usize, Option<String>) {
    let bytes = line.as_bytes();
    let mut start = end;
    while start > 0 && matches!(bytes[start - 1], b'a'..=b'z' | b'0'..=b'9' | b'-' | b':') {
        start -= 1;
    }
    let candidate = &line[start..end];
    let is_ident = |part: &str| {
        part.chars().next().is_some_and(|c| c.is_ascii_lowercase())
            && part
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    };
    let package = candidate
        .split_once(':')
        .filter(|(namespace, name)| is_ident(namespace) && is_ident(name))
        .map(|_| candidate.to_owned());
    (start, package)
}

/// The `X.Y.Z` starting at `start`, if the bytes there spell exactly one
/// and nothing version-like continues it.
fn version_after(line: &str, start: usize) -> Option<String> {
    let bytes = line.as_bytes();
    let mut end = start;
    while end < bytes.len() && (bytes[end].is_ascii_digit() || bytes[end] == b'.') {
        end += 1;
    }
    let continued = bytes
        .get(end)
        .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'-');
    let candidate = &line[start..end];
    let parts: Vec<&str> = candidate.split('.').collect();
    let numeric = parts.len() == 3
        && parts
            .iter()
            .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit()));
    (numeric && !continued).then(|| candidate.to_owned())
}

/// The token at `start`, up to whitespace or a delimiter — for the report.
fn token_from(line: &str, start: usize) -> &str {
    let rest = &line[start..];
    let end = rest
        .find(|c: char| c.is_whitespace() || "`()'\",;".contains(c))
        .unwrap_or(rest.len());
    &rest[..end]
}

/// Everything gate 1 reports for `text`: a version that is not the
/// declared one, a package nobody declares, or a candidate it cannot read.
pub fn disagreements(
    path: &str,
    text: &str,
    declared: &BTreeMap<String, String>,
) -> Vec<Disagreement> {
    candidates(text)
        .into_iter()
        .filter_map(|candidate| match candidate {
            Candidate::Unreadable { line, token } => Some(Disagreement::Unreadable {
                path: path.to_owned(),
                line,
                token,
            }),
            Candidate::Read(reference) => match declared.get(&reference.package) {
                None => Some(Disagreement::Undeclared {
                    path: path.to_owned(),
                    line: reference.line,
                    package: reference.package,
                    version: reference.version,
                }),
                Some(expected) if expected != &reference.version => Some(Disagreement::Version {
                    path: path.to_owned(),
                    line: reference.line,
                    package: reference.package,
                    found: reference.version,
                    declared: expected.clone(),
                }),
                Some(_) => None,
            },
        })
        .collect()
}
