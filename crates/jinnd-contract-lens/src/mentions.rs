//! Reader of bare WORLD MENTIONS (M2-K22, harness FINDINGS #44). Gate 1
//! ([`crate::refs`]) reads `jinn:plugin@X.Y.Z`; the instance that shipped
//! was `contracts/jinn-net/contract.wit:9` saying "the plugin world's
//! `net` import (wit/plugin.wit, 0.9.0)" under a world at 0.10.0 — a
//! version with no `@`, which gate 1 by its stated limit never saw.
//!
//! No format parses a comment, so this module is the ONE reader of that
//! convention (the M2-K16 ruling). Its grammar, and it FAILS CLOSED:
//!
//! ```text
//! anchored-line := a line containing `world` (any case; `world's` included)
//!                  or the path `wit/plugin.wit`
//! run           := digit-led maximal run of [0-9.] holding two or more dots
//! version       := digits "." digits "." digits, preceded by none of
//!                  [A-Za-z0-9-] and followed by none of [A-Za-z0-9-]
//! tag           := "M" digits "-" [A-Z]+ digits [a-z]?      (M1-P8, M2-K16, M1-P6c)
//! dated         := version ws* [,;(]? ws* tag
//! claim         := version, not dated
//! ```
//!
//! A run preceded by `@` is a package reference — [`crate::refs`]' domain
//! — and never a mention. On an anchored line every other run is a
//! CANDIDATE: a `dated` mention names a PAST edition of the world (`world
//! 0.3.0, M2-K4`) — read and counted, never checked, because history is
//! not a fact the code knows; a `claim` names the CURRENT world and must
//! equal the version `wit/plugin.wit` DECLARES; a run that reads as
//! neither (`0.10.0-rc1`, `v0.10.0`, `127.0.0.1`) is [`Candidate::Unreadable`],
//! reported at its line, never skipped. Off an anchored line a bare version
//! is nobody's claim here; a bundle's current identity is spelled
//! `jinn:x@X.Y.Z` so that gate 1 reads it.

/// One bare `X.Y.Z` token on a line, or a run that could not be read as one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Bare {
    /// A version, with the packet tag that dates it when one follows.
    Version {
        version: String,
        tag: Option<String>,
    },
    /// A dotted run that is not a version. Reported by every reader.
    Unreadable { token: String },
}

/// One world mention the reader judged, with the 1-based line it sits on.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Candidate {
    Dated {
        line: usize,
        version: String,
        tag: String,
    },
    Claim {
        line: usize,
        version: String,
    },
    Unreadable {
        line: usize,
        token: String,
    },
}

/// One reported disagreement of the world-mention reader.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Disagreement {
    /// A claim naming an edition the world does not declare.
    Edition {
        path: String,
        line: usize,
        found: String,
        declared: String,
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
            Disagreement::Edition {
                path,
                line,
                found,
                declared,
            } => write!(
                f,
                "{path}:{line}: world {found} but wit/plugin.wit declares {declared}"
            ),
            Disagreement::Unreadable { path, line, token } => write!(
                f,
                "{path}:{line}: `{token}` looks like a version but does not read as one"
            ),
        }
    }
}

/// Whether `line` is anchored to the world under the grammar above.
pub fn anchored(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    lower.contains("world") || lower.contains("wit/plugin.wit")
}

/// Every bare version token on one line, in order, each read or reported.
pub fn bare_versions(line: &str) -> Vec<Bare> {
    let bytes = line.as_bytes();
    let word = |b: u8| b.is_ascii_alphanumeric() || b == b'.' || b == b'-';
    let mut found = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        if !bytes[at].is_ascii_digit() {
            at += 1;
            continue;
        }
        let start = at;
        while at < bytes.len() && (bytes[at].is_ascii_digit() || bytes[at] == b'.') {
            at += 1;
        }
        let run = &line[start..at];
        if run.matches('.').count() < 2 {
            continue;
        }
        let before = start.checked_sub(1).map(|i| bytes[i]);
        if before == Some(b'@') {
            continue;
        }
        let bounded = !before.is_some_and(|b| b.is_ascii_alphanumeric() || b == b'-')
            && !bytes
                .get(at)
                .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'-');
        let three = run.split('.').count() == 3 && run.split('.').all(|part| !part.is_empty());
        if bounded && three {
            found.push(Bare::Version {
                version: run.to_owned(),
                tag: tag_after(line, at),
            });
            continue;
        }
        let mut from = start;
        while from > 0 && word(bytes[from - 1]) {
            from -= 1;
        }
        while at < bytes.len() && word(bytes[at]) {
            at += 1;
        }
        found.push(Bare::Unreadable {
            token: line[from..at].to_owned(),
        });
    }
    found
}

/// The packet tag that dates a version ending at `from`, if one follows.
fn tag_after(line: &str, from: usize) -> Option<String> {
    let bytes = line.as_bytes();
    let mut at = from;
    while at < bytes.len() && bytes[at] == b' ' {
        at += 1;
    }
    if at < bytes.len() && matches!(bytes[at], b',' | b';' | b'(') {
        at += 1;
    }
    while at < bytes.len() && bytes[at] == b' ' {
        at += 1;
    }
    let start = at;
    if bytes.get(at) != Some(&b'M') {
        return None;
    }
    at += 1;
    let digits = |at: &mut usize| {
        let from = *at;
        while *at < bytes.len() && bytes[*at].is_ascii_digit() {
            *at += 1;
        }
        *at > from
    };
    if !digits(&mut at) || bytes.get(at) != Some(&b'-') {
        return None;
    }
    at += 1;
    let letters = at;
    while at < bytes.len() && bytes[at].is_ascii_uppercase() {
        at += 1;
    }
    if at == letters || !digits(&mut at) {
        return None;
    }
    if at < bytes.len() && bytes[at].is_ascii_lowercase() {
        at += 1;
    }
    let continued = bytes.get(at).is_some_and(u8::is_ascii_alphanumeric);
    (!continued).then(|| line[start..at].to_owned())
}

/// Every world-mention candidate in `text`, in order, each read or reported.
pub fn candidates(text: &str) -> Vec<Candidate> {
    let mut found = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if !anchored(line) {
            continue;
        }
        let line_no = index + 1;
        for bare in bare_versions(line) {
            found.push(match bare {
                Bare::Version {
                    version,
                    tag: Some(tag),
                } => Candidate::Dated {
                    line: line_no,
                    version,
                    tag,
                },
                Bare::Version { version, tag: None } => Candidate::Claim {
                    line: line_no,
                    version,
                },
                Bare::Unreadable { token } => Candidate::Unreadable {
                    line: line_no,
                    token,
                },
            });
        }
    }
    found
}

/// Everything the reader reports for `text` against the world's DECLARED
/// version: a claim of another edition, or a candidate it cannot read.
pub fn disagreements(path: &str, text: &str, declared: &str) -> Vec<Disagreement> {
    candidates(text)
        .into_iter()
        .filter_map(|candidate| match candidate {
            Candidate::Dated { .. } => None,
            Candidate::Claim { version, .. } if version == declared => None,
            Candidate::Claim { line, version } => Some(Disagreement::Edition {
                path: path.to_owned(),
                line,
                found: version,
                declared: declared.to_owned(),
            }),
            Candidate::Unreadable { line, token } => Some(Disagreement::Unreadable {
                path: path.to_owned(),
                line,
                token,
            }),
        })
        .collect()
}
