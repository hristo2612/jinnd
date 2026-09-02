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
//! tag           := "M" digits "-" ("K" | "P") digits          (M1-P8, M2-K16)
//! end-of-clause := end of line | whitespace | [),;:] | "." (end of line | whitespace)
//! separator     := ws* [,;(] ws*
//! dated         := version (separator | ws*) tag end-of-clause
//! claim         := version followed by neither a separator nor "M" digit
//! malformed     := version (separator anything | ws* "M" digit anything), not dated
//! ```
//!
//! A run preceded by `@` is a package reference — [`crate::refs`]' domain
//! — and never a mention. On an anchored line every other run is a
//! CANDIDATE: a `dated` mention names a PAST edition of the world (`world
//! 0.3.0, M2-K4`) — read and counted, never checked, because history is
//! not a fact the code knows; a `claim` names the CURRENT world and must
//! equal the version `wit/plugin.wit` DECLARES; a run that reads as
//! neither (`0.10.0-rc1`, `v0.10.0`, `127.0.0.1`) is [`Candidate::Unreadable`],
//! reported at its line, never skipped. So is a `malformed` candidate: a
//! tag is read EXACTLY or not at all, and anything but a tag after a
//! separator reads as nothing — `world 0.9.0, M2-K16-extra` (the round-2
//! verifier fixture) is reported, never dated by the tag it wears. Off an
//! anchored line a bare version is nobody's claim here; a bundle's current
//! identity is spelled `jinn:x@X.Y.Z` so that gate 1 reads it.

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
            found.push(match tag_after(line, at) {
                Ok(tag) => Bare::Version {
                    version: run.to_owned(),
                    tag,
                },
                Err(end) => {
                    at = end;
                    Bare::Unreadable {
                        token: line[start..end].to_owned(),
                    }
                }
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

/// What follows a version ending at `from`: `Ok(None)` when nothing reads
/// as a tag attempt (a claim), `Ok(Some(tag))` when a tag is read EXACTLY
/// and followed by end-of-clause, and `Err(end)` for a malformed attempt
/// — anything but a tag after a separator, or an attempt that does not
/// read exactly — with the index its token extends to.
fn tag_after(line: &str, from: usize) -> Result<Option<String>, usize> {
    let bytes = line.as_bytes();
    let space = |mut at: usize| {
        while at < bytes.len() && bytes[at] == b' ' {
            at += 1;
        }
        at
    };
    let mut at = space(from);
    let separated = matches!(bytes.get(at), Some(b',' | b';' | b'('));
    if separated {
        at = space(at + 1);
    }
    let attempt = bytes.get(at) == Some(&b'M') && bytes.get(at + 1).is_some_and(u8::is_ascii_digit);
    if !separated && !attempt {
        return Ok(None);
    }
    match exact_tag(bytes, at) {
        Some(end) => Ok(Some(line[at..end].to_owned())),
        None => {
            let clause = |b: &&u8| !matches!(b, b' ' | b'\t' | b')' | b',' | b';' | b':');
            Err(at + bytes[at..].iter().take_while(clause).count())
        }
    }
}

/// The end of a tag read EXACTLY at `start` — `M` digits `-` (`K` | `P`)
/// digits — and followed by end-of-clause; `None` for anything else.
fn exact_tag(bytes: &[u8], start: usize) -> Option<usize> {
    let digits = |at: usize| {
        let from = at.min(bytes.len());
        let end = from
            + bytes[from..]
                .iter()
                .take_while(|b| b.is_ascii_digit())
                .count();
        (end > from).then_some(end)
    };
    if bytes.get(start) != Some(&b'M') {
        return None;
    }
    let at = digits(start + 1)?;
    if bytes.get(at) != Some(&b'-') || !matches!(bytes.get(at + 1), Some(b'K' | b'P')) {
        return None;
    }
    let at = digits(at + 2)?;
    let ends = match bytes.get(at) {
        None | Some(b' ' | b'\t' | b')' | b',' | b';' | b':') => true,
        Some(b'.') => matches!(bytes.get(at + 1), None | Some(b' ' | b'\t')),
        Some(_) => false,
    };
    ends.then_some(at)
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
