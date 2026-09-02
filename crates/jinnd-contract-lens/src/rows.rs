//! Gate 2 (M2-K16): every ledger row shape a contract enumerates —
//! `NetRequested { effect, method, host, ... }` in a bundle's prose — is
//! the shape the facade actually writes, compared against the facade's own
//! definition ([`crate::facade`]) rather than a hand-copied sentence.
//! Instance two was the net bundle enumerating a row without `effect`
//! while the facade and the README both carried it.
//!
//! Row mentions live in comments and READMEs, which no format parses, so
//! this module is the ONE reader of that convention (round-2 ruling 1).
//! Its grammar, and it FAILS CLOSED:
//!
//! ```text
//! mention := kind ws* "{" body "}"      kind  := [A-Z][A-Za-z0-9_]* at a word boundary
//! body    := item ("," item)* ("," tail)?
//! item    := field ((":" | "=") value)?  field := "`"? [a-z_][a-z0-9_]* "`"?
//! tail    := "..." | "…"                 (a stated PREFIX of the row)
//! ```
//!
//! Comment markers and line breaks fold to spaces, so a mention may span
//! lines. Every `kind {` is a CANDIDATE; one whose body does not read as
//! `body` — no closing brace within the window, an item that is not a
//! field, a tail that is not last — is [`Candidate::Unreadable`], and the
//! sweep reports it at its line — never skips it.

use std::collections::BTreeMap;

/// The characters after `{` searched for the closing brace.
const WINDOW: usize = 400;

/// One `Kind { field, field: value, ... }` mention in contract prose.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowMention {
    /// 1-based line the mention opens on.
    pub line: usize,
    pub kind: String,
    /// Field NAMES in the order written; a `field: value` spelling
    /// contributes its name only.
    pub fields: Vec<String>,
    /// The mention ends in the tail: a stated prefix, not the whole row.
    pub open: bool,
}

/// One `Kind {` the reader judged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Candidate {
    Read(RowMention),
    /// Looked like a row; did not read as one. Reported, never skipped.
    Unreadable {
        line: usize,
        kind: String,
        why: &'static str,
    },
}

/// One reported disagreement of gate 2.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Disagreement {
    /// A mention whose fields are not the facade's, or whose kind the
    /// facade never declared (`declared` is `None`).
    Shape {
        path: String,
        line: usize,
        kind: String,
        found: Vec<String>,
        declared: Option<Vec<String>>,
    },
    /// A candidate the grammar above could not read.
    Unreadable {
        path: String,
        line: usize,
        kind: String,
        why: &'static str,
    },
}

impl std::fmt::Display for Disagreement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Disagreement::Shape {
                path,
                line,
                kind,
                found,
                declared: Some(declared),
            } => write!(
                f,
                "{path}:{line}: {kind} {{ {} }} but the facade writes {{ {} }}",
                found.join(", "),
                declared.join(", ")
            ),
            Disagreement::Shape {
                path,
                line,
                kind,
                found,
                declared: None,
            } => write!(
                f,
                "{path}:{line}: {kind} {{ {} }} names no LedgerEventKind variant",
                found.join(", ")
            ),
            Disagreement::Unreadable {
                path,
                line,
                kind,
                why,
            } => write!(f, "{path}:{line}: {kind} {{ …: {why}"),
        }
    }
}

/// Every candidate in `text`, in order, each read or reported.
pub fn candidates(text: &str) -> Vec<Candidate> {
    // Each logical character with the line it came from, comment markers
    // and line breaks folded to single spaces.
    let mut chars: Vec<(char, usize)> = Vec::new();
    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim_start();
        let body = ["//!", "///", "//", "#"]
            .iter()
            .find_map(|marker| trimmed.strip_prefix(marker))
            .unwrap_or(trimmed);
        chars.extend(body.chars().map(|c| (c, index + 1)));
        chars.push((' ', index + 1));
    }
    let mut found = Vec::new();
    let mut at = 0;
    while at < chars.len() {
        let (c, line) = chars[at];
        let boundary = at == 0 || !is_ident_char(chars[at - 1].0);
        if !(boundary && c.is_ascii_uppercase()) {
            at += 1;
            continue;
        }
        let mut end = at;
        while end < chars.len() && is_ident_char(chars[end].0) {
            end += 1;
        }
        let kind: String = chars[at..end].iter().map(|(c, _)| *c).collect();
        let mut brace = end;
        while brace < chars.len() && chars[brace].0 == ' ' {
            brace += 1;
        }
        if brace >= chars.len() || chars[brace].0 != '{' {
            at = end;
            continue;
        }
        let close = (brace + 1..chars.len().min(brace + WINDOW)).find(|i| chars[*i].0 == '}');
        let body: String = chars[brace + 1..close.unwrap_or(chars.len())]
            .iter()
            .map(|(c, _)| *c)
            .collect();
        let read = match close {
            None => Err("no closing brace within the window"),
            Some(_) => read_fields(&body),
        };
        found.push(match read {
            Ok((fields, open)) => Candidate::Read(RowMention {
                line,
                kind,
                fields,
                open,
            }),
            Err(why) => Candidate::Unreadable { line, kind, why },
        });
        at = close.map_or(chars.len(), |close| close + 1);
    }
    found
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// The field names of a brace body under the grammar above, and whether it
/// ends in the tail; `Err` names the first thing that does not read.
fn read_fields(body: &str) -> Result<(Vec<String>, bool), &'static str> {
    let items: Vec<&str> = body.split(',').map(str::trim).collect();
    let mut names = Vec::new();
    for (index, item) in items.iter().enumerate() {
        if *item == "..." || *item == "…" {
            return if index > 0 && index + 1 == items.len() {
                Ok((names, true))
            } else {
                Err("the `...` tail is not the last item")
            };
        }
        let name = item
            .split([':', '='])
            .next()
            .unwrap_or_default()
            .trim()
            .trim_matches('`');
        let is_field = name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
            && name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        if !is_field {
            return Err("an item is not `field` or `field: value`");
        }
        names.push(name.to_owned());
    }
    Ok((names, false))
}

/// Everything gate 2 reports for `text`: a mention whose fields are not
/// the facade's, a kind the facade never declared, or a candidate it
/// cannot read.
pub fn disagreements(
    path: &str,
    text: &str,
    facade: &BTreeMap<String, Vec<String>>,
) -> Vec<Disagreement> {
    candidates(text)
        .into_iter()
        .filter_map(|candidate| match candidate {
            Candidate::Unreadable { line, kind, why } => Some(Disagreement::Unreadable {
                path: path.to_owned(),
                line,
                kind,
                why,
            }),
            Candidate::Read(mention) => {
                let declared = facade.get(&mention.kind);
                let agrees = declared.is_some_and(|declared| {
                    if mention.open {
                        declared.starts_with(&mention.fields)
                    } else {
                        declared == &mention.fields
                    }
                });
                (!agrees).then(|| Disagreement::Shape {
                    path: path.to_owned(),
                    line: mention.line,
                    kind: mention.kind,
                    found: mention.fields,
                    declared: declared.cloned(),
                })
            }
        })
        .collect()
}
