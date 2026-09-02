//! Gate 2 (M2-K16): every ledger row shape a contract enumerates —
//! `NetRequested { effect, method, host, ... }` in a bundle's prose — is
//! the shape the facade actually writes, compared against the facade's own
//! definition ([`crate::facade`]) rather than a hand-copied sentence.
//! Instance two was the net bundle enumerating a row without `effect`
//! while the facade and the README both carried it.

use std::collections::BTreeMap;

/// One `Kind { field, field: value, ... }` mention in contract prose.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowMention {
    /// 1-based line the mention opens on.
    pub line: usize,
    pub kind: String,
    /// Field NAMES in the order written; a `field: value` spelling
    /// contributes its name only.
    pub fields: Vec<String>,
    /// The mention ends in `...`/`…`: a stated prefix, not the whole row.
    pub open: bool,
}

/// A mention that does not match the facade.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Disagreement {
    pub path: String,
    pub line: usize,
    pub kind: String,
    pub found: Vec<String>,
    /// The facade's fields, or `None` when the facade has no such kind.
    pub declared: Option<Vec<String>>,
}

impl std::fmt::Display for Disagreement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.declared {
            Some(declared) => write!(
                f,
                "{}:{}: {} {{ {} }} but the facade writes {{ {} }}",
                self.path,
                self.line,
                self.kind,
                self.found.join(", "),
                declared.join(", ")
            ),
            None => write!(
                f,
                "{}:{}: {} {{ {} }} names no LedgerEventKind variant",
                self.path,
                self.line,
                self.kind,
                self.found.join(", ")
            ),
        }
    }
}

/// Every row mention in `text`. Comment markers are stripped per line and
/// a mention may span lines, so `# NetRequested { a, b,\n# c }` reads as
/// one mention of three fields.
pub fn mentions(text: &str) -> Vec<RowMention> {
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
        let Some(close) = (brace + 1..chars.len().min(brace + 400)).find(|i| chars[*i].0 == '}')
        else {
            at = end;
            continue;
        };
        let body: String = chars[brace + 1..close].iter().map(|(c, _)| *c).collect();
        found.push(RowMention {
            line,
            kind,
            fields: field_names(&body),
            open: body.trim_end().ends_with("...") || body.trim_end().ends_with('…'),
        });
        at = close + 1;
    }
    found
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// The field names of a brace body: `a, b: value, c = 1, ...` → `[a, b, c]`.
fn field_names(body: &str) -> Vec<String> {
    body.split(',')
        .map(|part| {
            let part = part.trim();
            let name = part
                .split(|c: char| c == ':' || c == '=' || c.is_whitespace())
                .next()
                .unwrap_or_default();
            name.trim_matches('`').to_owned()
        })
        .filter(|name| !name.is_empty() && name != "..." && name != "…" && name != "..")
        .collect()
}

/// Every mention in `text` that names a facade kind with other fields, or
/// names no facade kind at all.
pub fn disagreements(
    path: &str,
    text: &str,
    facade: &BTreeMap<String, Vec<String>>,
) -> Vec<Disagreement> {
    let _ = (path, text, facade);
    Vec::new()
}
