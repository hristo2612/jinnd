//! The contract index (M2-K22, harness FINDINGS #44). `contracts/README.md`
//! carried a hand-kept list — "`jinn-net` (0.1.0), `jinn-introspect`
//! (0.1.0)" — while the bundles declared 0.3.0 and 0.5.0: a second copy of
//! a fact the parser knows, in Markdown, outside every gate's surface.
//!
//! The copy is now DERIVED: [`render`] produces the index's version table
//! from every bundle's PARSED `contract.wit` (identity) and `metadata.toml`
//! (scope type), and the README carries that rendering between two marker
//! lines. This module is the ONE reader of the convention. Its grammar,
//! and it FAILS CLOSED:
//!
//! ```text
//! index  := prelude BEGIN table END postlude
//! BEGIN  := the exact line [`BEGIN`]         END := the exact line [`END`]
//! table  := the bytes [`render`] produces for the parsed bundles, verbatim
//! ```
//!
//! Exactly one `BEGIN` and one `END`, in that order: any other arrangement
//! is [`Disagreement::Malformed`] and nothing else about the file is read.
//! A table that is not the fresh rendering is [`Disagreement::Stale`] with
//! the fresh block attached, so the fix is a paste and never a hand edit.
//! Outside the block a bare version ([`crate::mentions::bare_versions`])
//! is the hand-kept copy this module exists to refuse: undated it is
//! [`Disagreement::Stray`]; dated (`world 0.3.0, M2-K4`) it is history
//! and reads clean; a run that is neither is [`Disagreement::Unreadable`].

use crate::bundles;
use crate::mentions::{Bare, bare_versions};

/// The contract index, repository-relative.
pub const PATH: &str = "contracts/README.md";

/// The line that opens the derived block.
pub const BEGIN: &str =
    "<!-- contract-index: begin (rendered by jinnd-contract-lens; never edit by hand) -->";

/// The line that closes the derived block.
pub const END: &str = "<!-- contract-index: end -->";

/// One bundle as the index renders it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Row {
    pub bundle: String,
    pub package: String,
    pub version: String,
    pub scope: Option<String>,
}

impl Row {
    /// A row from its parts, for fixtures.
    pub fn new(bundle: &str, package: &str, version: &str, scope: Option<&str>) -> Row {
        Row {
            bundle: bundle.to_owned(),
            package: package.to_owned(),
            version: version.to_owned(),
            scope: scope.map(str::to_owned),
        }
    }
}

/// Every shipped bundle's row, read off the parsed contract of record:
/// identity from `contract.wit`, scope type from `metadata.toml`.
pub fn rows() -> Vec<Row> {
    bundles()
        .iter()
        .map(|bundle| {
            let wit = bundle.wit().wit();
            Row {
                bundle: bundle.name().to_owned(),
                package: wit.package_name(),
                version: wit.version(),
                scope: bundle.metadata().metadata().string_at("scope.type"),
            }
        })
        .collect()
}

/// The derived block, markers included, ending in a newline. The package
/// column spells `ns:name@X.Y.Z` so that gate 1 reads it as well.
pub fn render(rows: &[Row]) -> String {
    let mut out = format!("{BEGIN}\n| bundle | contract of record | scope type |\n|---|---|---|\n");
    for row in rows {
        out.push_str(&format!(
            "| `contracts/{}` | `{}@{}` | {} |\n",
            row.bundle,
            row.package,
            row.version,
            row.scope.as_deref().unwrap_or("none")
        ));
    }
    out.push_str(END);
    out.push('\n');
    out
}

/// One reported disagreement of the index reader.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Disagreement {
    /// The block is missing, doubled, or closed before it opens.
    Malformed { path: String, why: String },
    /// The block at `line` is not the fresh rendering.
    Stale {
        path: String,
        line: usize,
        expected: String,
        found: String,
    },
    /// An undated bare version outside the block: a hand-kept copy.
    Stray {
        path: String,
        line: usize,
        version: String,
    },
    /// A dotted run outside the block that reads as no version.
    Unreadable {
        path: String,
        line: usize,
        token: String,
    },
}

impl std::fmt::Display for Disagreement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Disagreement::Malformed { path, why } => write!(f, "{path}: {why}"),
            Disagreement::Stale {
                path,
                line,
                expected,
                ..
            } => write!(
                f,
                "{path}:{line}: the derived block is stale; paste this one:\n{expected}"
            ),
            Disagreement::Stray {
                path,
                line,
                version,
            } => write!(
                f,
                "{path}:{line}: bare version {version} outside the derived block"
            ),
            Disagreement::Unreadable { path, line, token } => write!(
                f,
                "{path}:{line}: `{token}` looks like a version but does not read as one"
            ),
        }
    }
}

/// Everything the reader reports for the index text against the fresh
/// rendering `expected`.
pub fn disagreements(path: &str, text: &str, expected: &str) -> Vec<Disagreement> {
    let lines: Vec<&str> = text.lines().collect();
    let begins: Vec<usize> = (0..lines.len()).filter(|&i| lines[i] == BEGIN).collect();
    let ends: Vec<usize> = (0..lines.len()).filter(|&i| lines[i] == END).collect();
    let malformed = |why: &str| {
        vec![Disagreement::Malformed {
            path: path.to_owned(),
            why: why.to_owned(),
        }]
    };
    let (begin, end) = match (begins.as_slice(), ends.as_slice()) {
        ([begin], [end]) if begin < end => (*begin, *end),
        ([], _) => return malformed("no derived block: the BEGIN marker line is absent"),
        ([_], []) => return malformed("the derived block never closes: no END marker line"),
        ([_], [_]) => return malformed("the END marker line comes before the BEGIN line"),
        _ => return malformed("more than one derived block"),
    };
    let mut found = Vec::new();
    let block: String = lines[begin..=end]
        .iter()
        .map(|l| format!("{l}\n"))
        .collect();
    if block != expected {
        found.push(Disagreement::Stale {
            path: path.to_owned(),
            line: begin + 1,
            expected: expected.to_owned(),
            found: block,
        });
    }
    for (index, line) in lines.iter().enumerate() {
        if (begin..=end).contains(&index) {
            continue;
        }
        for bare in bare_versions(line) {
            found.push(match bare {
                Bare::Version { tag: Some(_), .. } => continue,
                Bare::Version { version, tag: None } => Disagreement::Stray {
                    path: path.to_owned(),
                    line: index + 1,
                    version,
                },
                Bare::Unreadable { token } => Disagreement::Unreadable {
                    path: path.to_owned(),
                    line: index + 1,
                    token,
                },
            });
        }
    }
    found
}
