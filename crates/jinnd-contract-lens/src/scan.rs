//! Gate 3 (M2-K16): loading a contract file's TEXT anywhere but this crate
//! fails. [`crate::Contract`] exposes no `&str`, so the only way to write
//! `WORLD.contains("...")` again is to `include_str!` the file yourself —
//! and that line is what this scan refuses. The shape becomes
//! unexpressible in the ordinary way, not merely discouraged (six shipped
//! instances say "discouraged" does not work).
//!
//! Threat model, inherited: an honest author who believes a substring is a
//! proof. Not an adversary hiding a file read behind indirection.

/// One line that loads a contract file outside the lens.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Offence {
    pub path: String,
    pub line: usize,
    pub text: String,
}

impl std::fmt::Display for Offence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}: {}", self.path, self.line, self.text.trim())
    }
}

/// The lines of `source` (at repository-relative `path`) that read a
/// contract-of-record file by literal path: `include_str!`/`include_bytes!`
/// or a `read_to_string`/`fs::read` naming `wit/` or `contracts/`.
pub fn offences(path: &str, source: &str) -> Vec<Offence> {
    if path.starts_with("crates/jinnd-contract-lens/") {
        return Vec::new();
    }
    source
        .lines()
        .enumerate()
        .filter(|(_, line)| {
            let trimmed = line.trim_start();
            !trimmed.starts_with("//") && reads_a_contract(line)
        })
        .map(|(index, line)| Offence {
            path: path.to_owned(),
            line: index + 1,
            text: line.to_owned(),
        })
        .collect()
}

/// Whether `line` names a contract file inside a loading form.
fn reads_a_contract(line: &str) -> bool {
    const LOADERS: [&str; 4] = [
        "include_str!",
        "include_bytes!",
        "read_to_string(",
        "fs::read(",
    ];
    LOADERS.iter().any(|loader| {
        let Some(after) = line.find(loader).map(|at| &line[at + loader.len()..]) else {
            return false;
        };
        // The first string literal after the loader is the path.
        let Some(open) = after.find('"') else {
            return false;
        };
        let literal = &after[open + 1..];
        let Some(close) = literal.find('"') else {
            return false;
        };
        is_contract_path(&literal[..close])
    })
}

/// A literal path is a contract of record when it lands under `wit/` or
/// `contracts/`, or names a `.wit` file anywhere.
fn is_contract_path(literal: &str) -> bool {
    let segments: Vec<&str> = literal.split('/').collect();
    let under = |dir: &str| segments.iter().rev().skip(1).any(|segment| *segment == dir);
    literal.ends_with(".wit") || under("wit") || under("contracts")
}
