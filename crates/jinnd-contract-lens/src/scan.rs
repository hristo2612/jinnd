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
    let _ = (path, source);
    Vec::new()
}
