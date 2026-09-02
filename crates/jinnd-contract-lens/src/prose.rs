//! The prose of a contract: what its COMMENTS say (M2-K16).
//!
//! Some contract statements are prose by nature — "suspend ≠ dispose",
//! "refused in EVERY mode" — and a test may want the contract to state
//! them in those words. That is honest only if the search cannot be
//! mistaken for a declaration check, so this view sees comment lines ONLY:
//! `states("record wait-cycle {")` is false however many times the
//! declaration appears, because a declaration is not prose, and a
//! declaration assertion routed here fails instead of passing on a comment.

/// The comment lines of one contract file, markers stripped; every line of
/// a Markdown file.
pub struct Prose {
    lines: Vec<String>,
}

impl Prose {
    /// The prose of `text`, by the file's extension.
    pub fn of(path: &str, text: &str) -> Prose {
        let markers: &[&str] = if path.ends_with(".md") {
            &[]
        } else if path.ends_with(".toml") {
            &["#"]
        } else {
            &["//!", "///", "//"]
        };
        let lines = text
            .lines()
            .filter_map(|line| {
                if markers.is_empty() {
                    return Some(line.to_owned());
                }
                let trimmed = line.trim_start();
                markers
                    .iter()
                    .find_map(|marker| trimmed.strip_prefix(marker))
                    .map(|rest| rest.trim().to_owned())
            })
            .collect();
        Prose { lines }
    }

    /// Whether any prose line carries `phrase` — a statement made in the
    /// contract's own words, never a declaration.
    pub fn states(&self, phrase: &str) -> bool {
        self.line_of(phrase).is_some()
    }

    /// The 1-based prose line index of the first line carrying `phrase`.
    pub fn line_of(&self, phrase: &str) -> Option<usize> {
        self.lines
            .iter()
            .position(|line| line.contains(phrase))
            .map(|index| index + 1)
    }
}
