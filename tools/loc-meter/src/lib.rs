//! The canonical LOC meter (M2-K18).
//!
//! Contract: one invocation measures the commits between `merge-base(base,
//! head)` and `head`, reads both sides from git objects (never from the
//! working tree), and reports every changed line under exactly one category.
//! Rust sources are categorised the way the compiler categorises them: a
//! file counts as production only if a `lib`/`bin` target of a workspace
//! package under `crates/` reaches it through `mod` declarations, and an item
//! whose `#[cfg(..)]` predicate is false in the default non-test build is
//! removed from the production count before diffing. Markdown is never
//! production; it has its own line. A dirty working tree is refused, never
//! footnoted: the meter prints no number it knows a reader could misread.

mod cargo;
mod cfg;
mod git;
mod side;
mod walk;

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};

pub use git::toplevel;
pub use side::Side;

/// Where a changed line is billed. Exactly one category per line.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Category {
    /// Kernel crates (`crates/*` minus the harness lane), non-test compiled sources only.
    Production,
    /// The conformance-harness lane: `crates/jinnd-api` and `crates/jinnd-adapter` (R10 metric note).
    Facade,
    /// Contract files: `.wit` and `.toml`.
    Contracts,
    /// Markdown, anywhere.
    Prose,
    /// Rust outside the non-test kernel build: `cfg`-false items, `tests/` trees, integration tests, fixtures, demo guests.
    Tests,
    /// Anything under `tools/` that is Rust.
    Tools,
    /// Everything else (lockfiles, scripts, CI, binary fixtures).
    Other,
}

impl Category {
    /// The four budget lines, in report order.
    pub const BUDGET: [Category; 4] = [
        Category::Production,
        Category::Facade,
        Category::Contracts,
        Category::Prose,
    ];
    /// The lines outside every budget, in report order.
    pub const EXCLUDED: [Category; 3] = [Category::Tests, Category::Tools, Category::Other];

    /// The short label printed at the start of the report line.
    pub fn label(self) -> &'static str {
        match self {
            Category::Production => "production",
            Category::Facade => "facade",
            Category::Contracts => "contracts",
            Category::Prose => "prose",
            Category::Tests => "tests",
            Category::Tools => "tools",
            Category::Other => "other",
        }
    }

    /// What the line covers, printed after the numbers.
    pub fn describe(self) -> &'static str {
        match self {
            Category::Production => "crates/* minus the harness lane; non-test compiled Rust only",
            Category::Facade => "crates/jinnd-api + crates/jinnd-adapter (harness lane, R10)",
            Category::Contracts => ".wit + .toml",
            Category::Prose => ".md",
            Category::Tests => {
                "Rust outside the non-test kernel build (cfg-off items, tests/, fixtures)"
            }
            Category::Tools => "Rust under tools/",
            Category::Other => "everything else",
        }
    }
}

/// Added and deleted line counts, git `--numstat` semantics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Delta {
    pub added: u64,
    pub deleted: u64,
}

impl Delta {
    /// `added - deleted`.
    pub fn net(self) -> i64 {
        self.added as i64 - self.deleted as i64
    }

    fn add(&mut self, other: Delta) {
        self.added += other.added;
        self.deleted += other.deleted;
    }
}

/// One billed row: a path (old and/or new side) under one category.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileRow {
    pub category: Category,
    pub delta: Delta,
    pub old: Option<String>,
    pub new: Option<String>,
    /// True for the companion row that carries a production file's `cfg`-off items.
    pub cfg_off: bool,
}

impl FileRow {
    /// The path as it should be printed: `old -> new` for renames.
    pub fn display_path(&self) -> String {
        let suffix = if self.cfg_off { " [cfg-off items]" } else { "" };
        match (&self.old, &self.new) {
            (Some(o), Some(n)) if o != n => format!("{o} -> {n}{suffix}"),
            (_, Some(n)) => format!("{n}{suffix}"),
            (Some(o), None) => format!("{o}{suffix}"),
            (None, None) => suffix.to_string(),
        }
    }
}

/// The measurement of one range of commits.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    /// The merge-base actually measured from (full sha).
    pub base: String,
    /// The head measured to (full sha).
    pub head: String,
    pub files: Vec<FileRow>,
}

impl Report {
    /// The summed delta of one category.
    pub fn total(&self, category: Category) -> Delta {
        let mut total = Delta::default();
        for row in self.files.iter().filter(|r| r.category == category) {
            total.add(row.delta);
        }
        total
    }
}

/// Why the meter printed no number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MeterError {
    /// The working tree has uncommitted or untracked paths; the number would not include them.
    Dirty(Vec<String>),
    /// A subprocess or parse failed; the message names it.
    Failed(String),
}

impl std::fmt::Display for MeterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MeterError::Dirty(paths) => {
                writeln!(
                    f,
                    "REFUSED: the working tree is dirty; the number would not include:"
                )?;
                for p in paths {
                    writeln!(f, "  {p}")?;
                }
                Ok(())
            }
            MeterError::Failed(msg) => write!(f, "{msg}"),
        }
    }
}

/// What to measure.
#[derive(Clone, Debug)]
pub struct Options {
    /// Repository root (any path inside the repo works for the CLI; the library wants the root).
    pub repo: PathBuf,
    /// The integration branch; the measurement starts at `merge-base(base, head)`.
    pub base: String,
    /// The commit measured.
    pub head: String,
}

/// Measure `merge-base(base, head)..head`. Refuses on a dirty working tree.
pub fn measure(opts: &Options) -> Result<Report, MeterError> {
    let repo = opts.repo.as_path();
    let dirty = git::dirty_paths(repo)?;
    if !dirty.is_empty() {
        return Err(MeterError::Dirty(dirty));
    }
    let base = git::rev_parse(repo, &opts.base)?;
    let head = git::rev_parse(repo, &opts.head)?;
    let merge_base = git::merge_base(repo, &base, &head)?;
    let old_side = Side::load(repo, &merge_base)?;
    let new_side = Side::load(repo, &head)?;
    let mut files = Vec::new();
    for change in git::name_status(repo, &merge_base, &head)? {
        bill(&old_side, &new_side, &change, &mut files)?;
    }
    Ok(Report {
        base: merge_base,
        head,
        files,
    })
}

fn bill(
    old_side: &Side,
    new_side: &Side,
    change: &git::Change,
    files: &mut Vec<FileRow>,
) -> Result<(), MeterError> {
    let old = change
        .old
        .as_deref()
        .map(|p| old_side.texts(Path::new(p)))
        .transpose()?;
    let new = change
        .new
        .as_deref()
        .map(|p| new_side.texts(Path::new(p)))
        .transpose()?;
    let category = match (&change.new, &change.old) {
        (Some(p), _) => new_side.classify(Path::new(p)),
        (None, Some(p)) => old_side.classify(Path::new(p)),
        (None, None) => return Ok(()),
    };
    let counted = git::numstat(
        old.as_ref().map(|t| t.counted.as_str()),
        new.as_ref().map(|t| t.counted.as_str()),
    )?;
    files.push(FileRow {
        category,
        delta: counted,
        old: change.old.clone(),
        new: change.new.clone(),
        cfg_off: false,
    });
    let old_off = old
        .as_ref()
        .map(|t| t.cfg_off.as_str())
        .filter(|s| !s.is_empty());
    let new_off = new
        .as_ref()
        .map(|t| t.cfg_off.as_str())
        .filter(|s| !s.is_empty());
    if old_off.is_some() || new_off.is_some() {
        files.push(FileRow {
            category: Category::Tests,
            delta: git::numstat(old_off, new_off)?,
            old: change.old.clone(),
            new: change.new.clone(),
            cfg_off: true,
        });
    }
    Ok(())
}
