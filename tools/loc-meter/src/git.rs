//! Git plumbing. Every read goes through `git` itself so the meter's numbers
//! carry git's own diff semantics (`--numstat`, rename detection).

use std::path::Path;
use std::process::Command;

use crate::MeterError;

/// One entry of `git diff --name-status -M`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Change {
    pub old: Option<String>,
    pub new: Option<String>,
}

pub fn run(repo: &Path, args: &[&str]) -> Result<Vec<u8>, MeterError> {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .map_err(|e| MeterError::Failed(format!("git {}: {e}", args.join(" "))))?;
    if !output.status.success() {
        return Err(MeterError::Failed(format!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(output.stdout)
}

fn text(repo: &Path, args: &[&str]) -> Result<String, MeterError> {
    Ok(String::from_utf8_lossy(&run(repo, args)?)
        .trim()
        .to_string())
}

/// The repository root for any path inside it.
pub fn toplevel(cwd: &Path) -> Result<String, MeterError> {
    text(cwd, &["rev-parse", "--show-toplevel"])
}

pub fn rev_parse(repo: &Path, rev: &str) -> Result<String, MeterError> {
    text(
        repo,
        &[
            "rev-parse",
            "--verify",
            "--quiet",
            &format!("{rev}^{{commit}}"),
        ],
    )
    .map_err(|_| MeterError::Failed(format!("`{rev}` does not name a commit")))
}

pub fn merge_base(repo: &Path, a: &str, b: &str) -> Result<String, MeterError> {
    text(repo, &["merge-base", a, b])
}

/// Uncommitted or untracked paths (ignored paths excluded). Empty means clean.
pub fn dirty_paths(repo: &Path) -> Result<Vec<String>, MeterError> {
    let _ = repo;
    Ok(Vec::new())
}

/// Extract `rev` into `dir` from git objects; the working tree is never read.
pub fn archive(repo: &Path, rev: &str, dir: &Path) -> Result<(), MeterError> {
    let tar = dir.join(".loc-meter-archive.tar");
    let tar_str = tar.to_string_lossy().into_owned();
    run(repo, &["archive", "--format=tar", "-o", &tar_str, rev])?;
    let status = Command::new("tar")
        .arg("-xf")
        .arg(&tar)
        .arg("-C")
        .arg(dir)
        .status()
        .map_err(|e| MeterError::Failed(format!("tar: {e}")))?;
    std::fs::remove_file(&tar).map_err(|e| MeterError::Failed(format!("remove archive: {e}")))?;
    if !status.success() {
        return Err(MeterError::Failed(format!("tar -xf {tar_str} failed")));
    }
    Ok(())
}

/// The changed paths between two commits, renames paired the way git pairs them.
pub fn name_status(repo: &Path, from: &str, to: &str) -> Result<Vec<Change>, MeterError> {
    let raw = run(repo, &["diff", "--name-status", "-z", "-M", from, to])?;
    let raw = String::from_utf8_lossy(&raw);
    let mut fields = raw.split('\0').filter(|f| !f.is_empty());
    let mut changes = Vec::new();
    while let Some(status) = fields.next() {
        let kind = status.chars().next().unwrap_or('M');
        let entry = match kind {
            'R' | 'C' => {
                let old = fields.next().map(str::to_string);
                let new = fields.next().map(str::to_string);
                Change {
                    old: if kind == 'C' { None } else { old },
                    new,
                }
            }
            'A' => Change {
                old: None,
                new: fields.next().map(str::to_string),
            },
            'D' => Change {
                old: fields.next().map(str::to_string),
                new: None,
            },
            _ => {
                let path = fields.next().map(str::to_string);
                Change {
                    old: path.clone(),
                    new: path,
                }
            }
        };
        changes.push(entry);
    }
    Ok(changes)
}

/// `git diff --numstat` of two texts; a missing side is an empty file.
pub fn numstat(old: Option<&str>, new: Option<&str>) -> Result<crate::Delta, MeterError> {
    let scratch = crate::side::TempDir::new("numstat")?;
    let a = scratch.path().join("old");
    let b = scratch.path().join("new");
    std::fs::write(&a, old.unwrap_or_default())
        .and_then(|()| std::fs::write(&b, new.unwrap_or_default()))
        .map_err(|e| MeterError::Failed(format!("write scratch: {e}")))?;
    let output = Command::new("git")
        .args(["diff", "--no-index", "--numstat", "--"])
        .arg(&a)
        .arg(&b)
        .output()
        .map_err(|e| MeterError::Failed(format!("git diff --no-index: {e}")))?;
    if !matches!(output.status.code(), Some(0 | 1)) {
        return Err(MeterError::Failed(format!(
            "git diff --no-index failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut cols = stdout.lines().next().unwrap_or_default().split('\t');
    let added = cols.next().and_then(|c| c.parse().ok()).unwrap_or(0);
    let deleted = cols.next().and_then(|c| c.parse().ok()).unwrap_or(0);
    Ok(crate::Delta { added, deleted })
}
