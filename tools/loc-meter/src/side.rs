//! One side of the measurement: a commit extracted from git objects, with the
//! compiler's view of which Rust files and items the non-test build contains.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{Category, MeterError, cargo, git, walk};

/// A scratch directory removed on drop.
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> Result<Self, MeterError> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let dir = std::env::temp_dir().join(format!(
            "loc-meter-{tag}-{}-{n}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir)
            .map_err(|e| MeterError::Failed(format!("mkdir {}: {e}", dir.display())))?;
        let dir = std::fs::canonicalize(&dir).unwrap_or(dir);
        Ok(TempDir(dir))
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The two texts a Rust file splits into: what the non-test build compiles and what it drops.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Texts {
    pub counted: String,
    pub cfg_off: String,
}

/// What the compiler knows about one compiled file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Compiled {
    /// Directory of the owning package's manifest, relative to the tree root.
    pub package_root: PathBuf,
    /// 1-based inclusive line ranges the non-test build drops.
    pub cfg_off: Vec<(usize, usize)>,
}

/// A commit, extracted, indexed by the compiler's view.
pub struct Side {
    tree: TempDir,
    compiled: BTreeMap<PathBuf, Compiled>,
}

impl Side {
    /// Extract `rev` and walk every `lib`/`bin` root the workspace declares.
    pub fn load(repo: &Path, rev: &str) -> Result<Self, MeterError> {
        let tree = TempDir::new("side")?;
        git::archive(repo, rev, tree.path())?;
        let mut compiled = BTreeMap::new();
        for package in cargo::workspace(tree.path())? {
            for root in &package.roots {
                for (file, cfg_off) in walk::walk(tree.path(), root, &package.features)? {
                    compiled.entry(file).or_insert(Compiled {
                        package_root: package.root.clone(),
                        cfg_off,
                    });
                }
            }
        }
        Ok(Side { tree, compiled })
    }

    /// The files the non-test build compiles, relative to the tree root.
    pub fn compiled_files(&self) -> impl Iterator<Item = &Path> {
        self.compiled.keys().map(PathBuf::as_path)
    }

    /// Which line a change to `path` is billed on.
    pub fn classify(&self, path: &Path) -> Category {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or_default();
        match ext {
            "md" => Category::Prose,
            "wit" | "toml" => Category::Contracts,
            "rs" if path.starts_with("tools") => Category::Tools,
            "rs" => match self.compiled.get(path) {
                Some(c) if is_facade(&c.package_root) => Category::Facade,
                Some(c) if c.package_root.starts_with("crates") => Category::Production,
                _ => Category::Tests,
            },
            _ => Category::Other,
        }
    }

    /// The counted and dropped texts of `path`; the whole file counts unless the compiler drops parts of it.
    pub fn texts(&self, path: &Path) -> Result<Texts, MeterError> {
        let full = self.tree.path().join(path);
        let bytes = std::fs::read(&full)
            .map_err(|e| MeterError::Failed(format!("read {}: {e}", path.display())))?;
        let content = String::from_utf8_lossy(&bytes);
        let Some(compiled) = self.compiled.get(path) else {
            return Ok(Texts {
                counted: content.into_owned(),
                cfg_off: String::new(),
            });
        };
        Ok(split(&content, &compiled.cfg_off))
    }
}

fn is_facade(package_root: &Path) -> bool {
    package_root == Path::new("crates/jinnd-api")
        || package_root == Path::new("crates/jinnd-adapter")
}

/// Split `content` into the lines outside `cfg_off` ranges and the lines inside them.
pub fn split(content: &str, cfg_off: &[(usize, usize)]) -> Texts {
    let mut texts = Texts::default();
    for (index, line) in content.split_inclusive('\n').enumerate() {
        let number = index + 1;
        let off = cfg_off
            .iter()
            .any(|&(start, end)| start <= number && number <= end);
        let target = if off {
            &mut texts.cfg_off
        } else {
            &mut texts.counted
        };
        target.push_str(line);
    }
    texts
}
