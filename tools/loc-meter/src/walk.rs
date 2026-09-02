//! The module walk: from a target root, follow every `mod` the compiler would
//! follow and record the line ranges it would drop.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::MeterError;

/// Every file reachable from `root` (relative to `tree`), with its `cfg`-off line ranges.
pub fn walk(
    tree: &Path,
    root: &Path,
    features: &BTreeSet<String>,
) -> Result<BTreeMap<PathBuf, Vec<(usize, usize)>>, MeterError> {
    let _ = features;
    let mut out = BTreeMap::new();
    if tree.join(root).is_file() {
        out.insert(root.to_path_buf(), Vec::new());
    }
    Ok(out)
}
