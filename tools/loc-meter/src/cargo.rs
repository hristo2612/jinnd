//! The compiler's list of what it compiles: `cargo metadata --no-deps` gives
//! every workspace package, its `lib`/`bin` roots, and its feature graph.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

use crate::MeterError;

/// One workspace package as the non-test build sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Package {
    pub name: String,
    /// Directory of the manifest, relative to the tree root.
    pub root: PathBuf,
    /// Target root files compiled outside `--tests` (lib, bin, build script), relative to the tree root.
    pub roots: Vec<PathBuf>,
    /// Features enabled for this package in a `cargo build --workspace` (defaults + what workspace members ask for).
    pub features: BTreeSet<String>,
}

const COMPILED_KINDS: &[&str] = &[
    "lib",
    "rlib",
    "dylib",
    "cdylib",
    "staticlib",
    "proc-macro",
    "bin",
    "custom-build",
];

/// The packages of the workspace rooted at `tree`; empty when there is no manifest.
pub fn workspace(tree: &Path) -> Result<Vec<Package>, MeterError> {
    let manifest = tree.join("Cargo.toml");
    if !manifest.exists() {
        return Ok(Vec::new());
    }
    let output = Command::new("cargo")
        .args([
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--offline",
            "--manifest-path",
        ])
        .arg(&manifest)
        .output()
        .map_err(|e| MeterError::Failed(format!("cargo metadata: {e}")))?;
    if !output.status.success() {
        return Err(MeterError::Failed(format!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let doc: Value = serde_json::from_slice(&output.stdout)
        .map_err(|e| MeterError::Failed(format!("cargo metadata output: {e}")))?;
    let packages = doc["packages"].as_array().cloned().unwrap_or_default();
    let requested = requested_features(&packages);
    let mut out = Vec::new();
    for pkg in &packages {
        let name = pkg["name"].as_str().unwrap_or_default().to_string();
        let manifest_path = Path::new(pkg["manifest_path"].as_str().unwrap_or_default());
        let root = relative(tree, manifest_path.parent().unwrap_or(manifest_path));
        let roots = pkg["targets"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|t| {
                t["kind"].as_array().is_some_and(|kinds| {
                    kinds
                        .iter()
                        .any(|k| COMPILED_KINDS.contains(&k.as_str().unwrap_or_default()))
                })
            })
            .filter_map(|t| t["src_path"].as_str())
            .map(|p| relative(tree, Path::new(p)))
            .collect();
        let mut seeds: BTreeSet<String> = requested.get(&name).cloned().unwrap_or_default();
        if !seeds.remove("!no-default") {
            seeds.insert("default".to_string());
        }
        let features = closure(&pkg["features"], seeds);
        out.push(Package {
            name,
            root,
            roots,
            features,
        });
    }
    Ok(out)
}

/// Features that workspace members request on each other through normal or
/// build dependencies (dev-dependencies are test-only and ignored). The marker
/// `!no-default` records that every requester turned default features off.
fn requested_features(packages: &[Value]) -> BTreeMap<String, BTreeSet<String>> {
    let members: BTreeSet<&str> = packages.iter().filter_map(|p| p["name"].as_str()).collect();
    let mut wants: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut default_on: BTreeSet<String> = BTreeSet::new();
    for pkg in packages {
        for dep in pkg["dependencies"].as_array().into_iter().flatten() {
            let name = dep["name"].as_str().unwrap_or_default();
            if !members.contains(name) || dep["kind"].as_str() == Some("dev") {
                continue;
            }
            let entry = wants.entry(name.to_string()).or_default();
            for f in dep["features"].as_array().into_iter().flatten() {
                if let Some(f) = f.as_str() {
                    entry.insert(f.to_string());
                }
            }
            if dep["uses_default_features"].as_bool().unwrap_or(true) {
                default_on.insert(name.to_string());
            }
        }
    }
    for (name, set) in wants.iter_mut() {
        if !default_on.contains(name) {
            set.insert("!no-default".to_string());
        }
    }
    wants
}

/// Expand feature names through the `[features]` table. `dep:` and `crate/feature`
/// entries enable nothing the meter can see and are dropped.
fn closure(table: &Value, seeds: BTreeSet<String>) -> BTreeSet<String> {
    let mut done = BTreeSet::new();
    let mut todo: Vec<String> = seeds.into_iter().collect();
    while let Some(f) = todo.pop() {
        if !done.insert(f.clone()) {
            continue;
        }
        for implied in table[&f].as_array().into_iter().flatten() {
            if let Some(name) = implied
                .as_str()
                .filter(|n| !n.starts_with("dep:") && !n.contains('/'))
            {
                todo.push(name.to_string());
            }
        }
    }
    done
}

fn relative(tree: &Path, path: &Path) -> PathBuf {
    path.strip_prefix(tree)
        .map(Path::to_path_buf)
        .unwrap_or_else(|_| path.to_path_buf())
}
