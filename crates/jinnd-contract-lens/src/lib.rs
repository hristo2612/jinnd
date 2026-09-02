//! The structural lens over the contracts of record (M2-K16; R3, R12).
//!
//! Six shipped instances, three authors, one shape: a hand-maintained
//! second copy of a fact the code already knows, guarded by a check that
//! could not fail — a version in a title line nothing derives, a row shape
//! nobody cross-checks, a `contains` over contract text satisfied by a
//! comment. Attention did not prevent it; this crate is the mechanism.
//!
//! A contract file is asserted by PARSING it wherever a format exists: WIT
//! through `wit-parser`, the bundle metadata through `toml`, the facade
//! through `syn`. A test never sees the text — [`Contract`] exposes no
//! `&str` and no `contains`, only the parsed views ([`Contract::wit`],
//! [`Contract::metadata`]); a statement made in prose is asserted against
//! the doc block the parser attached to ONE named item
//! ([`wit::Docs`]), never against "any comment in the file". Where no
//! format exists — version references and row shapes written inside
//! comments are OUR convention — [`refs`] and [`rows`] are the one reader
//! each, with the grammar in their docs, and they FAIL CLOSED: a candidate
//! they cannot read is reported, never skipped. The scan in [`scan`] makes
//! loading a contract file anywhere else fail the build's tests, so the
//! unfirable shape is unexpressible rather than discouraged.
//!
//! Dev-only support crate (R10): only ever a `[dev-dependencies]` entry.

#![forbid(unsafe_code)]

pub mod facade;
pub mod index;
pub mod mentions;
pub mod metadata;
pub mod refs;
pub mod rows;
pub mod scan;
pub mod wit;

#[cfg(test)]
mod gates;
#[cfg(test)]
mod gates_index;
#[cfg(test)]
mod gates_scan;

use std::path::{Path, PathBuf};

/// The repository root, resolved from this crate's own manifest.
pub fn repo_root() -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    root.canonicalize().unwrap_or_else(|refused| {
        panic!("resolve the repository root {}: {refused}", root.display())
    })
}

/// One contract file as the lens loaded it. The text is private on
/// purpose: the only ways out are the parsed views and the prose view.
pub struct Contract {
    path: String,
    text: String,
}

impl Contract {
    /// Load a contract file by its repository-relative path, panicking
    /// with the path when it cannot be read.
    pub fn load(relative: &str) -> Contract {
        let full = repo_root().join(relative);
        let text = std::fs::read_to_string(&full)
            .unwrap_or_else(|refused| panic!("read {relative}: {refused}"));
        Contract {
            path: relative.to_owned(),
            text,
        }
    }

    /// A contract from text, for fixtures: the same views over a document
    /// that restores a shipped defect.
    pub fn from_text(path: &str, text: &str) -> Contract {
        Contract {
            path: path.to_owned(),
            text: text.to_owned(),
        }
    }

    /// The repository-relative path, for messages.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// The raw text — for the gate sweeps in this crate only. Nothing
    /// outside the lens can reach it, which is the whole point.
    #[cfg(test)]
    pub(crate) fn text(&self) -> &str {
        &self.text
    }

    /// The document PARSED as WIT; a document the toolchain cannot read
    /// panics here naming the file.
    pub fn wit(&self) -> wit::Wit {
        wit::Wit::parse(&self.path, &self.text)
    }

    /// The document PARSED as a TOML bundle; a malformed one panics here
    /// naming the file.
    pub fn metadata(&self) -> metadata::Metadata {
        metadata::Metadata::parse(&self.path, &self.text)
    }

    /// Every ledger row shape the document enumerates (`Kind { a, b }`),
    /// read under [`rows`]' grammar. Fails closed: a candidate the reader
    /// cannot read panics here naming the file and line.
    pub fn rows(&self) -> Vec<rows::RowMention> {
        rows::candidates(&self.text)
            .into_iter()
            .map(|candidate| match candidate {
                rows::Candidate::Read(mention) => mention,
                rows::Candidate::Unreadable { line, kind, why } => {
                    panic!("{}:{line}: {kind} {{ …: {why}", self.path)
                }
            })
            .collect()
    }

    /// Every `namespace:name@version` reference in the document, read
    /// under [`refs`]' grammar. Fails closed: a candidate the reader
    /// cannot read panics here naming the file and line.
    pub fn references(&self) -> Vec<refs::Reference> {
        refs::candidates(&self.text)
            .into_iter()
            .map(|candidate| match candidate {
                refs::Candidate::Read(reference) => reference,
                refs::Candidate::Unreadable { line, token } => {
                    panic!(
                        "{}:{line}: `{token}` does not read as a reference",
                        self.path
                    )
                }
            })
            .collect()
    }
}

/// The Tier A plugin world, `wit/plugin.wit`.
pub fn world() -> Contract {
    Contract::load("wit/plugin.wit")
}

/// One shipped contract bundle under `contracts/<name>/`.
pub struct Bundle {
    name: String,
}

impl Bundle {
    /// The bundle's directory name, `jinn-<contract>`.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// `contracts/<name>/contract.wit`.
    pub fn wit(&self) -> Contract {
        Contract::load(&format!("contracts/{}/contract.wit", self.name))
    }

    /// `contracts/<name>/metadata.toml`.
    pub fn metadata(&self) -> Contract {
        Contract::load(&format!("contracts/{}/metadata.toml", self.name))
    }

    /// `contracts/<name>/README.md`, where the bundle ships one.
    pub fn readme(&self) -> Option<Contract> {
        let relative = format!("contracts/{}/README.md", self.name);
        repo_root()
            .join(&relative)
            .is_file()
            .then(|| Contract::load(&relative))
    }
}

/// One shipped bundle by name; panics naming it when it does not exist.
pub fn bundle(name: &str) -> Bundle {
    let dir = repo_root().join("contracts").join(name);
    assert!(
        dir.join("contract.wit").is_file() && dir.join("metadata.toml").is_file(),
        "contracts/{name} ships a contract.wit and a metadata.toml"
    );
    Bundle {
        name: name.to_owned(),
    }
}

/// Every shipped bundle, read off the directory — never a hand-kept list,
/// which would be the next copy that drifts.
pub fn bundles() -> Vec<Bundle> {
    let dir = repo_root().join("contracts");
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|refused| panic!("read {}: {refused}", dir.display()))
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.join("metadata.toml").is_file())
        .filter_map(|path| path.file_name()?.to_str().map(str::to_owned))
        .collect();
    names.sort();
    assert!(!names.is_empty(), "contracts/ holds at least one bundle");
    names.into_iter().map(|name| Bundle { name }).collect()
}

/// Every contract-of-record file under `wit/` and `contracts/`: `.wit`,
/// `.toml` and `.md`, in path order.
pub fn contract_files() -> Vec<Contract> {
    let root = repo_root();
    let mut paths = Vec::new();
    for top in ["wit", "contracts"] {
        collect(&root.join(top), &mut paths);
    }
    paths.sort();
    paths
        .iter()
        .map(|path| {
            let relative = path
                .strip_prefix(&root)
                .unwrap_or_else(|_| panic!("{} lies under the repository", path.display()))
                .to_string_lossy()
                .replace('\\', "/");
            Contract::load(&relative)
        })
        .collect()
}

fn collect(dir: &Path, into: &mut Vec<PathBuf>) {
    let entries = std::fs::read_dir(dir)
        .unwrap_or_else(|refused| panic!("read {}: {refused}", dir.display()));
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        if path.is_dir() {
            collect(&path, into);
            continue;
        }
        let is_contract = path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| matches!(ext, "wit" | "toml" | "md"));
        if is_contract {
            into.push(path);
        }
    }
}
