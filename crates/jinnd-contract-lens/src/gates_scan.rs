//! Gate 3 of M2-K16 as a sweep over the workspace's Rust sources, plus the
//! R10 pin that this crate is only ever a dev-dependency — both read off
//! the tree, never a hand-kept list.

use crate::{Contract, scan};

/// GATE 3 sweep: no Rust source outside this crate loads a contract file.
#[test]
fn no_source_outside_the_lens_reads_a_contract_file() {
    let root = crate::repo_root();
    let mut sources = Vec::new();
    for top in ["crates", "tests", "demo", "fixtures"] {
        rust_sources(&root.join(top), &mut sources);
    }
    sources.sort();
    assert!(
        sources.len() > 50,
        "the walk found the workspace: {}",
        sources.len()
    );
    let mut found = Vec::new();
    for source in &sources {
        let relative = source
            .strip_prefix(&root)
            .unwrap_or_else(|_| panic!("{} lies under the repository", source.display()))
            .to_string_lossy()
            .replace('\\', "/");
        let text = std::fs::read_to_string(source)
            .unwrap_or_else(|refused| panic!("read {relative}: {refused}"));
        found.extend(scan::offences(&relative, &text));
    }
    println!("gate 3: scanned {} Rust sources", sources.len());
    println!("gate 3: {} offences", found.len());
    for offence in &found {
        println!("  {offence}");
    }
    assert!(found.is_empty(), "{found:#?}");
}

fn rust_sources(dir: &std::path::Path, into: &mut Vec<std::path::PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.filter_map(Result::ok) {
        let path = entry.path();
        let name = entry.file_name();
        if path.is_dir() {
            if name != "target" {
                rust_sources(&path, into);
            }
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            into.push(path);
        }
    }
}

/// GATE 3 red-first: the line that opened every one of the six instances.
#[test]
fn gate_three_fires_on_the_shape_that_shipped_six_times() {
    let shipped = "const WORLD: &str = include_str!(\"../../../../wit/plugin.wit\");\n\
                   const META: &str = include_str!(\"../../../../contracts/jinn-net/metadata.toml\");\n\
                   let bundle = std::fs::read_to_string(\"contracts/jinn-fs/metadata.toml\");\n";
    let found = scan::offences("crates/jinnd-wasm/src/bindings/tests.rs", shipped);
    assert_eq!(
        found.iter().map(|o| o.line).collect::<Vec<_>>(),
        vec![1, 2, 3],
        "{found:#?}"
    );
    // The lens itself is the one sanctioned reader.
    assert_eq!(
        scan::offences("crates/jinnd-contract-lens/src/lib.rs", shipped),
        vec![]
    );
    // A comment that mentions the form, a Rust source read, and a fixture
    // that is not a contract are all fine.
    let fine = "// once `include_str!(\"../../wit/plugin.wit\")` was how it was done\n\
                const LEDGER: &str = include_str!(\"../../jinnd-api/src/ledger.rs\");\n\
                let profile = include_str!(\"../fixtures/profile.json\");\n";
    assert_eq!(scan::offences("crates/x/src/y.rs", fine), vec![]);
}

/// R10: this crate is only ever a dev-dependency. Read from every
/// workspace manifest, PARSED.
#[test]
fn the_lens_is_only_ever_a_dev_dependency() {
    let root = crate::repo_root();
    let workspace = Contract::load("Cargo.toml").metadata();
    let mut members = 0;
    for member in glob_members(&root) {
        let manifest = Contract::load(&format!("{member}/Cargo.toml")).metadata();
        members += 1;
        assert!(
            !manifest.has_key("dependencies.jinnd-contract-lens")
                && !manifest.has_key("build-dependencies.jinnd-contract-lens"),
            "{member}: jinnd-contract-lens is a [dev-dependencies] entry only (R10)"
        );
    }
    assert!(members >= 12, "{members} manifests read");
    assert!(workspace.has_key("workspace.members"));
}

fn glob_members(root: &std::path::Path) -> Vec<String> {
    let mut members = Vec::new();
    for dir in ["crates", "tests"] {
        let Ok(entries) = std::fs::read_dir(root.join(dir)) else {
            continue;
        };
        for entry in entries.filter_map(Result::ok) {
            if entry.path().join("Cargo.toml").is_file() {
                members.push(format!("{dir}/{}", entry.file_name().to_string_lossy()));
            }
        }
    }
    members.sort();
    members
}
