//! The three gates of M2-K16, each run as a SWEEP over every contract of
//! record on disk, and each proven to FIRE against the instance that
//! shipped. A gate whose first run finds nothing on a codebase that just
//! produced six instances deserves suspicion, so every sweep prints what it
//! swept.

use std::collections::BTreeMap;

use crate::{Contract, bundles, contract_files, facade, refs, rows, scan, world};

/// Every declared package identity: each `.wit` under `wit/` and
/// `contracts/`, parsed. The metadata's `[contract]` table must agree with
/// the WIT beside it — the third copy of the same fact.
fn declared_versions() -> BTreeMap<String, String> {
    let mut declared = BTreeMap::new();
    let world = world().wit();
    declared.insert(world.package_name(), world.version());
    for bundle in bundles() {
        let wit = bundle.wit().wit();
        let metadata = bundle.metadata().metadata();
        assert_eq!(
            (metadata.name(), metadata.version()),
            (wit.package_name(), wit.version()),
            "contracts/{}: [contract] names what contract.wit declares",
            bundle.name()
        );
        declared.insert(wit.package_name(), wit.version());
    }
    declared
}

#[test]
fn every_bundle_parses_and_its_three_identity_copies_agree() {
    let declared = declared_versions();
    println!("declared packages: {declared:?}");
    assert!(declared.len() >= 10, "{declared:?}");
}

/// GATE 1 sweep.
#[test]
fn every_package_reference_in_the_contracts_names_the_declared_version() {
    let declared = declared_versions();
    let mut swept = 0;
    let mut unknown = Vec::new();
    let mut found = Vec::new();
    for file in contract_files() {
        for reference in refs::references(file.text()) {
            swept += 1;
            if !declared.contains_key(&reference.package) {
                unknown.push(format!(
                    "{}:{}: {}@{}",
                    file.path(),
                    reference.line,
                    reference.package,
                    reference.version
                ));
            }
        }
        found.extend(refs::disagreements(file.path(), file.text(), &declared));
    }
    println!(
        "gate 1: swept {swept} package references across {} files",
        contract_files().len()
    );
    println!("gate 1: {} disagreements", found.len());
    for disagreement in &found {
        println!("  {disagreement}");
    }
    assert!(
        unknown.is_empty(),
        "references to packages nobody declares: {unknown:#?}"
    );
    assert!(found.is_empty(), "{found:#?}");
}

/// GATE 1 red-first: instance one, restored verbatim in a fixture.
#[test]
fn gate_one_fires_on_the_title_line_that_shipped() {
    let fixture = Contract::from_text(
        "wit/plugin.wit",
        "/// jinn:plugin@0.7.0 — the Tier A plugin world (M1-P8; constitution 01, R7, R12).\n\
         package jinn:plugin@0.8.0;\n\
         interface types { record refused-target { entry: string } }\n",
    );
    let wit = fixture.wit();
    let declared = BTreeMap::from([(wit.package_name(), wit.version())]);
    let found = refs::disagreements(fixture.path(), fixture.text(), &declared);
    assert_eq!(
        found,
        vec![refs::Disagreement {
            path: "wit/plugin.wit".to_owned(),
            line: 1,
            package: "jinn:plugin".to_owned(),
            found: "0.7.0".to_owned(),
            declared: "0.8.0".to_owned(),
        }]
    );
}

/// GATE 2 sweep.
#[test]
fn every_row_shape_the_contracts_enumerate_is_the_facades() {
    let facade = facade::ledger_event_kinds();
    assert!(facade.contains_key("NetRequested"), "{facade:?}");
    let mut swept = 0;
    let mut found = Vec::new();
    for file in contract_files() {
        for mention in rows::mentions(file.text()) {
            swept += 1;
            println!(
                "  {}:{}: {} {{ {} }}",
                file.path(),
                mention.line,
                mention.kind,
                mention.fields.join(", ")
            );
        }
        found.extend(rows::disagreements(file.path(), file.text(), &facade));
    }
    println!(
        "gate 2: swept {swept} row mentions across {} files",
        contract_files().len()
    );
    println!("gate 2: {} disagreements", found.len());
    for disagreement in &found {
        println!("  {disagreement}");
    }
    assert!(found.is_empty(), "{found:#?}");
}

/// GATE 2 red-first: instance two, restored in a fixture — the row without
/// `effect`, as `contracts/jinn-net/metadata.toml` shipped it before 15019d3.
#[test]
fn gate_two_fires_on_the_row_shape_that_shipped() {
    let facade = facade::ledger_event_kinds();
    let fixture = "# The row is\n# `NetRequested { method, host, path, status, request_bytes,\n# response_bytes, duration_ms }` — the SHAPE of the call.\n";
    let found = rows::disagreements("contracts/jinn-net/metadata.toml", fixture, &facade);
    assert_eq!(found.len(), 1, "{found:#?}");
    assert_eq!(found[0].line, 2);
    assert_eq!(found[0].kind, "NetRequested");
    assert_eq!(found[0].found[0], "method");
    assert_eq!(
        found[0]
            .declared
            .as_deref()
            .and_then(|d| d.first().cloned())
            .as_deref(),
        Some("effect")
    );
}

/// GATE 2 red-first: a VALUE written where a FIELD belongs (the keystore
/// bundle's `{ get, key, digest }`), and a kind the facade never declared.
#[test]
fn gate_two_fires_on_a_value_for_a_field_and_on_an_unknown_kind() {
    let facade = facade::ledger_event_kinds();
    let value = "// Ledgered as `KeystoreAccessed { get, key, digest }`.\n";
    assert_eq!(rows::disagreements("x.wit", value, &facade).len(), 1);
    let unknown = "// Ledgered as `KeystoreTouched { operation, key, digest }`.\n";
    let found = rows::disagreements("x.wit", unknown, &facade);
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].declared, None);
    // The spellings the contracts legitimately use all pass: `field: value`,
    // a multi-line mention, and a stated prefix.
    for fine in [
        "// `LedgerConsumed { first: seq, last: seq, count: 0 }`\n",
        "# `NetRequested { effect, method, host, path, status, request_bytes,\n# response_bytes, duration_ms }`\n",
        "// `KeystoreAccessed { operation, ... }`\n",
    ] {
        assert_eq!(rows::disagreements("x", fine, &facade), vec![], "{fine}");
    }
}

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

/// The prose view sees comments only, so a declaration is invisible to it
/// and a comment cannot pass for a table (instance five, both directions).
#[test]
fn prose_is_comments_and_a_comment_is_not_a_table() {
    let toml = Contract::from_text("x.toml", "[equality]\n# a note that mentions [recovery]\n");
    assert!(toml.prose().states("[recovery]"));
    assert!(!toml.prose().states("[equality]"));
    assert!(!toml.metadata().has_table("recovery"));
    assert!(toml.metadata().has_table("equality"));
    let wit = Contract::from_text(
        "x.wit",
        "package a:b@0.1.0;\n/// the wait-cycle record\ninterface t { record wait-cycle { on: string } }\n",
    );
    assert!(!wit.prose().states("record wait-cycle {"));
    assert!(wit.prose().states("wait-cycle record"));
    assert_eq!(
        wit.wit().interface("t").record_fields("wait-cycle"),
        ["on: string"]
    );
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
