//! The three gates of M2-K16, each run as a SWEEP over every contract of
//! record on disk, and each proven to FIRE against the instance that
//! shipped. A gate whose first run finds nothing on a codebase that just
//! produced six instances deserves suspicion, so every sweep prints what it
//! swept.

use std::collections::BTreeMap;

use crate::{Contract, bundles, contract_files, facade, refs, rows, world};

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
    let mut found = Vec::new();
    for file in contract_files() {
        swept += refs::candidates(file.text()).len();
        found.extend(refs::disagreements(file.path(), file.text(), &declared));
    }
    println!(
        "gate 1: swept {swept} package-reference candidates across {} files",
        contract_files().len()
    );
    println!("gate 1: {} disagreements", found.len());
    for disagreement in &found {
        println!("  {disagreement}");
    }
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
        vec![refs::Disagreement::Version {
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
        for candidate in rows::candidates(file.text()) {
            swept += 1;
            if let rows::Candidate::Read(mention) = candidate {
                println!(
                    "  {}:{}: {} {{ {} }}",
                    file.path(),
                    mention.line,
                    mention.kind,
                    mention.fields.join(", ")
                );
            }
        }
        found.extend(rows::disagreements(file.path(), file.text(), &facade));
    }
    println!(
        "gate 2: swept {swept} row-mention candidates across {} files",
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
    let [
        rows::Disagreement::Shape {
            line,
            kind,
            found: stated,
            declared: Some(declared),
            ..
        },
    ] = found.as_slice()
    else {
        panic!("{found:#?}");
    };
    assert_eq!((*line, kind.as_str()), (2, "NetRequested"));
    assert_eq!(
        (stated[0].as_str(), declared[0].as_str()),
        ("method", "effect")
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
    assert!(
        matches!(
            found.as_slice(),
            [rows::Disagreement::Shape { declared: None, .. }]
        ),
        "{found:#?}"
    );
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

/// GATE 1 fails CLOSED (round-2 ruling 1a): a token that looks like a
/// package reference but does not read as one is REPORTED at its line,
/// never skipped — a skipping reader is the vacuity in a new place.
#[test]
fn gate_one_refuses_a_reference_it_cannot_read() {
    let declared = BTreeMap::from([("jinn:plugin".to_owned(), "0.10.0".to_owned())]);
    let fixture = "// jinn:plugin@0.10 — a two-part version\n\
                   // jinn:plugin@0.10.0-rc1 — trailing junk\n\
                   // a URL with userinfo: http://user:pw@host/\n\
                   // jinn:plugin@0.10.0 reads, and agrees\n";
    let found = refs::disagreements("x.wit", fixture, &declared);
    assert_eq!(
        where_found(&found),
        ["x.wit:1", "x.wit:2", "x.wit:3"],
        "{found:#?}"
    );
}

/// GATE 2 fails CLOSED (round-2 ruling 1a): a `Kind {` that does not read
/// as a row is REPORTED at its line, never skipped.
#[test]
fn gate_two_refuses_a_row_it_cannot_read() {
    let facade = facade::ledger_event_kinds();
    let fixture = "// `NetRequested { effect method, host }` — no comma\n\
                   // `NetRequested { effect, ..., host }` — the tail is not last\n\
                   // `NetRequested { effect, ... }` reads, and agrees\n\
                   // `NetRequested { effect, method\n";
    let found = rows::disagreements("x.md", fixture, &facade);
    assert_eq!(
        where_found(&found),
        ["x.md:1", "x.md:2", "x.md:4"],
        "{found:#?}"
    );
}

/// `path:line` of every reported disagreement, whatever its kind.
fn where_found<D: std::fmt::Display>(found: &[D]) -> Vec<String> {
    found
        .iter()
        .map(|d| {
            d.to_string()
                .split(": ")
                .next()
                .unwrap_or_default()
                .to_owned()
        })
        .collect()
}

/// A phrase binds to ONE named item's doc block (round-2 ruling 1b): an
/// unrelated comment elsewhere in the file satisfies nothing, a declaration
/// is never prose, and a comment naming a table is not one (instance five).
#[test]
fn a_doc_block_binds_to_its_item_and_a_comment_is_not_a_table() {
    let wit = Contract::from_text(
        "x.wit",
        "package a:b@0.1.0;\n\
         interface t {\n\
           /// the wait-cycle record, stated only-here\n\
           record wait-cycle { on: string }\n\
           // elsewhere: a plain comment about walk()\n\
           walk: func() -> wait-cycle;\n\
         }\n",
    )
    .wit();
    let t = wit.interface("t");
    assert!(t.type_docs("wait-cycle").states("only-here"));
    assert!(!t.func_docs("walk").states("only-here"));
    assert!(t.func_docs("walk").states("about walk()"));
    assert!(!t.type_docs("wait-cycle").states("record wait-cycle {"));
    let toml = Contract::from_text("x.toml", "[equality]\n# a note that mentions [recovery]\n");
    assert!(!toml.metadata().has_table("recovery"));
    assert!(toml.metadata().has_table("equality"));
}
