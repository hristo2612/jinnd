//! The pin that every shipped contract bundle (`contracts/*/metadata.toml`)
//! is well formed, read by a REAL TOML parser.
//!
//! M2-K19. A bundle is a CONTRACT OF RECORD, and for a long time nothing in
//! this workspace parsed one: every assertion about a bundle was
//! `bundle.contains("…")`. That let a malformed header — `[equality].`, an
//! edit that broke `[operations.write]` in half — ship inside a valid file,
//! while the test meant to catch it passed on a COMMENT that happened to
//! mention `[recovery]`.
//!
//! Round 2 replaced the substring with a hand-written reader, which was the
//! same hole one level deeper: a hand-maintained copy of a GRAMMAR instead
//! of a hand-maintained copy of a value. It checked the glyph set of a
//! header rather than its shape, so it accepted `[recovery..policy]` — a
//! dotted key with an empty segment that TOML rejects.
//!
//! So the grammar comes from the crate that defines it. `toml` is a
//! DEV-dependency (R10, COO round-2 ruling, recorded so it is not
//! re-litigated): the bundles ship as `include_str!` constants and nothing
//! in the kernel's hot path parses TOML at runtime, so this is a test
//! concern and adds ZERO runtime surface. `DeTable::parse` is the crate's
//! serde-free document API, so no serializer, no `serde`, no derive.

use toml::de::{DeTable, DeValue};

/// Every bundle the kernel ships, by the directory that holds it.
const SHIPPED: [(&str, &str); 9] = [
    (
        "jinn-auth",
        include_str!("../../../contracts/jinn-auth/metadata.toml"),
    ),
    (
        "jinn-clock",
        include_str!("../../../contracts/jinn-clock/metadata.toml"),
    ),
    (
        "jinn-fs",
        include_str!("../../../contracts/jinn-fs/metadata.toml"),
    ),
    (
        "jinn-introspect",
        include_str!("../../../contracts/jinn-introspect/metadata.toml"),
    ),
    (
        "jinn-keystore",
        include_str!("../../../contracts/jinn-keystore/metadata.toml"),
    ),
    (
        "jinn-ledger",
        include_str!("../../../contracts/jinn-ledger/metadata.toml"),
    ),
    (
        "jinn-net",
        include_str!("../../../contracts/jinn-net/metadata.toml"),
    ),
    (
        "jinn-process",
        include_str!("../../../contracts/jinn-process/metadata.toml"),
    ),
    (
        "jinn-profile",
        include_str!("../../../contracts/jinn-profile/metadata.toml"),
    ),
];

/// The value under `key` in `table`, whatever its type.
///
/// `DeTable` keys are spanned, so they are compared through their text
/// rather than looked up by a borrowed key.
fn entry<'a, 'i>(table: &'a DeTable<'i>, key: &str) -> Option<&'a DeValue<'i>> {
    table
        .iter()
        .find(|(name, _)| name.get_ref().as_ref() == key)
        .map(|(_, value)| value.get_ref())
}

/// The string a dotted path names, or `None` if the path is absent or does
/// not end at a string. Written against the parsed document, so
/// `"operations.write.commit"` walks three real tables — it cannot be
/// satisfied by a header that merely spells that text.
fn string_at<'a>(root: &'a DeTable<'_>, path: &str) -> Option<&'a str> {
    let mut segments = path.split('.').peekable();
    let mut table = root;
    while let Some(segment) = segments.next() {
        let value = entry(table, segment)?;
        if segments.peek().is_none() {
            return value.as_str();
        }
        table = value.as_table()?;
    }
    None
}

/// The class pin. A contract of record that does not parse is not a
/// contract, and no amount of care at review time has caught this shape.
/// Now the TOML crate reads them all.
///
/// `include_str!` needs literal paths, so the list above is written by
/// hand — which would make THIS assertion the next one that cannot fire,
/// the moment a ninth bundle lands unlisted. So the list is checked
/// against the directory rather than trusted.
#[test]
fn every_shipped_contract_bundle_is_well_formed() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../contracts");
    let mut on_disk: Vec<String> = std::fs::read_dir(&dir)
        .unwrap_or_else(|refused| panic!("read {}: {refused}", dir.display()))
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.join("metadata.toml").is_file())
        .filter_map(|path| path.file_name()?.to_str().map(str::to_owned))
        .collect();
    on_disk.sort();
    let mut listed: Vec<String> = SHIPPED.iter().map(|(name, _)| (*name).to_owned()).collect();
    listed.sort();
    assert_eq!(
        on_disk, listed,
        "every bundle on disk is read here: add it to SHIPPED"
    );

    for (name, bundle) in SHIPPED {
        if let Err(refused) = DeTable::parse(bundle) {
            panic!("contracts/{name}/metadata.toml: {refused}");
        }
    }
}

/// The pin earns its keep only if it REFUSES, so these are the shapes that
/// got past its two predecessors — the header that actually shipped, and
/// the empty dotted segment the hand-written reader waved through.
///
/// Only refusal is asserted, never the wording: pinning another crate's
/// error prose would be one more hand-maintained copy of someone else's
/// grammar, which is the defect this test exists to end.
#[test]
fn the_parser_refuses_the_shapes_that_got_past_a_substring_and_a_hand_reader() {
    for (shape, bundle) in [
        (
            "a trailing dot after a header",
            "[equality].\nkey = \"v\"\n",
        ),
        (
            "an empty dotted segment",
            "[recovery..policy]\non-failure = \"refuse-open\"\n",
        ),
        ("a key with no `=`", "[a]\nkey \"v\"\n"),
        ("a bare unquoted value", "[a]\nkey = unquoted\n"),
        ("trailing junk after a value", "[a]\nkey = \"v\" junk\n"),
        ("a key set twice", "[a]\nkey = \"v\"\nkey = \"w\"\n"),
        ("a table declared twice", "[a]\n[a]\n"),
    ] {
        assert!(
            DeTable::parse(bundle).is_err(),
            "{shape} is refused: {bundle:?}"
        );
    }
    // …and the shape the bundles are actually written in still reads.
    let read = DeTable::parse("# note\n[a.b]\nk = \"v\"   # why\n")
        .unwrap_or_else(|refused| panic!("the shipped shape parses: {refused}"));
    assert_eq!(string_at(read.get_ref(), "a.b.k"), Some("v"));
}

/// A sweep is only sound if the suffix is the kernel's alone, and the
/// bundle is where that reservation is of record. Read as STRUCTURE, not
/// as a substring: round 1 asserted `bundle.contains("[recovery]")` and it
/// passed on a COMMENT, over a file whose `[operations.write]` table had
/// been broken in half by the very edit under test.
#[test]
fn the_bundle_states_what_becomes_of_a_stage_file_whose_rename_never_came() {
    let bundle = include_str!("../../../contracts/jinn-fs/metadata.toml");
    let document = DeTable::parse(bundle)
        .unwrap_or_else(|refused| panic!("the fs bundle is well formed: {refused}"));
    let fs = document.get_ref();
    assert_eq!(
        string_at(fs, "recovery.staged"),
        Some("<name>.jinnd-stage"),
        "the reserved staging name is declared in a [recovery] TABLE, not described in prose"
    );
    assert_eq!(
        string_at(fs, "recovery.policy"),
        Some("sweep-staged-on-open-never-adopt"),
        "a staged file whose rename never came is deleted, never adopted"
    );
    assert_eq!(
        string_at(fs, "recovery.on-failure"),
        Some("refuse-open"),
        "the sweep is absolute, and the contract says so in those words"
    );
    // The table the round-1 edit destroyed: it declares the commit shape
    // that reserves the staging name in the first place.
    assert_eq!(
        string_at(fs, "operations.write.commit"),
        Some("stage-fsync-rename"),
        "the commit shape that reserves the name is intact"
    );
}

/// M2-K15 (R12): the TLS decision and the version that carries it are read
/// out of the net bundle BY PARSING — `[contract].version`, and the
/// `[tls]` table's three recorded choices as real keys in a real table.
///
/// Not `contains`. Five instances of the unanchored-substring vacuity have
/// shipped in this program: a `contains("0.3.0")` passes on a comment, on
/// a changelog line, on another contract's version, and would pass on a
/// bundle whose `[tls]` header was broken in half. `string_at` walks the
/// document, so an absent table and a malformed one both fail.
#[test]
fn the_net_bundle_records_the_tls_decision_at_its_new_version() {
    let bundle = SHIPPED
        .iter()
        .find(|(name, _)| *name == "jinn-net")
        .map(|(_, text)| *text)
        .unwrap_or_else(|| panic!("the net bundle ships"));
    let parsed =
        DeTable::parse(bundle).unwrap_or_else(|refused| panic!("the net bundle parses: {refused}"));
    let net = parsed.get_ref();
    for (path, value) in [
        ("contract.name", "jinn:net"),
        ("contract.version", "0.3.0"),
        // The one design decision this packet was allowed, of record.
        ("tls.stack", "rustls-ring-over-tokio-rustls"),
        (
            "tls.anchors",
            "vendored-public-roots-never-the-platform-store",
        ),
        ("tls.verify", "always-on-no-off-switch"),
        // Law 3 is UNCHANGED by the new transport: both doors, still
        // irreversible, re-read at 0.3.0 rather than assumed to carry.
        ("operations.request.effect", "irreversible"),
        ("operations.send-request.effect", "irreversible"),
    ] {
        assert_eq!(
            string_at(net, path),
            Some(value),
            "contracts/jinn-net/metadata.toml {path}"
        );
    }
}
