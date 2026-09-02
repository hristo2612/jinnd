//! The pin that every shipped contract bundle (`contracts/*/metadata.toml`
//! and `contracts/*/contract.wit`) is well formed, read by the REAL
//! parsers.
//!
//! M2-K19. A bundle is a CONTRACT OF RECORD, and for a long time nothing in
//! this workspace parsed one: every assertion about a bundle was a
//! substring. That let a malformed header — `[equality].`, an edit that
//! broke `[operations.write]` in half — ship inside a valid file, while the
//! test meant to catch it passed on a COMMENT that happened to mention
//! `[recovery]`. Round 2 replaced the substring with a hand-written reader,
//! which was the same hole one level deeper; the grammar now comes from the
//! crate that defines it (the M2-K19 round-3 ruling), through
//! `jinnd_contract_lens` (M2-K16), which is also where the bundles are
//! enumerated — off the directory, never a hand-kept list.
//!
//! M2-K16 extended the pin to the WIT beside each bundle: parsing found
//! `contracts/jinn-introspect/contract.wit` had never been readable by the
//! toolchain (a keyword used as a field name, a record and a function
//! sharing one name), guarded by substrings that could not see it.

use jinnd_contract_lens::bundle;

/// The class pin. A contract of record that does not parse is not a
/// contract, and no amount of care at review time has caught this shape.
/// Now the parsers read them all: every `metadata.toml` as TOML, every
/// `contract.wit` as WIT, and the three copies of each bundle's identity
/// (the WIT `package`, `[contract].name` and `[contract].version`) agree.
#[test]
fn every_shipped_contract_bundle_is_well_formed() {
    let shipped = jinnd_contract_lens::bundles();
    assert!(shipped.len() >= 9, "{} bundles on disk", shipped.len());
    for bundle in &shipped {
        let wit = bundle.wit().wit();
        let metadata = bundle.metadata().metadata();
        assert_eq!(
            (metadata.name(), metadata.version()),
            (wit.package_name(), wit.version()),
            "contracts/{}: [contract] names what contract.wit declares",
            bundle.name()
        );
    }
}

/// A sweep is only sound if the suffix is the kernel's alone, and the
/// bundle is where that reservation is of record. Read as STRUCTURE, not
/// as a substring: round 1 asserted `[recovery]` as text and it passed on
/// a COMMENT, over a file whose `[operations.write]` table had been broken
/// in half by the very edit under test.
#[test]
fn the_bundle_states_what_becomes_of_a_stage_file_whose_rename_never_came() {
    let fs = bundle("jinn-fs").metadata().metadata();
    assert_eq!(
        fs.string_at("recovery.staged").as_deref(),
        Some("<name>.jinnd-stage"),
        "the reserved staging name is declared in a [recovery] TABLE, not described in prose"
    );
    assert_eq!(
        fs.string_at("recovery.policy").as_deref(),
        Some("sweep-staged-on-open-never-adopt"),
        "a staged file whose rename never came is deleted, never adopted"
    );
    assert_eq!(
        fs.string_at("recovery.on-failure").as_deref(),
        Some("refuse-open"),
        "the sweep is absolute, and the contract says so in those words"
    );
    // The table the round-1 edit destroyed: it declares the commit shape
    // that reserves the staging name in the first place.
    assert_eq!(
        fs.string_at("operations.write.commit").as_deref(),
        Some("stage-fsync-rename"),
        "the commit shape that reserves the name is intact"
    );
}

/// M2-K15 (R12): the TLS decision and the version that carries it are read
/// out of the net bundle BY PARSING — `[contract].version`, and the
/// `[tls]` table's three recorded choices as real keys in a real table.
/// A substring for `0.3.0` passes on a comment, on a changelog line, on
/// another contract's version, and would pass on a bundle whose `[tls]`
/// header was broken in half; a walk of the document fails on all of them.
#[test]
fn the_net_bundle_records_the_tls_decision_at_its_new_version() {
    let net = bundle("jinn-net").metadata().metadata();
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
            net.string_at(path).as_deref(),
            Some(value),
            "contracts/jinn-net/metadata.toml {path}"
        );
    }
}
