//! A deliberately strict reader for the shipped contract bundles
//! (`contracts/*/metadata.toml`), and the pin that every one of them is
//! well formed.
//!
//! M2-K19 round 2. A bundle is a CONTRACT OF RECORD, and nothing in this
//! workspace ever parsed one: every assertion about a bundle was
//! `bundle.contains("…")`. That let a malformed header — `[equality].`,
//! an edit that broke `[operations.write]` in half — ship inside a valid
//! file, while the test meant to catch it passed on a COMMENT that
//! happened to mention `[recovery]`. A claim about STRUCTURE has to be
//! answered by structure, or it is a claim that cannot fail.
//!
//! This reader accepts the subset the bundles are written in — blank
//! lines, `#` comments, `[table]` headers, and `key = value` pairs whose
//! value is a quoted string or a bare integer — and REFUSES every line it
//! does not recognize. It is therefore stricter than TOML on purpose: a
//! bundle that starts using a TOML feature it does not know fails loudly
//! and this reader gets extended. It cannot fail the other way and accept
//! a header TOML would reject, which is the direction that matters here.

use std::collections::BTreeMap;

/// Table name -> key -> value text, in declaration-independent order.
pub(crate) type Tables = BTreeMap<String, BTreeMap<String, String>>;

/// Reads `bundle` into its tables, or says which line is not well formed.
pub(crate) fn tables(bundle: &str) -> Result<Tables, String> {
    let mut tables = Tables::new();
    let mut current: Option<String> = None;
    for (index, raw) in bundle.lines().enumerate() {
        let at = index + 1;
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') {
            let name = line
                .strip_prefix('[')
                .and_then(|rest| rest.strip_suffix(']'))
                .filter(|name| {
                    !name.is_empty()
                        && name
                            .chars()
                            .all(|glyph| glyph.is_ascii_alphanumeric() || "._-".contains(glyph))
                })
                .ok_or_else(|| format!("line {at}: malformed table header {line:?}"))?;
            if tables.insert(name.to_owned(), BTreeMap::new()).is_some() {
                return Err(format!("line {at}: table [{name}] is declared twice"));
            }
            current = Some(name.to_owned());
            continue;
        }
        let (key, rest) = line
            .split_once('=')
            .ok_or_else(|| format!("line {at}: neither a table header nor a key {line:?}"))?;
        let key = key.trim();
        if key.is_empty()
            || !key
                .chars()
                .all(|glyph| glyph.is_ascii_alphanumeric() || "_-".contains(glyph))
        {
            return Err(format!("line {at}: malformed key {key:?}"));
        }
        let (value, tail) = read_value(rest.trim())
            .ok_or_else(|| format!("line {at}: unreadable value for {key:?}"))?;
        if !(tail.is_empty() || tail.starts_with('#')) {
            return Err(format!("line {at}: trailing {tail:?} after {key:?}"));
        }
        let table = current
            .as_ref()
            .and_then(|name| tables.get_mut(name))
            .ok_or_else(|| format!("line {at}: {key:?} sits outside any table"))?;
        if table.insert(key.to_owned(), value).is_some() {
            return Err(format!("line {at}: {key:?} is set twice in the same table"));
        }
    }
    Ok(tables)
}

/// A quoted string or a bare non-negative integer, and whatever follows it.
fn read_value(rest: &str) -> Option<(String, &str)> {
    if let Some(body) = rest.strip_prefix('"') {
        let (text, tail) = body.split_once('"')?;
        return Some((text.to_owned(), tail.trim()));
    }
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() {
        return None;
    }
    Some((digits.clone(), rest[digits.len()..].trim()))
}

/// Every bundle the kernel ships, by the directory that holds it.
const SHIPPED: [(&str, &str); 8] = [
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

/// The class pin. A contract of record that does not parse is not a
/// contract, and no amount of care at review time has caught this shape —
/// this is the fifth instance. Now a machine reads them all.
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
        if let Err(refused) = tables(bundle) {
            panic!("contracts/{name}/metadata.toml: {refused}");
        }
    }
}

/// The reader earns its keep only if it REFUSES; these are the shapes it
/// exists to catch, including the exact one that shipped.
#[test]
fn the_reader_refuses_the_shapes_that_shipped_past_a_substring_assertion() {
    for (bundle, expected) in [
        ("[equality].\nkey = \"v\"\n", "malformed table header"),
        ("[a]\nkey \"v\"\n", "neither a table header nor a key"),
        ("key = \"v\"\n", "sits outside any table"),
        ("[a]\nkey = unquoted\n", "unreadable value"),
        ("[a]\nkey = \"v\" junk\n", "trailing"),
        ("[a]\nkey = \"v\"\nkey = \"w\"\n", "is set twice"),
        ("[a]\n[a]\n", "declared twice"),
    ] {
        let Err(refused) = tables(bundle) else {
            panic!("refuses {bundle:?}");
        };
        assert!(refused.contains(expected), "{refused:?} names {expected:?}");
    }
    // …and accepts what the bundles actually contain, comments included.
    let read = tables("# note\n[a.b]\nk = \"v\"   # why\nn = 12\n")
        .unwrap_or_else(|refused| panic!("the accepted subset: {refused}"));
    assert_eq!(read["a.b"]["k"], "v");
    assert_eq!(read["a.b"]["n"], "12");
}

/// A sweep is only sound if the suffix is the kernel's alone, and the
/// bundle is where that reservation is of record. Read as STRUCTURE, not
/// as a substring: round 1 asserted `bundle.contains("[recovery]")` and
/// it passed on a COMMENT, over a file whose `[operations.write]` table
/// had been broken in half by the very edit under test.
#[test]
fn the_bundle_states_what_becomes_of_a_stage_file_whose_rename_never_came() {
    let bundle = include_str!("../../../contracts/jinn-fs/metadata.toml");
    let tables =
        tables(bundle).unwrap_or_else(|refused| panic!("the fs bundle is well formed: {refused}"));
    let recovery = tables
        .get("recovery")
        .unwrap_or_else(|| panic!("a [recovery] TABLE, not prose mentioning one"));
    assert_eq!(
        recovery.get("staged").map(String::as_str),
        Some("<name>.jinnd-stage"),
        "the reserved staging name is declared, not merely described"
    );
    assert_eq!(
        recovery.get("policy").map(String::as_str),
        Some("sweep-staged-on-open-never-adopt"),
        "a staged file whose rename never came is deleted, never adopted"
    );
    assert_eq!(
        recovery.get("on-failure").map(String::as_str),
        Some("refuse-open"),
        "the sweep is absolute, and the contract says so in those words"
    );
    // The table the round-1 edit destroyed: it declares the commit shape
    // that reserves the staging name in the first place.
    assert_eq!(
        tables["operations.write"].get("commit").map(String::as_str),
        Some("stage-fsync-rename"),
        "the commit shape that reserves the name is intact"
    );
}
