//! The serde-typed profile document: parse, render, and the realm directives
//! (`#entryId` local, `@label` shared) at the contract boundary (R3).

use jinnd_api::{EntryId, ErrorCode, Realm};
use jinnd_loader::{Document, DocumentEntry};

fn entry(id: &str, package: &str) -> DocumentEntry {
    DocumentEntry {
        id: id.to_owned(),
        package: package.to_owned(),
        version: String::new(),
        hash: String::new(),
        config: serde_json::json!({}),
        disabled: false,
        parent: None,
        isolate: Default::default(),
        extra: Default::default(),
    }
}

#[test]
fn parse_render_round_trip_preserves_every_entry() {
    let json = r#"{
        "entries": [
            {"id": "foo", "package": "test/foo", "config": {"a": 1}},
            {"id": "bar", "package": "test/bar", "disabled": true, "parent": "foo"}
        ]
    }"#;
    let document = Document::parse(json).grab();
    assert_eq!(document.entries.len(), 2);
    let again = Document::parse(&document.render()).grab();
    assert_eq!(document, again);
}

#[test]
fn a_malformed_entry_is_contained_and_its_siblings_still_load() {
    let json = r#"{
        "entries": [
            {"id": "good", "package": "test/good"},
            {"id": 7, "package": "test/bad"},
            {"id": "late", "package": "test/late", "disabled": "yes"},
            {"id": "tail", "package": "test/tail"}
        ]
    }"#;
    let document = Document::parse(json).grab();
    assert_eq!(document.entries.len(), 2, "the well-formed entries decode");

    let (profile, faults) = document.resolve();
    assert_eq!(profile.entries.len(), 2);
    assert_eq!(profile.entries[0].id, EntryId("good".to_owned()));
    assert_eq!(profile.entries[1].id, EntryId("tail".to_owned()));
    // Each malformed entry surfaces exactly one recorded fault (R11): by its
    // id when one is legible, by its position otherwise.
    assert_eq!(faults.len(), 2);
    assert_eq!(faults[0].entry, EntryId("entries[1]".to_owned()));
    assert_eq!(faults[0].error.code, ErrorCode::InvalidProfile);
    assert_eq!(faults[1].entry, EntryId("late".to_owned()));
    assert_eq!(faults[1].error.code, ErrorCode::InvalidProfile);
}

#[test]
fn a_malformed_entry_survives_write_back_verbatim() {
    let json = r#"{
        "entries": [
            {"id": "good", "package": "test/good"},
            {"id": 7, "package": "test/bad"}
        ]
    }"#;
    let document = Document::parse(json).grab();
    // Write-back re-emits what it did not understand, in place: a save never
    // erases a faulted entry (v0.1: no destructive compaction).
    let rendered = document.render();
    assert!(
        rendered.contains("\"id\": 7"),
        "verbatim entry kept: {rendered}"
    );
    let again = Document::parse(&rendered).grab();
    assert_eq!(document, again);
}

#[test]
fn unknown_fields_and_raw_entries_round_trip_byte_for_byte() {
    // "Verbatim" means bytes, not value-equivalence (v0.1 bounds; PLA-276
    // round-2 blocker 2): a normalizing parse→Value→pretty pipeline is the
    // silent-rewrite class the constitution bans.
    let json = r#"{"entries":[{"id":"known","package":"test/known","config":1,"note":{"spacing":"keep"}},{"id":"future","package":42,"payload":{"raw":[1,2,3]}}]}"#;
    let document = Document::parse(json).grab();
    let rendered = document.render();
    assert!(
        rendered.contains(r#""note":{"spacing":"keep"}"#),
        "the unknown known-entry field must round-trip byte-for-byte: {rendered}"
    );
    assert!(
        rendered.contains(r#"{"id":"future","package":42,"payload":{"raw":[1,2,3]}}"#),
        "the opaque future entry must round-trip byte-for-byte: {rendered}"
    );
    let again = Document::parse(&rendered).grab();
    assert_eq!(document, again);
}

#[test]
fn parse_refuses_documents_that_are_not_json() {
    let Err(error) = Document::parse("entries: nope") else {
        panic!("a non-document must not parse");
    };
    assert_eq!(error.code, ErrorCode::InvalidProfile);
}

#[test]
fn resolve_maps_local_and_shared_realm_directives() {
    let mut provider = entry("bar", "test/bar");
    provider
        .isolate
        .insert("svc.bar".to_owned(), "#bar".to_owned());
    let mut shared = entry("qux", "test/qux");
    shared
        .isolate
        .insert("svc.bar".to_owned(), "@beta".to_owned());
    let document = Document {
        entries: vec![provider, shared],
        raw: Vec::new(),
    };

    let (profile, faults) = document.resolve();
    assert!(faults.is_empty());
    assert_eq!(profile.entries.len(), 2);
    assert_eq!(
        profile.entries[0].isolation[0].realm,
        Realm::Local(EntryId("bar".to_owned()))
    );
    assert_eq!(
        profile.entries[1].isolation[0].realm,
        Realm::Shared("beta".to_owned())
    );
}

#[test]
fn resolve_contains_a_malformed_directive_to_its_own_entry() {
    let mut bad = entry("bad", "test/bad");
    bad.isolate
        .insert("svc.bar".to_owned(), "no-sigil".to_owned());
    let good = entry("good", "test/good");
    let document = Document {
        entries: vec![bad, good],
        raw: Vec::new(),
    };

    let (profile, faults) = document.resolve();
    // The good entry loads; the bad one surfaces a recorded error (R11).
    assert_eq!(profile.entries.len(), 1);
    assert_eq!(profile.entries[0].id, EntryId("good".to_owned()));
    assert_eq!(faults.len(), 1);
    assert_eq!(faults[0].entry, EntryId("bad".to_owned()));
    assert_eq!(faults[0].error.code, ErrorCode::InvalidProfile);
}

#[test]
fn from_profile_writes_back_the_directive_syntax() {
    let json = r##"{
        "entries": [
            {"id": "foo", "package": "test/foo", "config": {"a": 1},
             "isolate": {"svc.bar": "#foo", "svc.qux": "@shared"}}
        ]
    }"##;
    let document = Document::parse(json).grab();
    let (profile, faults) = document.resolve();
    assert!(faults.is_empty());
    let back = Document::from_profile(&profile);
    assert_eq!(document, back);
}

/// Repo convention: no `unwrap`/`expect`. `grab` is the tests' one panicking
/// accessor, carrying the caller's location.
pub trait Grab<T> {
    fn grab(self) -> T;
}

impl<T, E: std::fmt::Debug> Grab<T> for Result<T, E> {
    #[track_caller]
    fn grab(self) -> T {
        match self {
            Ok(value) => value,
            Err(error) => panic!("unexpected error: {error:?}"),
        }
    }
}

impl<T> Grab<T> for Option<T> {
    #[track_caller]
    fn grab(self) -> T {
        match self {
            Some(value) => value,
            None => panic!("unexpectedly empty"),
        }
    }
}
