//! The facade's ledger row kinds, read out of the facade's OWN definition
//! (M2-K16 gate 2). Instance two was `metadata.toml` enumerating
//! `NetRequested { method, ... }` while `jinnd-api` carried `{ effect,
//! method, ... }`: two hand-maintained copies of one fact. The copy that
//! counts is the Rust enum, and Rust's parser reads it.

use std::collections::BTreeMap;

/// `crates/jinnd-api/src/ledger.rs`, the file that declares
/// `LedgerEventKind`. `include_str!` binds the path at build time, so a
/// moved file is a build error rather than a silently empty gate.
const LEDGER_SOURCE: &str = include_str!("../../jinnd-api/src/ledger.rs");

/// Every `LedgerEventKind` variant, with its named fields in declaration
/// order. A tuple variant (`FiberTransition(Transition)`) maps to an empty
/// list: it has no field names for a bundle to enumerate.
pub fn ledger_event_kinds() -> BTreeMap<String, Vec<String>> {
    let file = syn::parse_file(LEDGER_SOURCE)
        .unwrap_or_else(|err| panic!("jinnd-api/src/ledger.rs parses as Rust: {err}"));
    let ledger = file
        .items
        .iter()
        .find_map(|item| match item {
            syn::Item::Enum(item) if item.ident == "LedgerEventKind" => Some(item),
            _ => None,
        })
        .unwrap_or_else(|| panic!("jinnd-api/src/ledger.rs declares enum LedgerEventKind"));
    ledger
        .variants
        .iter()
        .map(|variant| {
            let fields = match &variant.fields {
                syn::Fields::Named(named) => named
                    .named
                    .iter()
                    .filter_map(|field| field.ident.as_ref().map(ToString::to_string))
                    .collect(),
                syn::Fields::Unnamed(_) | syn::Fields::Unit => Vec::new(),
            };
            (variant.ident.to_string(), fields)
        })
        .collect()
}
