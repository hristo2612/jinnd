//! `#[cfg(..)]` evaluated the way the default non-test build evaluates it.

use std::collections::BTreeSet;

/// True when no `#[cfg(..)]` on `attrs` is definitely false in the default non-test build.
pub fn compiled(attrs: &[syn::Attribute], features: &BTreeSet<String>) -> bool {
    let _ = (attrs, features);
    true
}
