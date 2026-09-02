//! `#[cfg(..)]` evaluated the way the default non-test build evaluates it.
//!
//! Three-valued on purpose: `test`, unknown bare cfgs (`loom`, `kani`, ...)
//! and features nobody enables are FALSE; platform and profile cfgs
//! (`unix`, `target_os`, `debug_assertions`, ...) are MAYBE, because the
//! kernel ships on more than one platform and a size meter must count code
//! that some build compiles. An item is dropped only when its predicate is
//! definitely false.

use std::collections::BTreeSet;

use syn::{Attribute, Meta};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Tri {
    False,
    Maybe,
    True,
}

/// Bare cfg names the default build may set (platform/profile); anything
/// else bare, `test` included, is false in a `cargo build`.
const MAYBE_BARE: &[&str] = &[
    "unix",
    "windows",
    "debug_assertions",
    "doc",
    "doctest",
    "proc_macro",
    "overflow_checks",
    "ub_checks",
];

/// True when no `#[cfg(..)]` on `attrs` is definitely false in the default non-test build.
pub fn compiled(attrs: &[Attribute], features: &BTreeSet<String>) -> bool {
    attrs.iter().filter(|a| a.path().is_ident("cfg")).all(|a| {
        a.parse_args::<Meta>()
            .map(|m| eval(&m, features) != Tri::False)
            .unwrap_or(true)
    })
}

fn eval(meta: &Meta, features: &BTreeSet<String>) -> Tri {
    match meta {
        Meta::Path(path) => {
            let name = path
                .get_ident()
                .map(ToString::to_string)
                .unwrap_or_default();
            if MAYBE_BARE.contains(&name.as_str()) {
                Tri::Maybe
            } else {
                Tri::False
            }
        }
        Meta::NameValue(nv) => {
            if !nv.path.is_ident("feature") {
                return Tri::Maybe;
            }
            match &nv.value {
                syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(s),
                    ..
                }) if features.contains(&s.value()) => Tri::True,
                _ => Tri::False,
            }
        }
        Meta::List(list) => {
            let items = list
                .parse_args_with(
                    syn::punctuated::Punctuated::<Meta, syn::Token![,]>::parse_terminated,
                )
                .map(|p| p.into_iter().collect::<Vec<_>>())
                .unwrap_or_default();
            let values = items.iter().map(|m| eval(m, features));
            if list.path.is_ident("all") {
                values.min().unwrap_or(Tri::True)
            } else if list.path.is_ident("any") {
                values.max().unwrap_or(Tri::False)
            } else if list.path.is_ident("not") {
                match values.min().unwrap_or(Tri::Maybe) {
                    Tri::False => Tri::True,
                    Tri::True => Tri::False,
                    Tri::Maybe => Tri::Maybe,
                }
            } else {
                Tri::Maybe
            }
        }
    }
}
