//! The module walk: from a target root, follow every `mod` the compiler would
//! follow (rustc's file-resolution rules, `#[path]` included) and record the
//! line ranges the non-test build drops.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use syn::spanned::Spanned;
use syn::{Attribute, Item};

use crate::{MeterError, cfg};

type Ranges = Vec<(usize, usize)>;

/// Every file reachable from `root` (relative to `tree`), with its `cfg`-off line ranges.
pub fn walk(
    tree: &Path,
    root: &Path,
    features: &BTreeSet<String>,
) -> Result<BTreeMap<PathBuf, Ranges>, MeterError> {
    let mut out = BTreeMap::new();
    let walker = Walker { tree, features };
    walker.file(root, true, &mut out)?;
    Ok(out)
}

struct Walker<'a> {
    tree: &'a Path,
    features: &'a BTreeSet<String>,
}

/// Where a `mod x;` inside the current file looks for `x`.
struct Scope {
    /// Directory of the current file (base for `#[path]` outside inline blocks).
    file_dir: PathBuf,
    /// Directory for non-`#[path]` children (`x.rs` / `x/mod.rs`).
    mod_dir: PathBuf,
    inline: bool,
}

impl Walker<'_> {
    fn file(
        &self,
        rel: &Path,
        mod_rs_like: bool,
        out: &mut BTreeMap<PathBuf, Ranges>,
    ) -> Result<(), MeterError> {
        if out.contains_key(rel) {
            return Ok(());
        }
        let abs = self.tree.join(rel);
        let Ok(source) = std::fs::read_to_string(&abs) else {
            return Ok(());
        };
        let file = syn::parse_file(&source)
            .map_err(|e| MeterError::Failed(format!("parse {}: {e}", rel.display())))?;
        let mut ranges = Ranges::new();
        if !cfg::compiled(&file.attrs, self.features) {
            ranges.push((1, usize::MAX));
            out.insert(rel.to_path_buf(), ranges);
            return Ok(());
        }
        let file_dir = rel.parent().map(Path::to_path_buf).unwrap_or_default();
        let mod_dir = if mod_rs_like {
            file_dir.clone()
        } else {
            file_dir.join(rel.file_stem().unwrap_or_default())
        };
        let scope = Scope {
            file_dir,
            mod_dir,
            inline: false,
        };
        let mut children = Vec::new();
        self.items(&file.items, &scope, &source, &mut ranges, &mut children);
        absorb_blank_lines(&source, &mut ranges);
        out.insert(rel.to_path_buf(), ranges);
        for (child, mod_rs_like) in children {
            self.file(&child, mod_rs_like, out)?;
        }
        Ok(())
    }

    fn items(
        &self,
        items: &[Item],
        scope: &Scope,
        source: &str,
        ranges: &mut Ranges,
        children: &mut Vec<(PathBuf, bool)>,
    ) {
        for item in items {
            if !cfg::compiled(attrs(item), self.features) {
                ranges.push(lines(item.span()));
                continue;
            }
            match item {
                Item::Mod(m) => match &m.content {
                    Some((_, inner)) => {
                        let nested = Scope {
                            file_dir: scope.file_dir.clone(),
                            mod_dir: scope.mod_dir.join(m.ident.to_string()),
                            inline: true,
                        };
                        self.items(inner, &nested, source, ranges, children);
                    }
                    None => children.extend(self.resolve(&m.attrs, &m.ident.to_string(), scope)),
                },
                Item::Impl(i) => {
                    for member in &i.items {
                        let (a, span) = match member {
                            syn::ImplItem::Const(x) => (&x.attrs, x.span()),
                            syn::ImplItem::Fn(x) => (&x.attrs, x.span()),
                            syn::ImplItem::Type(x) => (&x.attrs, x.span()),
                            syn::ImplItem::Macro(x) => (&x.attrs, x.span()),
                            _ => continue,
                        };
                        if !cfg::compiled(a, self.features) {
                            ranges.push(lines(span));
                        }
                    }
                }
                Item::Trait(t) => {
                    for member in &t.items {
                        let (a, span) = match member {
                            syn::TraitItem::Const(x) => (&x.attrs, x.span()),
                            syn::TraitItem::Fn(x) => (&x.attrs, x.span()),
                            syn::TraitItem::Type(x) => (&x.attrs, x.span()),
                            syn::TraitItem::Macro(x) => (&x.attrs, x.span()),
                            _ => continue,
                        };
                        if !cfg::compiled(a, self.features) {
                            ranges.push(lines(span));
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// rustc's rule: `#[path]` is relative to the file's directory outside inline
    /// blocks and to the inline module's directory inside them, and a
    /// `#[path]`-loaded file resolves its own children like `mod.rs` does.
    fn resolve(&self, attrs: &[Attribute], name: &str, scope: &Scope) -> Option<(PathBuf, bool)> {
        if let Some(path) = path_attr(attrs) {
            let base = if scope.inline {
                &scope.mod_dir
            } else {
                &scope.file_dir
            };
            return Some((base.join(path), true));
        }
        let flat = scope.mod_dir.join(format!("{name}.rs"));
        if self.tree.join(&flat).is_file() {
            return Some((flat, false));
        }
        let nested = scope.mod_dir.join(name).join("mod.rs");
        self.tree.join(&nested).is_file().then_some((nested, true))
    }
}

fn path_attr(attrs: &[Attribute]) -> Option<String> {
    attrs
        .iter()
        .filter(|a| a.path().is_ident("path"))
        .find_map(|a| match &a.meta {
            syn::Meta::NameValue(nv) => match &nv.value {
                syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(s),
                    ..
                }) => Some(s.value()),
                _ => None,
            },
            _ => None,
        })
}

fn lines(span: proc_macro2::Span) -> (usize, usize) {
    (span.start().line, span.end().line)
}

/// Blank lines that only separate a dropped item from what precedes it belong
/// to the dropped item: `\n#[cfg(test)]\nmod tests;` costs zero, not one.
fn absorb_blank_lines(source: &str, ranges: &mut Ranges) {
    let lines: Vec<&str> = source.lines().collect();
    for range in ranges.iter_mut() {
        while range.0 > 1 && lines.get(range.0 - 2).is_some_and(|l| l.trim().is_empty()) {
            range.0 -= 1;
        }
    }
}

fn attrs(item: &Item) -> &[Attribute] {
    match item {
        Item::Const(x) => &x.attrs,
        Item::Enum(x) => &x.attrs,
        Item::ExternCrate(x) => &x.attrs,
        Item::Fn(x) => &x.attrs,
        Item::ForeignMod(x) => &x.attrs,
        Item::Impl(x) => &x.attrs,
        Item::Macro(x) => &x.attrs,
        Item::Mod(x) => &x.attrs,
        Item::Static(x) => &x.attrs,
        Item::Struct(x) => &x.attrs,
        Item::Trait(x) => &x.attrs,
        Item::TraitAlias(x) => &x.attrs,
        Item::Type(x) => &x.attrs,
        Item::Union(x) => &x.attrs,
        Item::Use(x) => &x.attrs,
        _ => &[],
    }
}
