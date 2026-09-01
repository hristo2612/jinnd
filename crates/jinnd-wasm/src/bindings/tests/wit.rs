//! Contract text is asserted by PARSING (M2-K15 round-2 ruling), never by
//! substring. `WORLD.contains("package jinn:plugin@0.10.0;")` is satisfied
//! by those bytes ANYWHERE — a doc comment above a declaration that says
//! something else satisfies it — and no substring can see a variant case's
//! PAYLOAD or a function's parameter TYPES at all. `wit_parser::Resolve` is
//! the toolchain's own parser: it observes the DECLARATION.

/// Parse a standalone WIT document, answering the resolved graph and the
/// package it declares. A document the toolchain cannot read fails here,
/// naming the file — that is a broken contract of record whatever bytes it
/// happens to contain.
pub(super) fn parse_wit(file: &str, text: &str) -> (wit_parser::Resolve, wit_parser::PackageId) {
    let mut resolve = wit_parser::Resolve::default();
    let package = resolve
        .push_str(file, text)
        .unwrap_or_else(|err| panic!("{file} parses as WIT: {err:#}"));
    (resolve, package)
}

/// The declared package identity, `namespace:name@version`, as the parser
/// read it rather than as the file spells it.
pub(super) fn package_id(resolve: &wit_parser::Resolve, package: wit_parser::PackageId) -> String {
    resolve.packages[package].name.to_string()
}

/// One named interface out of a parsed package.
pub(super) fn interface<'a>(
    resolve: &'a wit_parser::Resolve,
    package: wit_parser::PackageId,
    name: &str,
) -> &'a wit_parser::Interface {
    let id = *resolve.packages[package]
        .interfaces
        .get(name)
        .unwrap_or_else(|| panic!("the package declares interface {name}"));
    &resolve.interfaces[id]
}

/// A type as WIT spells it, for the forms these contracts use. An
/// unanticipated form PANICS rather than rendering to something comparable:
/// a shape nobody expected is a finding, not a pass.
fn render(resolve: &wit_parser::Resolve, ty: wit_parser::Type) -> String {
    use wit_parser::{Type, TypeDefKind};
    match ty {
        Type::Bool => "bool".into(),
        Type::U8 => "u8".into(),
        Type::U16 => "u16".into(),
        Type::U32 => "u32".into(),
        Type::U64 => "u64".into(),
        Type::S8 => "s8".into(),
        Type::S16 => "s16".into(),
        Type::S32 => "s32".into(),
        Type::S64 => "s64".into(),
        Type::F32 => "f32".into(),
        Type::F64 => "f64".into(),
        Type::Char => "char".into(),
        Type::String => "string".into(),
        Type::ErrorContext => "error-context".into(),
        Type::Id(id) => {
            let def = &resolve.types[id];
            if let Some(name) = &def.name {
                return name.clone();
            }
            match &def.kind {
                TypeDefKind::List(inner) => format!("list<{}>", render(resolve, *inner)),
                TypeDefKind::Option(inner) => format!("option<{}>", render(resolve, *inner)),
                TypeDefKind::Tuple(tuple) => {
                    let parts: Vec<_> = tuple.types.iter().map(|t| render(resolve, *t)).collect();
                    format!("tuple<{}>", parts.join(", "))
                }
                TypeDefKind::Result(result) => match (result.ok, result.err) {
                    (Some(ok), Some(err)) => {
                        format!("result<{}, {}>", render(resolve, ok), render(resolve, err))
                    }
                    (Some(ok), None) => format!("result<{}>", render(resolve, ok)),
                    (None, Some(err)) => format!("result<_, {}>", render(resolve, err)),
                    (None, None) => "result".into(),
                },
                other => panic!("unrendered WIT type form: {other:?}"),
            }
        }
    }
}

/// A function as `name: func(param: type, ...) -> result`, from the PARSED
/// signature — so a renamed parameter, a reordered one, or a changed type
/// all fail here, none of which a substring on the source line can see.
pub(super) fn signature(
    resolve: &wit_parser::Resolve,
    iface: &wit_parser::Interface,
    name: &str,
) -> String {
    let func = iface
        .functions
        .get(name)
        .unwrap_or_else(|| panic!("the interface declares {name}"));
    let params: Vec<_> = func
        .params
        .iter()
        .map(|(param, ty)| format!("{param}: {}", render(resolve, *ty)))
        .collect();
    let result = func
        .result
        .map(|ty| format!(" -> {}", render(resolve, ty)))
        .unwrap_or_default();
    format!("{}: func({}){result}", func.name, params.join(", "))
}

/// A variant's cases as `name` / `name(payload)`, in declaration order.
pub(super) fn variant_cases(
    resolve: &wit_parser::Resolve,
    iface: &wit_parser::Interface,
    name: &str,
) -> Vec<String> {
    let id = *iface
        .types
        .get(name)
        .unwrap_or_else(|| panic!("the interface declares {name}"));
    match &resolve.types[id].kind {
        wit_parser::TypeDefKind::Variant(variant) => variant
            .cases
            .iter()
            .map(|case| match case.ty {
                Some(ty) => format!("{}({})", case.name, render(resolve, ty)),
                None => case.name.clone(),
            })
            .collect(),
        other => panic!("{name} is a variant, not {other:?}"),
    }
}

/// A record's fields as `name: type`, in declaration order.
pub(super) fn record_fields(
    resolve: &wit_parser::Resolve,
    iface: &wit_parser::Interface,
    name: &str,
) -> Vec<String> {
    let id = *iface
        .types
        .get(name)
        .unwrap_or_else(|| panic!("the interface declares {name}"));
    match &resolve.types[id].kind {
        wit_parser::TypeDefKind::Record(record) => record
            .fields
            .iter()
            .map(|field| format!("{}: {}", field.name, render(resolve, field.ty)))
            .collect(),
        other => panic!("{name} is a record, not {other:?}"),
    }
}
