//! WIT, observed through the toolchain's own parser (M2-K15 round-2
//! ruling, M2-K16). `contains("package jinn:plugin@0.10.0;")` is satisfied
//! by those bytes ANYWHERE — a doc comment above a declaration that says
//! something else satisfies it — and no substring can see a variant case's
//! PAYLOAD or a function's parameter TYPES at all. `wit_parser::Resolve`
//! observes the DECLARATION.

use wit_parser::{Interface, PackageId, Resolve, Type, TypeDefKind};

/// One parsed WIT document and the package it declares.
pub struct Wit {
    resolve: Resolve,
    package: PackageId,
    path: String,
}

impl Wit {
    /// Parse a standalone WIT document. A document the toolchain cannot
    /// read panics here naming the file: that is a broken contract of
    /// record whatever bytes it happens to contain.
    pub fn parse(path: &str, text: &str) -> Wit {
        let mut resolve = Resolve::default();
        let package = resolve
            .push_str(path, text)
            .unwrap_or_else(|err| panic!("{path} parses as WIT: {err:#}"));
        Wit {
            resolve,
            package,
            path: path.to_owned(),
        }
    }

    /// The declared identity `namespace:name@version`, as the parser read
    /// it rather than as any comment spells it.
    pub fn package_id(&self) -> String {
        self.resolve.packages[self.package].name.to_string()
    }

    /// The declared `namespace:name`, without the version.
    pub fn package_name(&self) -> String {
        let name = &self.resolve.packages[self.package].name;
        format!("{}:{}", name.namespace, name.name)
    }

    /// The declared version; a contract of record is always versioned
    /// (R12), so an unversioned package panics naming the file.
    pub fn version(&self) -> String {
        self.resolve.packages[self.package]
            .name
            .version
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| panic!("{} declares a package version (R12)", self.path))
    }

    /// One named interface out of the parsed package.
    pub fn interface(&self, name: &str) -> Iface<'_> {
        let id = *self.resolve.packages[self.package]
            .interfaces
            .get(name)
            .unwrap_or_else(|| panic!("{} declares interface {name}", self.path));
        Iface {
            resolve: &self.resolve,
            iface: &self.resolve.interfaces[id],
        }
    }
}

/// One parsed interface: declarations, never source lines.
pub struct Iface<'a> {
    resolve: &'a Resolve,
    iface: &'a Interface,
}

impl Iface<'_> {
    /// A function as `name: func(param: type, ...) -> result`, from the
    /// PARSED signature — a renamed parameter, a reordered one, or a
    /// changed type all differ here; none is visible to a substring.
    pub fn signature(&self, name: &str) -> String {
        let func = self
            .iface
            .functions
            .get(name)
            .unwrap_or_else(|| panic!("the interface declares {name}"));
        let params: Vec<_> = func
            .params
            .iter()
            .map(|(param, ty)| format!("{param}: {}", render(self.resolve, *ty)))
            .collect();
        let result = func
            .result
            .map(|ty| format!(" -> {}", render(self.resolve, ty)))
            .unwrap_or_default();
        format!("{}: func({}){result}", func.name, params.join(", "))
    }

    /// Every declared function name, in declaration order.
    pub fn functions(&self) -> Vec<String> {
        self.iface.functions.keys().cloned().collect()
    }

    /// A record's fields as `name: type`, in declaration order.
    pub fn record_fields(&self, name: &str) -> Vec<String> {
        match &self.type_def(name).kind {
            TypeDefKind::Record(record) => record
                .fields
                .iter()
                .map(|field| format!("{}: {}", field.name, render(self.resolve, field.ty)))
                .collect(),
            other => panic!("{name} is a record, not {other:?}"),
        }
    }

    /// A variant's cases as `name` / `name(payload)`, in declaration order.
    pub fn variant_cases(&self, name: &str) -> Vec<String> {
        match &self.type_def(name).kind {
            TypeDefKind::Variant(variant) => variant
                .cases
                .iter()
                .map(|case| match case.ty {
                    Some(ty) => format!("{}({})", case.name, render(self.resolve, ty)),
                    None => case.name.clone(),
                })
                .collect(),
            other => panic!("{name} is a variant, not {other:?}"),
        }
    }

    /// An enum's cases, in declaration order.
    pub fn enum_cases(&self, name: &str) -> Vec<String> {
        match &self.type_def(name).kind {
            TypeDefKind::Enum(cases) => cases.cases.iter().map(|case| case.name.clone()).collect(),
            other => panic!("{name} is an enum, not {other:?}"),
        }
    }

    fn type_def(&self, name: &str) -> &wit_parser::TypeDef {
        let id = *self
            .iface
            .types
            .get(name)
            .unwrap_or_else(|| panic!("the interface declares type {name}"));
        &self.resolve.types[id]
    }
}

/// The doc block the parser attached to ONE named item — every comment
/// run (`//`, `///`, `/* */`) immediately above that declaration, as
/// `wit_parser::Docs` collected it. Scoped by construction: a phrase found
/// here was written about THIS item, never somewhere else in the file.
pub struct Docs {
    contents: String,
}

impl Docs {
    /// PROSE PRESENCE in this one block, nothing more: `phrase` occurs
    /// verbatim in the comment run the parser attached to the named item.
    /// It proves the contract says it THERE; it never proves a declaration
    /// (a declaration is not a comment, so it is never in here).
    pub fn states(&self, phrase: &str) -> bool {
        self.contents.contains(phrase)
    }
}

impl Iface<'_> {
    /// The doc block of function `name`; panics naming a function the
    /// interface does not declare.
    pub fn func_docs(&self, name: &str) -> Docs {
        let func = self
            .iface
            .functions
            .get(name)
            .unwrap_or_else(|| panic!("the interface declares {name}"));
        Docs {
            contents: func.docs.contents.clone().unwrap_or_default(),
        }
    }

    /// The doc block of type `name`; panics naming a type the interface
    /// does not declare.
    pub fn type_docs(&self, name: &str) -> Docs {
        Docs {
            contents: self
                .type_def(name)
                .docs
                .contents
                .clone()
                .unwrap_or_default(),
        }
    }
}

/// A type as WIT spells it, for the forms these contracts use. An
/// unanticipated form PANICS rather than rendering to something
/// comparable: a shape nobody expected is a finding, not a pass.
fn render(resolve: &Resolve, ty: Type) -> String {
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
