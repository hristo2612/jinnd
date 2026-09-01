//! The `jinn:auth` bundle is a CONTRACT OF RECORD and is asserted by
//! PARSING (the M2-K15 round-2 ruling; the M2-K19 `toml` precedent) — the
//! WIT through `wit_parser::Resolve`, the metadata through `toml`'s
//! document API. A `contains` observes bytes wherever they sit: a version
//! in a comment satisfies it, and it cannot see a variant case's payload
//! or a function's parameter types at all. These read the DECLARATION.

use toml::de::{DeTable, DeValue};

const WIT: &str = include_str!("../../../../../contracts/jinn-auth/contract.wit");
const METADATA: &str = include_str!("../../../../../contracts/jinn-auth/metadata.toml");

fn parsed() -> (wit_parser::Resolve, wit_parser::PackageId) {
    let mut resolve = wit_parser::Resolve::default();
    let package = resolve
        .push_str("contracts/jinn-auth/contract.wit", WIT)
        .unwrap_or_else(|err| panic!("the auth bundle parses as WIT: {err:#}"));
    (resolve, package)
}

fn auth_interface(
    resolve: &wit_parser::Resolve,
    package: wit_parser::PackageId,
) -> &wit_parser::Interface {
    let id = *resolve.packages[package]
        .interfaces
        .get("auth")
        .unwrap_or_else(|| panic!("the package declares interface auth"));
    &resolve.interfaces[id]
}

/// A type as WIT spells it, for the forms this contract uses; an
/// unanticipated form panics rather than rendering to something comparable.
fn render(resolve: &wit_parser::Resolve, ty: wit_parser::Type) -> String {
    use wit_parser::{Type, TypeDefKind};
    match ty {
        Type::String => "string".into(),
        Type::Id(id) => {
            let def = &resolve.types[id];
            if let Some(name) = &def.name {
                return name.clone();
            }
            match &def.kind {
                TypeDefKind::Result(result) => match (result.ok, result.err) {
                    (Some(ok), Some(err)) => {
                        format!("result<{}, {}>", render(resolve, ok), render(resolve, err))
                    }
                    other => panic!("unrendered result form: {other:?}"),
                },
                other => panic!("unrendered WIT type form: {other:?}"),
            }
        }
        other => panic!("unrendered WIT type: {other:?}"),
    }
}

/// The declared identity, the one operation's PARSED signature, the
/// refusal variant's full case list with payloads, and the principal's
/// fields — none of which a substring on a source line can see.
#[test]
fn the_wit_declares_one_operation_one_refusal_case_and_a_named_principal() {
    let (resolve, package) = parsed();
    assert_eq!(
        resolve.packages[package].name.to_string(),
        "jinn:auth@0.1.0"
    );
    let auth = auth_interface(&resolve, package);
    let verify = auth
        .functions
        .get("verify")
        .unwrap_or_else(|| panic!("the interface declares verify"));
    let params: Vec<String> = verify
        .params
        .iter()
        .map(|(name, ty)| format!("{name}: {}", render(&resolve, *ty)))
        .collect();
    let result = verify
        .result
        .map(|ty| render(&resolve, ty))
        .unwrap_or_default();
    assert_eq!(
        format!("verify: func({}) -> {result}", params.join(", ")),
        "verify: func(presented: string) -> result<principal, auth-error>"
    );
    assert_eq!(
        auth.functions.len(),
        1,
        "one operation: the scope ruling admits no second"
    );
    let error = *auth
        .types
        .get("auth-error")
        .unwrap_or_else(|| panic!("the interface declares auth-error"));
    let cases: Vec<String> = match &resolve.types[error].kind {
        wit_parser::TypeDefKind::Variant(variant) => variant
            .cases
            .iter()
            .map(|case| match case.ty {
                Some(ty) => format!("{}({})", case.name, render(&resolve, ty)),
                None => case.name.clone(),
            })
            .collect(),
        other => panic!("auth-error is a variant, not {other:?}"),
    };
    assert_eq!(cases, vec!["unauthenticated(string)"]);
    let principal = *auth
        .types
        .get("principal")
        .unwrap_or_else(|| panic!("the interface declares principal"));
    let fields: Vec<String> = match &resolve.types[principal].kind {
        wit_parser::TypeDefKind::Record(record) => record
            .fields
            .iter()
            .map(|field| format!("{}: {}", field.name, render(&resolve, field.ty)))
            .collect(),
        other => panic!("principal is a record, not {other:?}"),
    };
    assert_eq!(fields, vec!["name: string"]);
}

fn entry<'a, 'i>(table: &'a DeTable<'i>, key: &str) -> Option<&'a DeValue<'i>> {
    table
        .iter()
        .find(|(name, _)| name.get_ref().as_ref() == key)
        .map(|(_, value)| value.get_ref())
}

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

fn integer_at(root: &DeTable<'_>, path: &str) -> Option<i64> {
    let mut segments = path.split('.').peekable();
    let mut table = root;
    while let Some(segment) = segments.next() {
        let value = entry(table, segment)?;
        if segments.peek().is_none() {
            return value
                .as_integer()
                .and_then(|integer| i64::from_str_radix(integer.as_str(), integer.radix()).ok());
        }
        table = value.as_table()?;
    }
    None
}

/// The metadata records, as real keys in real tables: the read effect
/// class, the credential's preconditions, the threat model's stated
/// limit, and that there is no off switch — each of which the provider
/// tests below enforce, so the contract and the code cannot drift apart
/// unnoticed.
#[test]
fn the_metadata_records_the_effect_class_the_preconditions_and_the_limit() {
    let document = DeTable::parse(METADATA)
        .unwrap_or_else(|refused| panic!("the auth bundle is well formed: {refused}"));
    let auth = document.get_ref();
    for (path, value) in [
        ("contract.name", "jinn:auth"),
        ("contract.version", "0.1.0"),
        ("operations.verify.effect", "read"),
        ("credential.name", "operator"),
        ("credential.rotation", "re-read-per-call-no-restart"),
        ("credential.absent", "deny"),
        ("credential.mode-mask", "0o077-must-be-clear"),
        ("credential.compare", "constant-time-over-sha256"),
        ("threat-model.same-uid", "not-in-model"),
        ("bypass.off-switch", "none"),
    ] {
        assert_eq!(
            string_at(auth, path),
            Some(value),
            "contracts/jinn-auth/metadata.toml {path}"
        );
    }
    assert_eq!(
        integer_at(auth, "credential.minimum-len"),
        i64::try_from(super::MIN_LEN).ok()
    );
    assert_eq!(
        integer_at(auth, "credential.maximum-file"),
        i64::try_from(super::MAX_FILE).ok()
    );
    assert!(
        entry(auth, "scope").is_none(),
        "the bundle declares NO scope table: nothing to attenuate"
    );
}
