//! The `jinn:auth` bundle is a CONTRACT OF RECORD and is asserted by
//! PARSING (the M2-K15 round-2 ruling; the M2-K19 `toml` precedent),
//! through `jinnd_contract_lens` (M2-K16): the WIT through the toolchain's
//! own parser, the metadata as real tables. A substring observes bytes
//! wherever they sit — a version in a comment satisfies it — and it cannot
//! see a variant case's payload or a function's parameter types at all.
//! These read the DECLARATION.

use jinnd_contract_lens::bundle;

/// The declared identity, the one operation's PARSED signature, the
/// refusal variant's full case list with payloads, and the principal's
/// fields — none of which a substring on a source line can see.
#[test]
fn the_wit_declares_one_operation_one_refusal_case_and_a_named_principal() {
    let wit = bundle("jinn-auth").wit().wit();
    assert_eq!(wit.package_id(), "jinn:auth@0.1.0");
    let auth = wit.interface("auth");
    assert_eq!(
        auth.signature("verify"),
        "verify: func(presented: string) -> result<principal, auth-error>"
    );
    assert_eq!(
        auth.functions(),
        ["verify"],
        "one operation: the scope ruling admits no second"
    );
    assert_eq!(
        auth.variant_cases("auth-error"),
        ["unauthenticated(string)"]
    );
    assert_eq!(auth.record_fields("principal"), ["name: string"]);
}

/// The metadata records, as real keys in real tables: the read effect
/// class, the credential's preconditions, the threat model's stated
/// limit, and that there is no off switch — each of which the provider
/// tests below enforce, so the contract and the code cannot drift apart
/// unnoticed.
#[test]
fn the_metadata_records_the_effect_class_the_preconditions_and_the_limit() {
    let auth = bundle("jinn-auth").metadata().metadata();
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
            auth.string_at(path).as_deref(),
            Some(value),
            "contracts/jinn-auth/metadata.toml {path}"
        );
    }
    assert_eq!(
        auth.integer_at("credential.minimum-len"),
        i64::try_from(super::MIN_LEN).ok()
    );
    assert_eq!(
        auth.integer_at("credential.maximum-file"),
        i64::try_from(super::MAX_FILE).ok()
    );
    assert!(
        !auth.has_key("scope"),
        "the bundle declares NO scope table: nothing to attenuate"
    );
}
