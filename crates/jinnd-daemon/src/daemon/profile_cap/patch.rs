//! The two pure decisions a `jinn:profile` patch is made of, split out by
//! responsibility (R10 file hygiene): RFC 7396 merge, and the schema the
//! daemon can decide BEFORE it commits. Neither touches kernel state, so
//! neither belongs beside the provider that does.

use jinnd_wasm::grant_refusals;

use crate::seat::seat_config;

/// RFC 7396 merge-patch: an object patch merges key by key (`null`
/// removes), anything else replaces the target whole.
pub(super) fn merge_patch(target: &mut serde_json::Value, patch: &serde_json::Value) {
    let serde_json::Value::Object(fields) = patch else {
        *target = patch.clone();
        return;
    };
    if !target.is_object() {
        *target = serde_json::Value::Object(serde_json::Map::new());
    }
    if let serde_json::Value::Object(existing) = target {
        for (key, value) in fields {
            if value.is_null() {
                existing.remove(key);
            } else {
                merge_patch(
                    existing
                        .entry(key.clone())
                        .or_insert(serde_json::Value::Null),
                    value,
                );
            }
        }
    }
}

/// The profile schema the daemon can decide before committing (04): a
/// config is an object whose `grants` read as grants and would ADMIT at
/// activation — a patch that would only fault the entry is refused whole,
/// nothing written. (d), M2-K23: the patched `grants` must equal the
/// committed `grants` by value — a grants change is `jinn:profile-admin`'s
/// (04 §Write-back is confined), never `patch-entry`'s; `injects` stays
/// patchable (a declaration gates and never widens).
pub(crate) fn validate(
    committed: &serde_json::Value,
    config: &serde_json::Value,
) -> Result<(), String> {
    if !config.is_object() {
        return Err("the patched config is not an object".to_owned());
    }
    let seat = seat_config(config);
    if let Some(fault) = seat.faults.first() {
        return Err(format!("grant entry refused: {fault}"));
    }
    if let Some(refused) = grant_refusals(&seat.grants).first() {
        return Err(refused.message.clone());
    }
    // Admission is judged first so a malformed grant is named as such;
    // a grants change that WOULD admit is the (d) refusal.
    if config.get("grants") != committed.get("grants") {
        return Err("grants are jinn:profile-admin's".to_owned());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{merge_patch, validate};

    /// RFC 7396: nested merge, null removes, scalars replace, a non-object
    /// patch replaces whole.
    #[test]
    fn merge_patch_follows_rfc_7396() {
        let mut target = serde_json::json!({ "grants": ["jinn:fs"], "data": { "a": 1, "b": 2 } });
        merge_patch(
            &mut target,
            &serde_json::json!({ "data": { "b": null, "c": 3 }, "extra": "x" }),
        );
        assert_eq!(
            target,
            serde_json::json!({ "grants": ["jinn:fs"], "data": { "a": 1, "c": 3 }, "extra": "x" })
        );
        merge_patch(&mut target, &serde_json::json!("plain"));
        assert_eq!(target, serde_json::json!("plain"));
    }

    /// (d), M2-K23 — the reading of `655f07e` confirmed red-first: a patch
    /// whose RESULT changes `config.grants` must refuse, because grants
    /// are `jinn:profile-admin`'s (04 §Write-back is confined) and a
    /// widening through `patch-entry` is a Law-1 side door. The committed
    /// config is what the result is compared against.
    #[test]
    fn a_patch_whose_result_changes_grants_is_refused_whole() {
        let committed = serde_json::json!({ "grants": ["jinn:fs"], "data": "a" });
        let mut widened = committed.clone();
        merge_patch(
            &mut widened,
            &serde_json::json!({ "grants": ["jinn:fs", "jinn:net"] }),
        );
        assert!(
            validate(&committed, &widened).is_err(),
            "a grants widening through patch-entry admits"
        );
        let mut narrowed = committed.clone();
        merge_patch(&mut narrowed, &serde_json::json!({ "grants": [] }));
        assert!(validate(&committed, &narrowed).is_err(), "a narrowing too");
        let mut same = committed.clone();
        merge_patch(&mut same, &serde_json::json!({ "data": "b" }));
        assert!(
            validate(&committed, &same).is_ok(),
            "config.data stays patchable"
        );
    }

    /// The decidable schema: an object whose grants would admit; a grant
    /// that would refuse at activation refuses the patch whole.
    #[test]
    fn validation_refuses_what_activation_would_refuse() {
        let good = serde_json::json!({ "grants": ["jinn:fs"], "data": "noop" });
        assert!(validate(&good, &good).is_ok());
        assert!(validate(&good, &serde_json::json!("noop")).is_err());
        let bad = serde_json::json!({ "grants": [7] });
        assert!(validate(&bad, &bad).is_err());
        let bad = serde_json::json!({ "grants": [{ "contract": "jinn:fs", "scope": 9 }] });
        assert!(validate(&bad, &bad).is_err());
    }
}
