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
/// nothing written.
pub(super) fn validate(config: &serde_json::Value) -> Result<(), String> {
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

    /// The decidable schema: an object whose grants would admit; a grant
    /// that would refuse at activation refuses the patch whole.
    #[test]
    fn validation_refuses_what_activation_would_refuse() {
        assert!(validate(&serde_json::json!({ "grants": ["jinn:fs"], "data": "noop" })).is_ok());
        assert!(validate(&serde_json::json!("noop")).is_err());
        assert!(validate(&serde_json::json!({ "grants": [7] })).is_err());
        assert!(
            validate(&serde_json::json!({ "grants": [{ "contract": "jinn:fs", "scope": 9 }] }))
                .is_err()
        );
    }
}
