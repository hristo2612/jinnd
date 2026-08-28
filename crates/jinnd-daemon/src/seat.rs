//! One wasm entry's seat configuration, decoded from its profile config
//! document (split from the lane per the under-300 source-file rule, R10).
//! The decoded [`SeatSpec`] is the lifted lane's seat seam (M2-K1).

use jinnd_wasm::{Grant, SeatSpec};

/// Decodes one entry's config document: `grants` are the contracts the
/// profile side grants the instance, each with its optional scope
/// (constitution 01: requests are not grants), `data` is the opaque payload
/// handed to the guest's `activate` (R9: data, never behavior).
pub(crate) fn seat_config(value: &serde_json::Value) -> SeatSpec {
    let grants = value
        .get("grants")
        .and_then(|grants| grants.as_array())
        .map(|grants| grants.iter().filter_map(grant).collect())
        .unwrap_or_default();
    let payload = match value.get("data") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::String(text)) => text.clone().into_bytes(),
        Some(other) => other.to_string().into_bytes(),
    };
    SeatSpec { grants, payload }
}

/// One profile grant entry (constitution 04 §Format): a bare contract name,
/// or `{ contract, scope }` where v0.1's enforced scope is the `rate` type
/// — a minimum period in milliseconds (contracts/jinn-clock `[scope]`,
/// M2-K2). An entry whose scope the kernel cannot enforce decodes to NO
/// grant: an unenforceable scope must never widen into an unscoped grant
/// (01 §Grants, attenuation).
fn grant(value: &serde_json::Value) -> Option<Grant> {
    match value {
        serde_json::Value::String(contract) => Some(Grant {
            contract: contract.clone(),
            scope: None,
        }),
        serde_json::Value::Object(fields) => {
            let contract = fields.get("contract")?.as_str()?.to_owned();
            match fields.get("scope") {
                None | Some(serde_json::Value::Null) => Some(Grant {
                    contract,
                    scope: None,
                }),
                Some(scope) => scope.as_u64().map(|floor| Grant {
                    contract,
                    scope: Some(floor),
                }),
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use jinnd_wasm::Grant;

    use super::seat_config;

    fn bare(contract: &str) -> Grant {
        Grant {
            contract: contract.to_owned(),
            scope: None,
        }
    }

    #[test]
    fn seat_config_decodes_grants_and_string_data() {
        let value = serde_json::json!({ "grants": ["demo:clock", "jinn:fs"], "data": "world" });
        let seat = seat_config(&value);
        assert_eq!(seat.grants, vec![bare("demo:clock"), bare("jinn:fs")]);
        assert_eq!(seat.payload, b"world".to_vec());
    }

    #[test]
    fn seat_config_defaults_to_no_grants_and_empty_payload() {
        let seat = seat_config(&serde_json::json!({}));
        assert!(seat.grants.is_empty());
        assert!(seat.payload.is_empty());
    }

    #[test]
    fn seat_config_serializes_structured_data() {
        let seat = seat_config(&serde_json::json!({ "data": { "a": 1 } }));
        assert_eq!(seat.payload, br#"{"a":1}"#.to_vec());
    }

    /// M2-K2 (constitution 04 §Format): a structured grant entry carries
    /// its scope — for `jinn:clock`, the rate floor in milliseconds.
    #[test]
    fn seat_config_decodes_a_scoped_grant() {
        let value = serde_json::json!({
            "grants": ["jinn:fs", { "contract": "jinn:clock", "scope": 1000 }],
        });
        let seat = seat_config(&value);
        assert_eq!(
            seat.grants,
            vec![
                bare("jinn:fs"),
                Grant {
                    contract: "jinn:clock".to_owned(),
                    scope: Some(1000),
                }
            ]
        );
    }

    /// A scope shape v0.1 cannot enforce decodes to NO grant — never to an
    /// unscoped (wider) one (01 §Grants, attenuation).
    #[test]
    fn seat_config_refuses_an_unenforceable_scope_rather_than_widening() {
        let value = serde_json::json!({
            "grants": [
                { "contract": "jinn:clock", "scope": "fine" },
                { "contract": "jinn:clock", "scope": -5 },
                { "scope": 100 },
                7,
            ],
        });
        let seat = seat_config(&value);
        assert!(
            seat.grants.is_empty(),
            "unenforceable entries drop whole: {:?}",
            seat.grants
        );
    }

    /// A structured entry without a scope is the bare grant.
    #[test]
    fn seat_config_decodes_a_structured_grant_without_scope() {
        let value = serde_json::json!({
            "grants": [{ "contract": "jinn:clock" }, { "contract": "jinn:fs", "scope": null }],
        });
        let seat = seat_config(&value);
        assert_eq!(seat.grants, vec![bare("jinn:clock"), bare("jinn:fs")]);
    }
}
