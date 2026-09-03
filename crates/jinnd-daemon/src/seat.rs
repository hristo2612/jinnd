//! One wasm entry's seat configuration, decoded from its profile config
//! document (split from the lane per the under-300 source-file rule, R10).
//! The decoded [`SeatSpec`] is the lifted lane's seat seam (M2-K1).

use jinnd_wasm::{Grant, ScopeValue, SeatSpec};

mod declaration;

pub(crate) use declaration::seat_declaration;

/// Decodes one entry's config document: `grants` are the contracts the
/// profile side grants the instance, each with its optional scope
/// (constitution 01: requests are not grants), `data` is the opaque payload
/// handed to the guest's `activate` (R9: data, never behavior).
///
/// The decoder READS shapes; it never judges authority (round-3 ruling):
/// every entry is preserved — a scope as the written [`ScopeValue`], an
/// entry unreadable as a grant as a fault — so the lane's fail-closed
/// admission point refuses invalid ones ON THE RECORD, never a silent drop
/// here and never a widened unscoped grant.
pub(crate) fn seat_config(value: &serde_json::Value) -> SeatSpec {
    let mut grants = Vec::new();
    let mut faults = Vec::new();
    for entry in value
        .get("grants")
        .and_then(|entries| entries.as_array())
        .into_iter()
        .flatten()
    {
        match grant(entry) {
            Ok(read) => grants.push(read),
            Err(fault) => faults.push(fault),
        }
    }
    let payload = match value.get("data") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::String(text)) => text.clone().into_bytes(),
        Some(other) => other.to_string().into_bytes(),
    };
    SeatSpec {
        grants,
        faults,
        payload,
    }
}

/// One profile grant entry (constitution 04 §Format): a bare contract name,
/// or `{ contract, scope }`. The scope decodes by written shape — a
/// non-negative integer to [`ScopeValue::Rate`], a string to
/// [`ScopeValue::Path`], arrays and objects to their structural shapes
/// (M2-K6), anything else carried as [`ScopeValue::Malformed`]
/// — and the admission point validates it against the contract's declared
/// scope type. An entry unreadable as a grant at all is the fault string
/// the admission point refuses on the record.
fn grant(value: &serde_json::Value) -> Result<Grant, String> {
    match value {
        serde_json::Value::String(contract) => Ok(Grant {
            contract: contract.clone(),
            scope: None,
            ops: None,
        }),
        serde_json::Value::Object(fields) => {
            let Some(contract) = fields.get("contract").and_then(|name| name.as_str()) else {
                return Err(format!("grant entry names no contract: {value}"));
            };
            let scope = match fields.get("scope") {
                None | Some(serde_json::Value::Null) => None,
                Some(scope) => Some(scope_value(scope)),
            };
            // The operation class (M2-K8, harness #24), by written shape,
            // judged at admission like the scope.
            let ops = match fields.get("ops") {
                None | Some(serde_json::Value::Null) => None,
                Some(ops) => Some(scope_value(ops)),
            };
            Ok(Grant {
                contract: contract.to_owned(),
                scope,
                ops,
            })
        }
        other => Err(format!("not a grant entry: {other}")),
    }
}

fn scope_value(value: &serde_json::Value) -> ScopeValue {
    match value {
        serde_json::Value::Number(number) => match number.as_u64() {
            Some(floor) => ScopeValue::Rate(floor),
            None => ScopeValue::Malformed(value.to_string()),
        },
        serde_json::Value::String(path) => ScopeValue::Path(path.clone()),
        // The policy shapes (M2-K6): read structurally, judged at admission.
        serde_json::Value::Array(items) => {
            ScopeValue::List(items.iter().map(scope_value).collect())
        }
        serde_json::Value::Object(fields) => ScopeValue::Map(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), scope_value(value)))
                .collect(),
        ),
        other => ScopeValue::Malformed(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use jinnd_wasm::{Grant, ScopeValue};

    use super::seat_config;

    fn bare(contract: &str) -> Grant {
        Grant {
            contract: contract.to_owned(),
            scope: None,
            ops: None,
        }
    }

    #[test]
    fn seat_config_decodes_grants_and_string_data() {
        let value = serde_json::json!({ "grants": ["demo:clock", "jinn:fs"], "data": "world" });
        let seat = seat_config(&value);
        assert_eq!(seat.grants, vec![bare("demo:clock"), bare("jinn:fs")]);
        assert!(seat.faults.is_empty());
        assert_eq!(seat.payload, b"world".to_vec());
    }

    #[test]
    fn seat_config_defaults_to_no_grants_and_empty_payload() {
        let seat = seat_config(&serde_json::json!({}));
        assert!(seat.grants.is_empty());
        assert!(seat.faults.is_empty());
        assert!(seat.payload.is_empty());
    }

    #[test]
    fn seat_config_serializes_structured_data() {
        let seat = seat_config(&serde_json::json!({ "data": { "a": 1 } }));
        assert_eq!(seat.payload, br#"{"a":1}"#.to_vec());
    }

    /// M2-K2 (constitution 04 §Format): a structured grant entry carries
    /// its scope by written shape — a number is the `rate` shape.
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
                    scope: Some(ScopeValue::Rate(1000)),
                    ops: None,
                }
            ]
        );
    }

    /// Round-3 ruling: the decoder PRESERVES what the profile wrote instead
    /// of judging it — a string scope decodes as the path shape, an
    /// unreadable scope shape is carried as malformed, and an entry that is
    /// no grant at all is carried as a fault. Nothing drops silently; the
    /// admission point refuses each on the record.
    #[test]
    fn seat_config_preserves_every_written_shape_for_admission() {
        let value = serde_json::json!({
            "grants": [
                { "contract": "jinn:clock", "scope": "fine" },
                { "contract": "jinn:clock", "scope": -5 },
                { "scope": 100 },
                7,
            ],
        });
        let seat = seat_config(&value);
        assert_eq!(
            seat.grants,
            vec![
                Grant {
                    contract: "jinn:clock".to_owned(),
                    scope: Some(ScopeValue::Path("fine".to_owned())),
                    ops: None,
                },
                Grant {
                    contract: "jinn:clock".to_owned(),
                    scope: Some(ScopeValue::Malformed("-5".to_owned())),
                    ops: None,
                },
            ],
        );
        assert_eq!(seat.faults.len(), 2, "unreadable entries carry as faults");
    }

    /// The verifier's probe decodes faithfully — a numeric scope on
    /// `jinn:fs` reaches admission as the rate SHAPE, where it refuses
    /// against the declared `path-prefix` type (never a widened bare grant).
    #[test]
    fn seat_config_carries_the_fs_probe_to_admission() {
        let value = serde_json::json!({
            "grants": [{ "contract": "jinn:fs", "scope": 9 }],
        });
        let seat = seat_config(&value);
        assert_eq!(
            seat.grants,
            vec![Grant {
                contract: "jinn:fs".to_owned(),
                scope: Some(ScopeValue::Rate(9)),
                ops: None,
            }],
        );
    }

    /// A structured entry without a scope is the bare grant.
    /// M2-K8 (harness #24): the operation class decodes by written shape
    /// beside the scope; admission judges it.
    #[test]
    fn seat_config_decodes_an_ops_attenuation() {
        let value = serde_json::json!({
            "grants": [{ "contract": "jinn:fs", "scope": "/doc", "ops": ["read", "meta"] }],
        });
        let seat = seat_config(&value);
        assert_eq!(
            seat.grants,
            vec![Grant {
                contract: "jinn:fs".to_owned(),
                scope: Some(ScopeValue::Path("/doc".to_owned())),
                ops: Some(ScopeValue::List(vec![
                    ScopeValue::Path("read".to_owned()),
                    ScopeValue::Path("meta".to_owned()),
                ])),
            }]
        );
    }

    #[test]
    fn seat_config_decodes_a_structured_grant_without_scope() {
        let value = serde_json::json!({
            "grants": [{ "contract": "jinn:clock" }, { "contract": "jinn:fs", "scope": null }],
        });
        let seat = seat_config(&value);
        assert_eq!(seat.grants, vec![bare("jinn:clock"), bare("jinn:fs")]);
        assert!(seat.faults.is_empty());
    }
}
