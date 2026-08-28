//! One wasm entry's seat configuration, decoded from its profile config
//! document (split from the lane per the under-300 source-file rule, R10).
//! The decoded [`SeatSpec`] is the lifted lane's seat seam (M2-K1).

use jinnd_wasm::SeatSpec;

/// Decodes one entry's config document: `grants` are the contract names the
/// profile side grants the instance (constitution 01: requests are not
/// grants), `data` is the opaque payload handed to the guest's `activate`
/// (R9: data, never behavior).
pub(crate) fn seat_config(value: &serde_json::Value) -> SeatSpec {
    let grants = value
        .get("grants")
        .and_then(|grants| grants.as_array())
        .map(|grants| {
            grants
                .iter()
                .filter_map(|grant| grant.as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    let payload = match value.get("data") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(serde_json::Value::String(text)) => text.clone().into_bytes(),
        Some(other) => other.to_string().into_bytes(),
    };
    SeatSpec { grants, payload }
}

#[cfg(test)]
mod tests {
    use super::seat_config;

    #[test]
    fn seat_config_decodes_grants_and_string_data() {
        let value = serde_json::json!({ "grants": ["demo:clock", "jinn:fs"], "data": "world" });
        let seat = seat_config(&value);
        assert_eq!(seat.grants, vec!["demo:clock", "jinn:fs"]);
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
}
