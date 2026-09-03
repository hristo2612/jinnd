//! The `injects` declaration decoder (M2-K24), split from the seat
//! decoder by responsibility (R10 file hygiene).

use jinnd_wasm::Declaration;

/// Decodes one entry's string-lane dependency declaration (M2-K24;
/// constitution 04 §Format): `injects`, beside `grants`, lists the
/// contracts the entry injects at activation — a bare contract name or
/// `{ contract }`, in declaration order (the epoch's identity); absent
/// means the empty list. As with grants the decoder READS shapes: an
/// element it cannot read as a declaration — not a string, an object
/// naming no `contract`, one carrying a scope or ops key (a declaration
/// gates; it carries no authority), or an `injects` that is present but
/// no list (`null` included; absent is the only empty spelling) — is
/// carried as a fault for the lane's admission point to refuse ON THE
/// RECORD (R11, fail closed), never dropped.
pub(crate) fn seat_declaration(value: &serde_json::Value) -> Declaration {
    let mut declaration = Declaration::default();
    match value.get("injects") {
        None => {}
        Some(serde_json::Value::Array(items)) => {
            for item in items {
                match declared(item) {
                    Ok(contract) => declaration.contracts.push(contract),
                    Err(fault) => declaration.faults.push(fault),
                }
            }
        }
        Some(other) => declaration
            .faults
            .push(format!("injects is not a list: {other}")),
    }
    declaration
}

fn declared(value: &serde_json::Value) -> Result<String, String> {
    match value {
        serde_json::Value::String(contract) => Ok(contract.clone()),
        serde_json::Value::Object(fields) => {
            if let Some(key) = fields.keys().find(|key| *key != "contract") {
                return Err(format!(
                    "injects entry carries `{key}`; a declaration names only a contract: {value}"
                ));
            }
            fields
                .get("contract")
                .and_then(|name| name.as_str())
                .map(str::to_owned)
                .ok_or_else(|| format!("injects entry names no contract: {value}"))
        }
        other => Err(format!("not an injects entry: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use jinnd_wasm::Declaration;

    use super::seat_declaration;

    /// M2-K24 (constitution 04 §Format): `injects` beside `grants` — a
    /// bare contract name or `{ contract }`, in declaration order (the
    /// epoch's identity); absent means the empty list.
    #[test]
    fn seat_declaration_decodes_injects_in_declaration_order() {
        let value = serde_json::json!({
            "grants": ["jinn:ui-bundle"],
            "injects": ["jinn:ui-bundle", { "contract": "jinn:fs" }],
        });
        assert_eq!(
            seat_declaration(&value),
            Declaration {
                contracts: vec!["jinn:ui-bundle".to_owned(), "jinn:fs".to_owned()],
                faults: Vec::new(),
            }
        );
        assert_eq!(
            seat_declaration(&serde_json::json!({})),
            Declaration::default()
        );
    }

    /// A malformed element — not a string, an object naming no
    /// `contract`, a scope or ops key (a declaration gates; it carries no
    /// authority), or an `injects` that is no list — is carried as a
    /// fault for admission to refuse on the record, never dropped (R11).
    #[test]
    fn seat_declaration_carries_every_malformed_element_as_a_fault() {
        let value = serde_json::json!({
            "injects": [7, { "scope": 1 }, { "contract": "jinn:fs", "ops": ["read"] }, "jinn:ok"],
        });
        let declaration = seat_declaration(&value);
        assert_eq!(declaration.contracts, ["jinn:ok"]);
        assert_eq!(declaration.faults.len(), 3, "{:?}", declaration.faults);
        // A present non-list faults, `null` included (round-1 ruling 3):
        // absent means the empty list; written means it must be a list.
        for written in [serde_json::json!("jinn:fs"), serde_json::Value::Null] {
            let not_a_list = seat_declaration(&serde_json::json!({ "injects": written }));
            assert!(not_a_list.contracts.is_empty());
            assert_eq!(not_a_list.faults.len(), 1, "{written} is no list");
        }
    }
}
