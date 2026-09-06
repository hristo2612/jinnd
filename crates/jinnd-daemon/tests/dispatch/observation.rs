//! Correlate one guest request with the existing ledger's sequence order.
//! Only the provider emits this topic, serially, between its fs markers.
//! A receipt names the payload, even if it lands outside that interval or
//! in a different incarnation. Earlier deliveries (including in the same
//! incarnation) cannot be mistaken for this request.

use std::path::Path;

use jinnd_api::{LedgerEventKind, LedgerRecord};

pub(super) fn attempt(data: &Path, name: &str) -> String {
    let id =
        std::fs::read_to_string(data.join(name)).unwrap_or_else(|error| panic!("{name}: {error}"));
    assert!(id.strip_prefix("dispatch-").is_some_and(|number| {
        !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
    }));
    id
}

fn marker(record: &LedgerRecord, path: &str) -> bool {
    matches!(&record.kind, LedgerEventKind::EffectRegistered { label }
        if label.starts_with(&format!("fs write {path} [effect ")))
}

pub(super) fn walk<'a>(records: &'a [LedgerRecord], id: &str) -> Vec<&'a LedgerRecord> {
    let boundary = |suffix| {
        let found: Vec<_> = records
            .iter()
            .filter(|record| {
                record
                    .entry
                    .as_ref()
                    .is_some_and(|entry| entry.0 == "provider")
                    && marker(record, &format!("{id}-{suffix}"))
            })
            .collect();
        assert_eq!(found.len(), 1, "one {suffix} marker for {id}: {found:?}");
        found[0].sequence
    };
    let (begin, end) = (boundary("begin"), boundary("end"));
    assert!(begin < end, "ordered request markers for {id}");
    records
        .iter()
        .filter(|record| begin < record.sequence && record.sequence < end)
        .collect()
}

pub(super) fn no_delivery(
    records: &[LedgerRecord],
    data: &Path,
    id: &str,
) -> Result<(), &'static str> {
    let path = format!("{id}-received");
    if data.join(&path).exists() || records.iter().any(|record| marker(record, &path)) {
        Err("request reached a listener")
    } else {
        Ok(())
    }
}

pub(super) fn no_trace(walk: &[&LedgerRecord]) -> Result<(), &'static str> {
    if walk.iter().any(|record| {
        matches!(&record.kind,
            LedgerEventKind::DispatchTrace { topic, .. } if topic == "jinn:test/settings-changed"
        )
    }) {
        Err("request dispatched a traced walk")
    } else {
        Ok(())
    }
}
