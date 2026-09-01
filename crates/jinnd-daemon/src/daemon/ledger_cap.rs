//! The `jinn:ledger` reader (M2-K7, harness #20; contract bundle
//! `contracts/jinn-ledger`, constitution 02): paged reads of the
//! append-only stream and the high-water sequence, behind the same choke
//! point as every provider. EVERY read appends a CONSUMPTION RECEIPT (02
//! family 2) attributed to the reader — a page's delivered span, a
//! `last-seq`'s consulted mark — and the reader's own receipts are
//! excluded from its feed: a read never feeds itself. Each delivered
//! event is the bundle's declared shape (kind name + JSON-text payload)
//! and carries its sensitivity class so an exporter can redact (02
//! §Redaction: personal is verbatim locally, secret is never stored).

use std::sync::Arc;

use jinnd_api::{EntryId, FiberId, KernelError, KernelFuture, LedgerEventKind, LedgerRecord};
use jinnd_ledger::Ledger;
use jinnd_wasm::{Broker, Peer, PeerId};

use super::storage;
use super::wire::{Callers, Reader, json, unknown};

/// The reader's contract name.
pub(crate) const LEDGER_CONTRACT: &str = "jinn:ledger";
/// The largest page one read delivers (the bundle's declared cap).
const PAGE_CAP: u32 = 500;

pub(crate) struct HostLedger {
    ledger: Ledger,
    callers: Callers,
}

/// The bundle's sensitivity class of one recorded kind: `personal` where
/// the payload may name paths, commands, key names, or an operator's
/// text; `public` for kernel bookkeeping. The `secret` class never
/// reaches storage (02 §Redaction): a `KeystoreAccessed` record is the
/// permitted trace of a secret crossing — its name and digest — and is
/// classed `personal` for export like any other name-bearing record.
fn sensitivity(kind: &LedgerEventKind) -> &'static str {
    match kind {
        LedgerEventKind::ErrorRecorded { .. }
        | LedgerEventKind::AuthDecided { .. }
        | LedgerEventKind::KeystoreAccessed { .. }
        | LedgerEventKind::ProcessSpawned { .. }
        | LedgerEventKind::AmendmentAccepted { .. }
        | LedgerEventKind::AmendmentRefused { .. }
        | LedgerEventKind::WriteBack { .. }
        | LedgerEventKind::ArtifactRefused { .. }
        | LedgerEventKind::GrantRefused { .. } => "personal",
        _ => "public",
    }
}

/// The bundle's `event` exactly as declared (R12: the wire carries the
/// contract's shape): `kind` is the canonical kind name, `payload` its
/// fields as JSON text.
fn event(record: &LedgerRecord) -> serde_json::Value {
    let (kind, payload) = split_kind(&record.kind);
    serde_json::json!({
        "id": record.sequence,
        "wall-ms": record.timestamp,
        "entry": record.entry.as_ref().map(|entry| entry.0.clone()),
        "fiber": record.fiber.map(|fiber| fiber.0),
        "kind": kind,
        "payload": payload,
        "sensitivity": sensitivity(&record.kind),
    })
}

/// A kind is externally tagged: one key naming the variant over its
/// fields (a field-less variant would be a bare name over `null`).
fn split_kind(kind: &LedgerEventKind) -> (String, String) {
    match serde_json::to_value(kind) {
        Ok(serde_json::Value::Object(fields)) if fields.len() == 1 => fields
            .into_iter()
            .next()
            .map_or_else(Default::default, |(name, value)| (name, value.to_string())),
        Ok(serde_json::Value::String(name)) => (name, "null".to_owned()),
        _ => ("Unrepresentable".to_owned(), "null".to_owned()),
    }
}

impl HostLedger {
    /// Registers the reader as a broker peer holding and providing the
    /// contract (providing is authority).
    ///
    /// # Errors
    ///
    /// The broker's refusal of the provision.
    pub(crate) fn register(broker: &Arc<Broker>, ledger: Ledger) -> Result<(), KernelError> {
        let peer = broker.register_peer(None);
        broker.grant(peer, LEDGER_CONTRACT);
        let provider = Arc::new(Self {
            ledger,
            callers: Callers::new(broker, LEDGER_CONTRACT),
        });
        broker.provide(peer, LEDGER_CONTRACT, Arc::new(LedgerPeer(provider)))
    }

    /// One page: `from-id` (u64-LE) and `limit` (u32-LE, clamped to
    /// 1..=500); the answer is the bundle's `page` record. The reader's
    /// own receipts are dropped from the delivery; the receipt for this
    /// read lands after it, under the reader's attribution.
    async fn read_range(&self, caller: PeerId, payload: Vec<u8>) -> Result<Vec<u8>, KernelError> {
        let (fiber, entry) = self.callers.attribution(caller);
        let mut reader = Reader::new(&payload, "ledger read-range");
        let from = reader.u64()?;
        let limit = reader.u32()?.clamp(1, PAGE_CAP);
        let records = self.ledger.page(from, limit).await.map_err(storage)?;
        let next = records.last().map_or(from, |record| record.sequence + 1);
        let delivered: Vec<&LedgerRecord> = records
            .iter()
            .filter(|record| !own_receipt(record, fiber, entry.as_ref()))
            .collect();
        // Every read is on the record: the delivered span, or — for an
        // empty page — the consulted position with nothing delivered.
        let (first, last) = match (delivered.first(), delivered.last()) {
            (Some(first), Some(last)) => (first.sequence, last.sequence),
            _ => (from, from),
        };
        self.ledger.record(
            LedgerEventKind::LedgerConsumed {
                first,
                last,
                count: u32::try_from(delivered.len()).unwrap_or(u32::MAX),
            },
            entry,
            fiber,
        );
        let events: Vec<serde_json::Value> = delivered.iter().map(|record| event(record)).collect();
        Ok(json(
            &serde_json::json!({ "events": events, "next-from": next }),
        ))
    }

    /// The high-water mark. Its receipt IS the record of the read: the
    /// consulted sequence as `first` and `last`, zero events delivered.
    async fn last_seq(&self, caller: PeerId) -> Result<Vec<u8>, KernelError> {
        let (fiber, entry) = self.callers.attribution(caller);
        let last = self.ledger.last_sequence().await.map_err(storage)?;
        self.ledger.record(
            LedgerEventKind::LedgerConsumed {
                first: last,
                last,
                count: 0,
            },
            entry,
            fiber,
        );
        Ok(last.to_le_bytes().to_vec())
    }
}

/// The reader's own consumption receipt: excluded from its feed (02
/// family 2 — recursion prevented without an off-ledger channel).
fn own_receipt(record: &LedgerRecord, fiber: Option<FiberId>, entry: Option<&EntryId>) -> bool {
    matches!(record.kind, LedgerEventKind::LedgerConsumed { .. })
        && record.fiber == fiber
        && record.entry.as_ref() == entry
}

/// The provider's broker face.
struct LedgerPeer(Arc<HostLedger>);

impl Peer for LedgerPeer {
    fn call(
        &self,
        caller: PeerId,
        _contract: &str,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let provider = Arc::clone(&self.0);
        let operation = operation.to_owned();
        Box::pin(async move {
            match operation.as_str() {
                "read-range" => provider.read_range(caller, payload).await,
                "last-seq" => provider.last_seq(caller).await,
                other => Err(unknown(LEDGER_CONTRACT, other)),
            }
        })
    }
}
