//! The async face of the single-writer store.

use std::path::Path;

use jinnd_api::{EntryId, FiberId, LedgerEventKind, LedgerQuery, LedgerRecord, Receipt};
use tokio::sync::{mpsc, oneshot};

use crate::writer;

/// Why a ledger operation could not be served.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LedgerError {
    /// The writer is gone: the ledger was dropped or its thread ended.
    Closed,
    /// The storage layer refused; the message is SQLite's, verbatim.
    Storage(String),
}

impl std::fmt::Display for LedgerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => f.write_str("the ledger writer is closed"),
            Self::Storage(message) => write!(f, "ledger storage error: {message}"),
        }
    }
}

impl std::error::Error for LedgerError {}

/// One operation crossing to the writer thread.
pub(crate) enum Op {
    Append {
        kind: LedgerEventKind,
        entry: Option<EntryId>,
        fiber: Option<FiberId>,
        /// `None` is the ordered, unreceipted lane: the write still happens,
        /// in channel order; only the durability acknowledgement is waived.
        ack: Option<oneshot::Sender<Result<Receipt, LedgerError>>>,
    },
    Query {
        query: LedgerQuery,
        ack: oneshot::Sender<Result<Vec<LedgerRecord>, LedgerError>>,
    },
}

/// One device-local append-only event stream (R6, Law 2).
///
/// Clones share the writer. Dropping every clone ends the writer thread after
/// it drains what was already sent — receipted appends were durable before
/// their receipts resolved, so an abrupt drop loses no acknowledged event.
#[derive(Clone)]
pub struct Ledger {
    pub(crate) tx: mpsc::UnboundedSender<Op>,
}

impl Ledger {
    /// Opens (creating if needed) the ledger at `path`: WAL journal,
    /// `synchronous=FULL`, one insert-only table.
    ///
    /// This constructor performs blocking I/O and belongs in kernel
    /// construction, never on an async path (R1).
    ///
    /// # Errors
    ///
    /// [`LedgerError::Storage`] when SQLite cannot open or migrate the file.
    pub fn open(path: &Path) -> Result<Self, LedgerError> {
        writer::spawn(writer::open_file(path)?)
    }

    /// Opens an in-memory ledger: same semantics, no device durability —
    /// the harness lane's store, and the unit under most tests.
    ///
    /// # Errors
    ///
    /// [`LedgerError::Storage`] when SQLite cannot initialize.
    pub fn open_in_memory() -> Result<Self, LedgerError> {
        writer::spawn(writer::open_memory()?)
    }

    /// Appends one event and resolves with its receipt only after the commit
    /// returned (constitution 02: a receipt is proof of durability).
    ///
    /// # Errors
    ///
    /// [`LedgerError`] when the writer is gone or storage refused.
    pub async fn append(
        &self,
        kind: LedgerEventKind,
        entry: Option<EntryId>,
        fiber: Option<FiberId>,
    ) -> Result<Receipt, LedgerError> {
        let (ack, receipt) = oneshot::channel();
        self.tx
            .send(Op::Append {
                kind,
                entry,
                fiber,
                ack: Some(ack),
            })
            .map_err(|_| LedgerError::Closed)?;
        receipt.await.map_err(|_| LedgerError::Closed)?
    }

    /// Appends one event on the ordered, unreceipted lane: the write shares
    /// the single writer and lands in send order relative to every other
    /// append and query, but no durability acknowledgement returns. For
    /// boundary sites that cannot await (R1); the receipted lane is
    /// [`Ledger::append`].
    pub fn record(
        &self,
        kind: LedgerEventKind,
        entry: Option<EntryId>,
        fiber: Option<FiberId>,
    ) {
        let _ = self.tx.send(Op::Append {
            kind,
            entry,
            fiber,
            ack: None,
        });
    }

    /// Reads records matching `query`, in monotonic sequence order. The read
    /// crosses the same single writer, so every event sent before this call
    /// is visible to it.
    ///
    /// # Errors
    ///
    /// [`LedgerError`] when the writer is gone or storage refused.
    pub async fn events(&self, query: LedgerQuery) -> Result<Vec<LedgerRecord>, LedgerError> {
        let (ack, records) = oneshot::channel();
        self.tx
            .send(Op::Query { query, ack })
            .map_err(|_| LedgerError::Closed)?;
        records.await.map_err(|_| LedgerError::Closed)?
    }
}

impl std::fmt::Debug for Ledger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ledger").finish_non_exhaustive()
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use jinnd_api::{EntryId, ErrorCode, FiberId, KernelError, LedgerEventKind, LedgerQuery};

    use super::Ledger;

    fn open() -> Ledger {
        Ledger::open_in_memory().unwrap_or_else(|error| panic!("open: {error}"))
    }

    fn entry(id: &str) -> Option<EntryId> {
        Some(EntryId(id.to_owned()))
    }

    #[tokio::test]
    async fn receipts_are_monotonic_and_events_replay_in_order() {
        let ledger = open();
        let mut last = 0;
        for label in ["one", "two", "three"] {
            let receipt = ledger
                .append(
                    LedgerEventKind::EffectRegistered {
                        label: label.to_owned(),
                    },
                    None,
                    None,
                )
                .await
                .unwrap_or_else(|error| panic!("append: {error}"));
            assert!(receipt.sequence > last, "sequence must be monotonic");
            last = receipt.sequence;
        }
        let records = ledger
            .events(LedgerQuery::default())
            .await
            .unwrap_or_else(|error| panic!("events: {error}"));
        assert_eq!(records.len(), 3);
        assert!(records.windows(2).all(|w| w[0].sequence < w[1].sequence));
    }

    #[tokio::test]
    async fn a_reopened_ledger_replays_identically() {
        let dir = std::env::temp_dir().join(format!("jinnd-ledger-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap_or_else(|error| panic!("mkdir: {error}"));
        let path = dir.join("ledger.sqlite3");

        let before = {
            let ledger =
                Ledger::open(&path).unwrap_or_else(|error| panic!("open: {error}"));
            for index in 0..3 {
                // Receipted: durable before the acknowledgement resolves, so
                // the abrupt drop below is the process-death simulation.
                ledger
                    .append(
                        LedgerEventKind::WriteBack {
                            detail: format!("commit {index}"),
                        },
                        entry("persisted"),
                        None,
                    )
                    .await
                    .unwrap_or_else(|error| panic!("append: {error}"));
            }
            ledger
                .events(LedgerQuery::default())
                .await
                .unwrap_or_else(|error| panic!("events: {error}"))
            // The ledger drops here with no orderly shutdown.
        };

        let reopened = Ledger::open(&path).unwrap_or_else(|error| panic!("reopen: {error}"));
        let after = reopened
            .events(LedgerQuery::default())
            .await
            .unwrap_or_else(|error| panic!("events after reopen: {error}"));
        assert_eq!(before, after, "a reopened ledger replays identically");
        std::fs::remove_dir_all(&dir).unwrap_or_else(|error| panic!("cleanup: {error}"));
    }

    #[tokio::test]
    async fn queries_attribute_errors_to_their_entry() {
        let ledger = open();
        ledger
            .append(
                LedgerEventKind::ErrorRecorded {
                    error: KernelError {
                        code: ErrorCode::PluginFailed,
                        message: "the entry failed".to_owned(),
                        fiber: None,
                    },
                },
                entry("failing-entry"),
                Some(FiberId(7)),
            )
            .await
            .unwrap_or_else(|error| panic!("append: {error}"));
        ledger
            .append(
                LedgerEventKind::WriteBack {
                    detail: "unrelated".to_owned(),
                },
                entry("other-entry"),
                None,
            )
            .await
            .unwrap_or_else(|error| panic!("append: {error}"));

        let by_entry = ledger
            .events(LedgerQuery {
                entry: entry("failing-entry"),
                ..LedgerQuery::default()
            })
            .await
            .unwrap_or_else(|error| panic!("events: {error}"));
        assert_eq!(by_entry.len(), 1);
        assert!(matches!(
            by_entry[0].kind,
            LedgerEventKind::ErrorRecorded { .. }
        ));

        let by_fiber = ledger
            .events(LedgerQuery {
                fiber: Some(FiberId(7)),
                ..LedgerQuery::default()
            })
            .await
            .unwrap_or_else(|error| panic!("events: {error}"));
        assert_eq!(by_fiber.len(), 1);

        let from = ledger
            .events(LedgerQuery {
                from_sequence: Some(by_entry[0].sequence + 1),
                ..LedgerQuery::default()
            })
            .await
            .unwrap_or_else(|error| panic!("events: {error}"));
        assert_eq!(from.len(), 1, "from_sequence is inclusive");
    }

    #[tokio::test]
    async fn the_unreceipted_lane_is_ordered_with_receipted_appends() {
        let ledger = open();
        ledger.record(
            LedgerEventKind::ServiceProvided {
                service: "jinn.test/first".to_owned(),
            },
            None,
            None,
        );
        ledger
            .append(
                LedgerEventKind::ServiceProvided {
                    service: "jinn.test/second".to_owned(),
                },
                None,
                None,
            )
            .await
            .unwrap_or_else(|error| panic!("append: {error}"));
        let records = ledger
            .events(LedgerQuery::default())
            .await
            .unwrap_or_else(|error| panic!("events: {error}"));
        let services: Vec<&str> = records
            .iter()
            .map(|record| match &record.kind {
                LedgerEventKind::ServiceProvided { service } => service.as_str(),
                other => panic!("unexpected kind: {other:?}"),
            })
            .collect();
        assert_eq!(services, ["jinn.test/first", "jinn.test/second"]);
    }

    #[tokio::test]
    async fn the_reserved_dispatch_trace_class_appends_and_replays() {
        let ledger = open();
        ledger
            .append(
                LedgerEventKind::DispatchTrace {
                    event: "jinn.test/reserved".to_owned(),
                },
                None,
                None,
            )
            .await
            .unwrap_or_else(|error| panic!("append: {error}"));
        let records = ledger
            .events(LedgerQuery::default())
            .await
            .unwrap_or_else(|error| panic!("events: {error}"));
        assert!(matches!(
            records[0].kind,
            LedgerEventKind::DispatchTrace { .. }
        ));
    }
}
