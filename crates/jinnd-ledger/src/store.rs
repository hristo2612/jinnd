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
        /// At most this many records (M2-K7 `jinn:ledger` paging); `None`
        /// is the whole match.
        limit: Option<u32>,
        ack: oneshot::Sender<Result<Vec<LedgerRecord>, LedgerError>>,
    },
    /// The highest committed sequence (0 for an empty stream).
    Last {
        ack: oneshot::Sender<Result<u64, LedgerError>>,
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
    /// A storage refusal on this lane is recorded via the ledger's own
    /// honesty path (an `ErrorRecorded` event, M2-K2); a writer that is
    /// gone can no longer testify, so that loss surfaces on the process
    /// trace — never silently (R6, R11).
    pub fn record(&self, kind: LedgerEventKind, entry: Option<EntryId>, fiber: Option<FiberId>) {
        let refused = self.tx.send(Op::Append {
            kind,
            entry,
            fiber,
            ack: None,
        });
        if let Err(mpsc::error::SendError(Op::Append { kind, .. })) = refused {
            tracing::error!(event = ?kind, "the ledger writer is gone; an unreceipted event was dropped");
        }
    }

    /// Reads records matching `query`, in monotonic sequence order. The read
    /// crosses the same single writer, so every event sent before this call
    /// is visible to it.
    ///
    /// # Errors
    ///
    /// [`LedgerError`] when the writer is gone or storage refused.
    pub async fn events(&self, query: LedgerQuery) -> Result<Vec<LedgerRecord>, LedgerError> {
        self.select(query, None).await
    }

    /// One page of the stream (M2-K7, the `jinn:ledger` reader): at most
    /// `limit` records with sequence ≥ `from`, in sequence order. Paging by
    /// the last delivered sequence plus one walks the whole stream.
    ///
    /// # Errors
    ///
    /// As [`Ledger::events`].
    pub async fn page(&self, from: u64, limit: u32) -> Result<Vec<LedgerRecord>, LedgerError> {
        let query = LedgerQuery {
            from_sequence: Some(from),
            ..LedgerQuery::default()
        };
        self.select(query, Some(limit)).await
    }

    /// The highest committed sequence, 0 for an empty stream — a read
    /// through the single writer, so every append sent before it counts.
    ///
    /// # Errors
    ///
    /// As [`Ledger::events`].
    pub async fn last_sequence(&self) -> Result<u64, LedgerError> {
        let (ack, last) = oneshot::channel();
        self.tx
            .send(Op::Last { ack })
            .map_err(|_| LedgerError::Closed)?;
        last.await.map_err(|_| LedgerError::Closed)?
    }

    async fn select(
        &self,
        query: LedgerQuery,
        limit: Option<u32>,
    ) -> Result<Vec<LedgerRecord>, LedgerError> {
        let (ack, records) = oneshot::channel();
        self.tx
            .send(Op::Query { query, limit, ack })
            .map_err(|_| LedgerError::Closed)?;
        records.await.map_err(|_| LedgerError::Closed)?
    }
}

impl std::fmt::Debug for Ledger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ledger").finish_non_exhaustive()
    }
}
