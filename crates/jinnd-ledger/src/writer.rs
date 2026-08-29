//! The dedicated writer thread: the only place rusqlite's blocking FFI runs.
//!
//! One thread owns the connection for the ledger's whole life. Every operation
//! arrives over the channel and is served in send order, which is what makes
//! the stream's sequence assignment race-free by construction and gives the
//! unreceipted lane its ordering guarantee. No async executor thread ever
//! reaches this file's blocking calls (R1).

use std::path::Path;

use jinnd_api::{LedgerEventKind, Receipt};
use rusqlite::Connection;
use tokio::sync::mpsc;

use crate::store::{Ledger, LedgerError, Op};

mod query;

use query::{last, select};

/// The single insert-only table (constitution 02: physically append-only —
/// this crate contains no UPDATE and no DELETE statement).
const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS events (
    seq   INTEGER PRIMARY KEY AUTOINCREMENT,
    ts    INTEGER NOT NULL,
    entry TEXT,
    fiber INTEGER,
    kind  TEXT NOT NULL
)";

pub(crate) fn open_file(path: &Path) -> Result<Connection, LedgerError> {
    let connection = Connection::open(path).map_err(storage)?;
    // WAL + FULL: an append whose commit returned is on disk; that pair is
    // what lets a receipt stand as proof of durability (constitution 02).
    connection
        .pragma_update(None, "journal_mode", "wal")
        .map_err(storage)?;
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(storage)?;
    connection.execute(SCHEMA, ()).map_err(storage)?;
    Ok(connection)
}

pub(crate) fn open_memory() -> Result<Connection, LedgerError> {
    let connection = Connection::open_in_memory().map_err(storage)?;
    connection.execute(SCHEMA, ()).map_err(storage)?;
    Ok(connection)
}

/// Moves `connection` onto its writer thread and returns the async face.
pub(crate) fn spawn(connection: Connection) -> Result<Ledger, LedgerError> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Op>();
    std::thread::Builder::new()
        .name("jinnd-ledger-writer".to_owned())
        .spawn(move || {
            while let Some(op) = rx.blocking_recv() {
                serve(&connection, op);
            }
        })
        .map_err(|error| LedgerError::Storage(error.to_string()))?;
    Ok(Ledger { tx })
}

fn serve(connection: &Connection, op: Op) {
    match op {
        Op::Append {
            kind,
            entry,
            fiber,
            ack,
        } => {
            let appended = append(connection, &kind, entry.as_ref(), fiber);
            match ack {
                Some(ack) => {
                    let _ = ack.send(appended);
                }
                // The unreceipted lane has no caller to answer: a storage
                // refusal is recorded via the ledger's own honesty path —
                // an `ErrorRecorded` event under the failed append's own
                // attribution — never silently dropped (M2-K2; R6, R11).
                None => {
                    if let Err(refused) = appended {
                        record_refusal(connection, &refused, entry.as_ref(), fiber);
                    }
                }
            }
        }
        Op::Query { query, limit, ack } => {
            let _ = ack.send(select(connection, &query, limit));
        }
        Op::Last { ack } => {
            let _ = ack.send(last(connection));
        }
    }
}

/// The honesty append for a refused unreceipted write. When storage refuses
/// the honesty event too, the ledger cannot testify about itself: the
/// last-resort surface is the process trace, never silence.
fn record_refusal(
    connection: &Connection,
    refused: &LedgerError,
    entry: Option<&jinnd_api::EntryId>,
    fiber: Option<jinnd_api::FiberId>,
) {
    let error = jinnd_api::KernelError {
        code: jinnd_api::ErrorCode::EffectFailed,
        message: format!("unreceipted ledger append failed: {refused}"),
        fiber,
    };
    if let Err(twice) = append(
        connection,
        &LedgerEventKind::ErrorRecorded { error },
        entry,
        fiber,
    ) {
        tracing::error!(refused = %refused, honesty = %twice, "ledger storage refused both an unreceipted append and its honesty event");
    }
}

/// One insert, autocommitted: by the time this returns, the event is through
/// the WAL under `synchronous=FULL` — the receipt the caller gets is honest.
fn append(
    connection: &Connection,
    kind: &LedgerEventKind,
    entry: Option<&jinnd_api::EntryId>,
    fiber: Option<jinnd_api::FiberId>,
) -> Result<Receipt, LedgerError> {
    let encoded =
        serde_json::to_string(kind).map_err(|error| LedgerError::Storage(error.to_string()))?;
    connection
        .execute(
            "INSERT INTO events (ts, entry, fiber, kind) VALUES (?1, ?2, ?3, ?4)",
            (
                now_millis(),
                entry.map(|entry| entry.0.as_str()),
                fiber.map(|fiber| i64::try_from(fiber.0).unwrap_or(i64::MAX)),
                encoded,
            ),
        )
        .map_err(storage)?;
    let sequence = u64::try_from(connection.last_insert_rowid()).unwrap_or(0);
    Ok(Receipt { sequence })
}

/// Milliseconds since the Unix epoch, stamped as the event commits. A clock
/// before the epoch reads 0 rather than failing an append: the timestamp is
/// observational; `seq` alone carries ordering authority.
fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

pub(super) fn storage(error: rusqlite::Error) -> LedgerError {
    LedgerError::Storage(error.to_string())
}

#[cfg(test)]
mod tests;
