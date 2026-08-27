//! The dedicated writer thread: the only place rusqlite's blocking FFI runs.
//!
//! One thread owns the connection for the ledger's whole life. Every operation
//! arrives over the channel and is served in send order, which is what makes
//! the stream's sequence assignment race-free by construction and gives the
//! unreceipted lane its ordering guarantee. No async executor thread ever
//! reaches this file's blocking calls (R1).

use std::path::Path;

use jinnd_api::{LedgerEventKind, LedgerQuery, LedgerRecord, Receipt};
use rusqlite::Connection;
use tokio::sync::mpsc;

use crate::store::{Ledger, LedgerError, Op};

/// The single insert-only table (constitution 02: physically append-only —
/// this crate contains no UPDATE and no DELETE statement).
const SCHEMA: &str = "CREATE TABLE IF NOT EXISTS events (
    seq   INTEGER PRIMARY KEY AUTOINCREMENT,
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
            if let Some(ack) = ack {
                let _ = ack.send(appended);
            }
        }
        Op::Query { query, ack } => {
            let _ = ack.send(select(connection, &query));
        }
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
            "INSERT INTO events (entry, fiber, kind) VALUES (?1, ?2, ?3)",
            (
                entry.map(|entry| entry.0.as_str()),
                fiber.map(|fiber| i64::try_from(fiber.0).unwrap_or(i64::MAX)),
                encoded,
            ),
        )
        .map_err(storage)?;
    let sequence = u64::try_from(connection.last_insert_rowid()).unwrap_or(0);
    Ok(Receipt { sequence })
}

fn select(connection: &Connection, query: &LedgerQuery) -> Result<Vec<LedgerRecord>, LedgerError> {
    let mut statement = connection
        .prepare(
            "SELECT seq, entry, fiber, kind FROM events
             WHERE (?1 IS NULL OR entry = ?1)
               AND (?2 IS NULL OR fiber = ?2)
               AND (?3 IS NULL OR seq >= ?3)
             ORDER BY seq",
        )
        .map_err(storage)?;
    let rows = statement
        .query_map(
            (
                query.entry.as_ref().map(|entry| entry.0.as_str()),
                query
                    .fiber
                    .map(|fiber| i64::try_from(fiber.0).unwrap_or(i64::MAX)),
                query
                    .from_sequence
                    .map(|sequence| i64::try_from(sequence).unwrap_or(i64::MAX)),
            ),
            |row| {
                let sequence: i64 = row.get(0)?;
                let entry: Option<String> = row.get(1)?;
                let fiber: Option<i64> = row.get(2)?;
                let kind: String = row.get(3)?;
                Ok((sequence, entry, fiber, kind))
            },
        )
        .map_err(storage)?;

    let mut records = Vec::new();
    for row in rows {
        let (sequence, entry, fiber, kind) = row.map_err(storage)?;
        let kind: LedgerEventKind =
            serde_json::from_str(&kind).map_err(|error| LedgerError::Storage(error.to_string()))?;
        records.push(LedgerRecord {
            sequence: u64::try_from(sequence).unwrap_or(0),
            kind,
            entry: entry.map(jinnd_api::EntryId),
            fiber: fiber.and_then(|fiber| u64::try_from(fiber).ok().map(jinnd_api::FiberId)),
        });
    }
    Ok(records)
}

fn storage(error: rusqlite::Error) -> LedgerError {
    LedgerError::Storage(error.to_string())
}
