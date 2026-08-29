//! The writer thread's read statements (split from `writer.rs` by
//! responsibility, R10 file hygiene): the filtered, ordered, bounded
//! select behind every read, and the high-water sequence (M2-K7 paging).

use jinnd_api::{LedgerEventKind, LedgerQuery, LedgerRecord};
use rusqlite::Connection;

use super::storage;
use crate::store::LedgerError;

pub(super) fn last(connection: &Connection) -> Result<u64, LedgerError> {
    let last: Option<i64> = connection
        .query_row("SELECT MAX(seq) FROM events", (), |row| row.get(0))
        .map_err(storage)?;
    Ok(last.and_then(|seq| u64::try_from(seq).ok()).unwrap_or(0))
}

/// `limit` is a SQL bound: `None` reads the whole match (a negative LIMIT
/// is "no limit" to SQLite).
pub(super) fn select(
    connection: &Connection,
    query: &LedgerQuery,
    limit: Option<u32>,
) -> Result<Vec<LedgerRecord>, LedgerError> {
    let mut statement = connection
        .prepare(
            "SELECT seq, ts, entry, fiber, kind FROM events
             WHERE (?1 IS NULL OR entry = ?1)
               AND (?2 IS NULL OR fiber = ?2)
               AND (?3 IS NULL OR seq >= ?3)
             ORDER BY seq
             LIMIT ?4",
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
                limit.map_or(-1, i64::from),
            ),
            |row| {
                let sequence: i64 = row.get(0)?;
                let timestamp: i64 = row.get(1)?;
                let entry: Option<String> = row.get(2)?;
                let fiber: Option<i64> = row.get(3)?;
                let kind: String = row.get(4)?;
                Ok((sequence, timestamp, entry, fiber, kind))
            },
        )
        .map_err(storage)?;

    let mut records = Vec::new();
    for row in rows {
        let (sequence, timestamp, entry, fiber, kind) = row.map_err(storage)?;
        let kind: LedgerEventKind =
            serde_json::from_str(&kind).map_err(|error| LedgerError::Storage(error.to_string()))?;
        records.push(LedgerRecord {
            sequence: u64::try_from(sequence).unwrap_or(0),
            timestamp: u64::try_from(timestamp).unwrap_or(0),
            kind,
            entry: entry.map(jinnd_api::EntryId),
            fiber: fiber.and_then(|fiber| u64::try_from(fiber).ok().map(jinnd_api::FiberId)),
        });
    }
    Ok(records)
}
