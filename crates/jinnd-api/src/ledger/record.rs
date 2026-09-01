//! What one appended ledger event looks like once it is DURABLE, and how a
//! reader asks for a slice of them (R6: every record traceable to the entry
//! and fiber that caused it).
//!
//! Split from the event vocabulary by responsibility when `ledger.rs`
//! passed R10's 300-line cap: what an event IS lives beside
//! `LedgerEventKind`; what a RECORD of one carries lives here.

use serde::{Deserialize, Serialize};

use crate::{EntryId, FiberId, LedgerEventKind};

/// Proof that one appended event is durable: the receipt resolves only after
/// the event's commit returned, never before (constitution 02).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Receipt {
    /// The event's monotonic sequence in the device-local stream.
    pub sequence: u64,
}

/// One event as recorded: monotonic sequence, wall-clock timestamp, typed
/// kind, and attribution to the profile entry and/or fiber that caused it
/// (the error→entry rule; the card's timestamped-event requirement).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LedgerRecord {
    pub sequence: u64,
    /// Milliseconds since the Unix epoch, stamped by the writer as the event
    /// commits. Ordering authority stays with `sequence`: wall clocks may
    /// repeat or step; the sequence never does.
    pub timestamp: u64,
    pub kind: LedgerEventKind,
    pub entry: Option<EntryId>,
    pub fiber: Option<FiberId>,
}

/// A ledger read: by entry, by fiber, and/or from a sequence (inclusive).
/// Empty means the whole stream, in sequence order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LedgerQuery {
    pub entry: Option<EntryId>,
    pub fiber: Option<FiberId>,
    pub from_sequence: Option<u64>,
}
