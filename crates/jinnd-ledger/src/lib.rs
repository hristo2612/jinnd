//! The append-only ledger and the revert lane built over it (R6, Law 2/3;
//! constitution 02/03; M1-P7).
//!
//! One SQLite stream of typed, serde-defined events ([`jinnd_api::LedgerRecord`],
//! R3), each with a monotonic sequence and profile-entry/fiber attribution.
//! The crate is pure-core (R10): it takes a database path or opens in memory,
//! holds no ambient authority and no globals, and persists nothing but what it
//! is handed.
//!
//! **Append-only, physically (constitution 02).** The crate contains no
//! `UPDATE` and no `DELETE` statement; the schema is one insert-only table.
//! The proof is behavioral, not stylistic: a reopened ledger replays the same
//! records in the same order, and nothing in the API can mutate one.
//!
//! **Receipts are proof of durability (constitution 02).** The store commits
//! each appended event — WAL journal, `synchronous=FULL` — before its receipt
//! resolves. This is the *synchronous append* choice the packet card offers:
//! honest and simple over batched-with-flush; a receipt never races its own
//! commit. Boundary sites that cannot await use the ordered, unreceipted
//! [`Ledger::record`] lane, which shares the same single-writer stream and
//! therefore the same ordering — it trades the durability *acknowledgement*
//! away, never the write itself.
//!
//! **Async-first (R1).** rusqlite is a blocking FFI surface, so every
//! connection call is confined to one dedicated writer thread; the async API
//! crosses to it over a channel and awaits a oneshot answer. No executor
//! thread ever blocks on the database, and no lock is held across an await.
//!
//! **Revert (constitution 03).** [`RevertLane`] drives the keyed exactly-once
//! protocol over ledger events and effect inverses: durable intent before the
//! inverse may run, at-most-one inverse execution per branch, the executable
//! witness checked before completion is recorded, and the normative
//! three-state resolution machine — `PendingRevert` is not closable by
//! declaration; only a same-key inverse success with a passing witness makes
//! `Reverted`; an operator-confirmed compensator makes `Compensated`, never
//! `Reverted`, and stays marked unclean unless it satisfies the original
//! witness. Harness-lane honesty: inverse executables and witnesses are
//! in-memory closures here; the serializable inverse-descriptor form is the
//! daemon host's obligation (03 §version stability), out of this packet's
//! scope.

#![forbid(unsafe_code)]
#![cfg_attr(feature = "loom", allow(dead_code))]

mod claim;
mod hydrate;
mod revert;
mod store;
mod writer;

pub use claim::Claim;
pub use revert::{Inverse, RevertLane};
pub use store::{Ledger, LedgerError};
