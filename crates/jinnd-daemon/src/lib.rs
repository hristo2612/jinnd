//! The `jinnd` daemon library: the production kernel assembly the binary
//! shell drives, exposed so the headless acceptance demo (`tests/demo/`) can
//! drive the exact same five steps without a terminal (M1-P9).
//!
//! Assembly (R10 — a shell, not a feature): profile path → loader →
//! registry/fiber/effects/events → ledger (real SQLite path) → wasm host
//! through the transport-agnostic broker. The conformance-harness lane is
//! not compiled into this crate: `jinnd-api` arrives without the `harness`
//! feature and `jinnd-adapter` is never a dependency (Law 1; test-harness
//! ruling 2026-08-25).

#![forbid(unsafe_code)]

mod daemon;
mod packages;
mod paths;
mod seat;
mod support;
mod swap;
mod watch;

pub use daemon::{Daemon, UnitMember};
pub use jinnd_wasm::{MasterKeySource, PASSPHRASE_ENV, PASSPHRASE_FILE_ENV, SwapOutcome};
pub use paths::DaemonPaths;
pub use watch::{Watch, log_report, log_status};
