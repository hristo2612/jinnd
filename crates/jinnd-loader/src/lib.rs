//! The profile loader: a config document and the running system are two views
//! of one truth (LAW §3, "Profiles & loader").
//!
//! The crate is the pure core the packet card names: parse (`document`), diff
//! (`plan`), and apply (`Loader::reconcile`) against a kernel seam
//! ([`PackageLane`] / [`EntryHandle`]) plus atomic persistence ([`FileStore`]).
//! File watching and the fs host-provider contract are deliberately absent —
//! the daemon wires those later through this seam (R10).

#![forbid(unsafe_code)]

mod amend;
mod apply;
mod cycles;
mod diff;
mod dispose;
mod document;
mod file_store;
mod gate;
pub mod host;
mod lanes;
mod loader;
#[cfg(all(test, feature = "loom"))]
mod models;
mod proxy;
mod rebind;
mod refuse;
mod state;
mod store;
mod tree;

pub use diff::{Attestation, Plan, Step, StepKind, plan};
pub use document::{Document, DocumentEntry, RawEntry};
pub use file_store::FileStore;
pub use lanes::{EntryHandle, PackageLane, SpawnFn, SpawnRequest};
pub use loader::{LaneConfig, Loader};
pub use store::DocumentStore;
