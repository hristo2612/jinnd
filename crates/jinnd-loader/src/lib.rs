//! The profile loader: a config document and the running system are two views
//! of one truth (LAW §3, "Profiles & loader").
//!
//! The crate is the pure core the packet card names: parse (`document`), diff
//! (`plan`), and apply (`Loader::reconcile`) against a kernel seam
//! ([`PackageLane`] / [`EntryHandle`]) plus atomic persistence ([`FileStore`]).
//! File watching and the fs host-provider contract are deliberately absent —
//! the daemon wires those later through this seam (R10).

#![forbid(unsafe_code)]

mod apply;
mod diff;
mod document;
mod lanes;
mod loader;
mod proxy;
mod store;
mod tree;

pub use diff::{Plan, Step, StepKind, plan};
pub use document::{Document, DocumentEntry};
pub use lanes::{EntryHandle, PackageLane, SpawnFn, SpawnRequest};
pub use loader::{LaneConfig, Loader};
pub use store::FileStore;
