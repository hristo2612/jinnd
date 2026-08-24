//! The context tree: a cheap, layered view over shared kernel state
//! (SOURCE-OF-TRUTH §3, "Context").
//!
//! A [`Context`] is a handle into a [`ContextTree`]. Cloning a handle is two atomic
//! increments and deriving a child allocates exactly one frozen layer, so derivation
//! is O(1) in the size of the tree. Each layer carries at most two kinds of own
//! binding:
//!
//! * the **isolation map** — service *name* to realm, deciding which realm a name
//!   resolves in;
//! * the **intercept chain** — per-subtree config overlays, read nearest-first
//!   (right-biased, as the paper's metadata monoid requires).
//!
//! Layers are frozen at derivation: a layer never gains or loses own keys afterwards.
//! The TS original likewise only ever adds own keys when a context is derived, so no
//! live-mutation machinery exists here to go wrong.
//!
//! # What this crate is not
//!
//! Pure tree and lookup structure (R10). It stores no services, owns no fibers and
//! dispatches no events; [`Context::resolve`] walks the tree and asks a
//! caller-supplied probe what each frame holds. Nothing here takes a lock on the walk,
//! so a probe never runs under one (R1). No operation can panic on well-formed input,
//! so no panic can reach a caller across the kernel boundary (R11).

#![forbid(unsafe_code)]

mod context;
mod derive;
mod key;
mod layer;
mod resolve;

pub use context::{Context, ContextTree};
pub use derive::Derive;
pub use key::{NameId, RealmId, ServiceKey};
pub use layer::InterceptChain;
pub use resolve::{Probe, ResolutionFrames, Resolved};
