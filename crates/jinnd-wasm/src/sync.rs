//! Primitive shim so the swap core's interleavings run under loom
//! (`--features loom`) and std everywhere else — the jinnd-fiber pattern.

#[cfg(feature = "loom")]
pub(crate) use loom::sync::Mutex;

#[cfg(not(feature = "loom"))]
pub(crate) use std::sync::Mutex;
