//! Primitive shim so the swap core's and the seal gate's interleavings run
//! under loom (`--features loom`) and std everywhere else — the jinnd-fiber
//! pattern.

#[cfg(feature = "loom")]
pub(crate) use loom::sync::atomic::AtomicBool;
#[cfg(feature = "loom")]
pub(crate) use loom::sync::{Mutex, MutexGuard};

#[cfg(not(feature = "loom"))]
pub(crate) use std::sync::atomic::AtomicBool;
#[cfg(not(feature = "loom"))]
pub(crate) use std::sync::{Mutex, MutexGuard};
