//! The primitive the listener table is built from.
//!
//! Loom can only model concurrency through primitives it owns, so the one
//! shared mutable cell in this crate — [`crate::table::ListenerTable`] — is
//! written against this shim: `std` in a normal build, `loom` under the model
//! checker (`--features loom`). The shim never changes what the code means.

#[cfg(feature = "loom")]
pub(crate) use loom::sync::Mutex;
#[cfg(not(feature = "loom"))]
pub(crate) use std::sync::Mutex;
