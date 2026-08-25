//! The primitives the decision cells are built from.
//!
//! Loom can only model concurrency through primitives it owns, so the shared
//! mutable cells in this crate — [`crate::leases::LeaseCell`] and
//! [`crate::slots::SlotMap`] — are written against this shim: `std` in a normal
//! build, `loom` under the model checker (`--features loom`). The shim never
//! changes what the code means.

#[cfg(feature = "loom")]
pub(crate) use loom::sync::Mutex;
#[cfg(not(feature = "loom"))]
pub(crate) use std::sync::Mutex;
