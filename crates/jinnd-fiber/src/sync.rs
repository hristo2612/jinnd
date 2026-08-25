//! The primitives the steering cell is built from.
//!
//! Loom can only model concurrency through primitives it owns, so the one piece of
//! shared mutable state in this crate — [`crate::steering::SteeringCell`] — is
//! written against this shim: `std` in a normal build, `loom` under the model
//! checker (`--features loom`). Nothing else in the crate uses it, and the shim
//! never changes what the code means.

#[cfg(feature = "loom")]
pub(crate) use loom::sync::Mutex;
#[cfg(not(feature = "loom"))]
pub(crate) use std::sync::Mutex;
