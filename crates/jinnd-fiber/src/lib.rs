//! The fiber lifecycle engine: one plugin instantiation, supervised (§3, "Fiber").
//!
//! A fiber is the cell a plugin lives in. It holds one activation at a time, it
//! publishes exactly the states it rests in — `Pending / Loading / Active / Failed /
//! Unloading / Disposed` — and it owns the effect scope that activation registered
//! its inverses in. Withdrawing a fiber withdraws exactly that scope, last
//! registered first, and nothing else (I1).
//!
//! Four properties this crate owes the rest of the kernel:
//!
//! * **Async-first (R1).** Each fiber is one tokio task. State is published through
//!   a `watch` channel, targets arrive through a small shared cell whose lock is
//!   never held across an `await`, and the plugin body is never called with a lock
//!   held. There is no blocking executor and no `block_on`.
//! * **Single-flight inertia.** At most one transition is in flight per fiber. A
//!   launched transition always lands: targets that arrive while it runs are
//!   absorbed and reconciled afterwards, so intermediate targets coalesce into the
//!   latest one instead of racing it or being lost. Cancellation is cooperative —
//!   the activation is *told* its target moved and may stop early; nothing is ever
//!   aborted from outside.
//! * **No silent replacement.** An activation is made for one dependency epoch and
//!   keeps it for its whole life. A provider that changes forces a clean unload and
//!   a new activation (§3, "Epoch gating").
//! * **Failure is local (R11), and is not retried (R9).** A body that errors or
//!   panics fails its own fiber, contained, after withdrawing exactly what it had
//!   applied. A failed fiber rests failed until its environment actually changes.
//!
//! # What this crate is not
//!
//! There is no service registry, no epoch computation, no event bus and no profile
//! loader here (R10). Dependency availability arrives as a signal
//! ([`ReadinessSignal`]) that the registry packet implements; this crate ships a
//! watch-backed [`ReadinessSource`] so the engine stands on its own.

#![forbid(unsafe_code)]
// The loom build compiles every concurrency-sensitive decision — the planner, the
// steering cell, and the absorb/staleness path the supervisor drives through it —
// plus the models in `models`. The tokio supervisor itself cannot be expressed in
// loom's primitives (`select!`, `Notify`, `watch`) and is gated out below; it is
// thin by construction, every choice it makes delegated to the modelled code.
#![cfg_attr(feature = "loom", allow(dead_code))]

#[cfg(all(test, feature = "loom"))]
mod models;
mod owed;
mod plan;
mod rest;
mod steering;
mod sync;
mod withdrawal;

#[cfg(not(feature = "loom"))]
mod body;
#[cfg(not(feature = "loom"))]
mod contain;
#[cfg(not(feature = "loom"))]
mod current;
#[cfg(not(feature = "loom"))]
mod fiber;
#[cfg(not(feature = "loom"))]
mod landing;
#[cfg(not(feature = "loom"))]
mod readiness;
#[cfg(not(feature = "loom"))]
mod record;
#[cfg(not(feature = "loom"))]
mod shared;
#[cfg(not(feature = "loom"))]
mod supervisor;
#[cfg(not(feature = "loom"))]
mod teardown;
#[cfg(not(feature = "loom"))]
mod uid;

#[cfg(not(feature = "loom"))]
pub use body::{FaultSink, FiberBody, Setup};
#[cfg(not(feature = "loom"))]
pub use current::current_fiber;
#[cfg(not(feature = "loom"))]
pub use fiber::Fiber;
#[cfg(not(feature = "loom"))]
pub use readiness::{ReadinessSignal, ReadinessSource, Signal, WatchReadiness};
#[cfg(not(feature = "loom"))]
pub use record::FiberRecord;
#[cfg(not(feature = "loom"))]
pub use teardown::in_teardown;
