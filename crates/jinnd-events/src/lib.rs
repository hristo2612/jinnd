//! The event bus: one bus, five dispatch modes, inverted routing
//! (SOURCE-OF-TRUTH §3, "Events").
//!
//! Events are typed and carry their dispatch mode in their type
//! ([`jinnd_api::Event`], R3). Routing is inverted: the emitted payload selects
//! its listeners by interrogating each listener's registration context — a
//! listener never filters the payload. Registering a listener is just an effect
//! (R5): [`EventBus::listen`] applies the registration and hands back the
//! idempotent [`Registration`] the effects engine wraps as the undo.
//!
//! Three properties this crate owes the rest of the kernel:
//!
//! * **No abort, no false bail (R9, mechanically).** Every listener call is
//!   panic-contained (R11) and its outcome recorded; an erroring listener never
//!   aborts the remaining walk in a collecting mode. Bail awaits each result and
//!   asks the payload whether the *resolved value* is decisive — a pending async
//!   result is never treated as bailed.
//! * **Async-first (R1).** Dispatch is an ordinary future. The listener set is
//!   snapshotted before the walk, so registration during dispatch can neither
//!   deadlock nor invalidate the iteration, and no lock is held across a
//!   listener call — every table operation returns owned data.
//! * **At-most-once delivery for once-listeners.** A once-registration is
//!   claimed by removing it under the table's one lock before its call: of any
//!   number of concurrent dispatches, exactly one observes the removal.
//!
//! # What this crate is not
//!
//! Pure bus (R10): it owns no fibers, no services, and no context wiring. The
//! adapter charges registrations to an effect scope and resolves contexts; this
//! crate sees only their identities.

#![forbid(unsafe_code)]
// Under loom only the listener table and its models compile; the tokio-facing
// dispatch layers are gated out, mirroring `jinnd-registry`'s split between
// modelled decisions and thin async machinery.
#![cfg_attr(feature = "loom", allow(dead_code))]

#[cfg(all(test, feature = "loom"))]
mod models;
mod sync;
mod table;

#[cfg(not(feature = "loom"))]
mod bus;
#[cfg(not(feature = "loom"))]
mod contain;
#[cfg(not(feature = "loom"))]
mod dispatch;

#[cfg(not(feature = "loom"))]
pub use bus::{EventBus, Registration};
