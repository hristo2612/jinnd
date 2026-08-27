//! The reversible-effect engine: the kernel's single mutation primitive
//! (SOURCE-OF-TRUTH §3, "Reversible effects"; R5).
//!
//! A side effect is only allowed to happen if its inverse is registered at the same
//! boundary. [`EffectScope`] holds those inverses as a tree: each record carries a
//! label, one disposer, and the effects registered under it. Teardown replays the
//! tree strictly last-in-first-out, so a child effect is withdrawn before the effect
//! it nested under, and disposing a parent cascades structurally through its whole
//! subtree — no tree-walk by the caller, and nothing left to reconcile afterwards.
//!
//! Three properties this crate owes the rest of the kernel:
//!
//! * **Exactly once.** An inverse is consumed by value when it runs and its record
//!   leaves the tree as it is taken, so there is no second copy to run. Replaying a
//!   scope twice withdraws nothing the second time — and a replay that is dropped
//!   part-way pauses the teardown rather than discharging it: what it never reached
//!   stays live and replayable, and what was in flight is reported, not lost.
//! * **Failure is local (R11).** An inverse that errors or panics is contained,
//!   recorded, and the remaining inverses still run. Replay never aborts on the first
//!   failure (R9) and no panic crosses this crate's boundary.
//! * **Async-first (R1).** Replay is an ordinary future: no blocking executor, no
//!   `block_on`, and no lock is held while an inverse runs — a record is taken out of
//!   the tree as its own inverse starts, leaving the effects behind it untouched and
//!   still live until their turn comes.
//!
//! Replay returns a [`ReplayReport`]: one line per withdrawn effect, in the order
//! their inverses ran. That report is the ledger's future feed (R6); this crate
//! persists nothing.
//!
//! # What this crate is not
//!
//! Pre-fiber and standalone (R10). It owns no fibers, no services and no context
//! wiring; the fiber engine adopts a scope per fiber in the next packet. Cancellation
//! here is a plain [`CancellationToken`](tokio_util::sync::CancellationToken) seam —
//! epoch-checked cancellation arrives with that engine.

#![forbid(unsafe_code)]

mod contain;
mod disposer;
mod drain;
mod forward;
mod report;
mod scope;
mod tree;
mod undo;
mod withdrawal;

pub use disposer::Disposer;
pub use forward::{ForwardAction, ForwardEffect, ForwardEnd, advance, discharge};
pub use report::{EffectReport, ReplayReport, UndoOutcome};
pub use scope::{Detached, EffectScope};
pub use undo::{FutureUndo, StepwiseUndo, UndoStep, step};
