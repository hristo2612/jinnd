//! Types-only contract between the M1 invariant suite and the future kernel.
//!
//! This crate deliberately contains no runtime, storage, scheduling, or test double.
//! Kernel packets implement these traits without changing verifier-owned tests.
//!
//! Layout (M1-P8 pre-work, COO-ordered at M1-P7 round 2): one module per
//! domain, every item re-exported at the crate root so no caller changes —
//! pure extraction, zero semantic change.
//!
//! The `harness` feature (default-on) carries the conformance-harness lane —
//! the in-proc [`Kernel`] and [`WasmLane`] facade traits. Production builds
//! take this crate with `default-features = false`: the harness lane is
//! compile-gated out of them (test-harness ruling 2026-08-25; Law 1).

#![forbid(unsafe_code)]

mod effect;
mod error;
mod event;
mod fiber;
mod forward;
mod ids;
mod inject;
#[cfg(feature = "harness")]
mod kernel;
mod ledger;
mod plugin;
mod profile;
mod service;
#[cfg(feature = "harness")]
mod wasm;

pub use effect::{EffectDescriptor, Undo};
pub use error::{ErrorCode, KernelError, KernelFuture};
pub use event::{DispatchMode, DispatchReport, Event, EventListener};
pub use fiber::{FiberState, Owed, Transition, TransitionCause};
pub use forward::{EffectHost, ForwardAction, ForwardEffect};
pub use ids::{ContextId, EffectId, EntryId, FiberId, Generation, Realm};
pub use inject::{Inject, ServiceResolver, ServiceType};
#[cfg(feature = "harness")]
pub use kernel::Kernel;
pub use ledger::{
    LedgerEventKind, LedgerQuery, LedgerRecord, ProfileWrite, Receipt, RefusalReason, RevertKey,
    RevertResolution, SwapPhaseKind, Witness,
};
pub use plugin::{Activation, ActivationReceipt, PluginContract};
pub use profile::{
    EntryFault, GROUP_PACKAGE, IsolationBinding, PluginRef, Profile, ProfileEntry, ReconcileReport,
};
pub use service::{DependencySnapshot, Epoch, ServiceContract, ServiceHandle};
#[cfg(feature = "harness")]
pub use wasm::{SwapReport, WasmArtifact, WasmLane};
