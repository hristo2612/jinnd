//! Types-only contract between the M1 invariant suite and the future kernel.
//!
//! This crate deliberately contains no runtime, storage, scheduling, or test double.
//! Kernel packets implement these traits without changing verifier-owned tests.
//!
//! Layout (M1-P8 pre-work, COO-ordered at M1-P7 round 2): one module per
//! domain, every item re-exported at the crate root so no caller changes —
//! pure extraction, zero semantic change.

#![forbid(unsafe_code)]

mod effect;
mod error;
mod event;
mod fiber;
mod forward;
mod ids;
mod inject;
mod kernel;
mod ledger;
mod plugin;
mod profile;
mod service;

pub use effect::{EffectDescriptor, Undo};
pub use error::{ErrorCode, KernelError, KernelFuture};
pub use event::{DispatchMode, DispatchReport, Event, EventListener};
pub use fiber::{FiberState, Transition, TransitionCause};
pub use forward::{EffectHost, ForwardAction, ForwardEffect};
pub use ids::{ContextId, EffectId, EntryId, FiberId, Generation, Realm};
pub use inject::{Inject, ServiceResolver, ServiceType};
pub use kernel::Kernel;
pub use ledger::{
    LedgerEventKind, LedgerQuery, LedgerRecord, Receipt, RevertKey, RevertResolution, Witness,
};
pub use plugin::{Activation, ActivationReceipt, PluginContract};
pub use profile::{
    EntryFault, GROUP_PACKAGE, IsolationBinding, PluginRef, Profile, ProfileEntry, ReconcileReport,
};
pub use service::{DependencySnapshot, Epoch, ServiceContract, ServiceHandle};
