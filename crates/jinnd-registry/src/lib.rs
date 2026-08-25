//! The service registry: typed slots, epochs, and reactive availability
//! (SOURCE-OF-TRUTH §3, "Services (coeffects)" and "Epoch gating").
//!
//! A provider's contribution to the registry is one slot: the value it published,
//! under the service's typed key ([`jinnd_context::ServiceKey`], R3), in the realm
//! its context resolves that name in. Provision is an **effect** on the owning
//! scope (R5): [`Registry::provide`] applies the slot and hands back the
//! [`Disposer`](jinnd_effects::Disposer) that withdraws it — the slot disappears
//! through undo replay, never through ad-hoc mutation.
//!
//! Three properties this crate owes the rest of the kernel:
//!
//! * **Reactive availability (R1).** Consumers never poll. [`InjectedReadiness`]
//!   implements the fiber engine's [`ReadinessSignal`](jinnd_fiber::ReadinessSignal)
//!   over a `watch` channel that is recomputed only when the store actually
//!   changes.
//! * **Epoch gating.** A consumer's epoch is the value-identity of every injected
//!   provider — provider fiber, generation, realm. Any provider change bumps a
//!   generation, which changes the epoch, which forces the consumer through a full
//!   clean unload → reload in the fiber engine. There is no silent replace (R9).
//! * **Provider-waits-for-consumers (I2).** A dying provider's drain phase
//!   removes its slot — no new resolutions, availability withdrawn — and
//!   completes only when every dependent's lease has drained, BEFORE any of
//!   the provider's inverses replay (paper Alg 5), so dependents may still
//!   call the dying service during their own teardown and observe it whole.
//!
//! # What this crate is not
//!
//! It owns no fibers, runs no plugin code, and stores no history (R10): the fiber
//! engine consumes its signals, the effects engine replays its undos, and the
//! ledger packet will persist what they report.

#![forbid(unsafe_code)]
// Under loom only the decision cells and their models compile; the tokio-facing
// layers (store watch, availability, typed registry) are gated out, mirroring
// `jinnd-fiber`'s split between modelled decisions and thin async machinery.
#![cfg_attr(feature = "loom", allow(dead_code))]

mod leases;
#[cfg(all(test, feature = "loom"))]
mod models;
mod slots;
mod sync;
mod vitality;

#[cfg(not(feature = "loom"))]
mod availability;
#[cfg(not(feature = "loom"))]
mod registry;
#[cfg(not(feature = "loom"))]
mod resolver;
#[cfg(not(feature = "loom"))]
mod store;

pub use leases::LeaseCell;

#[cfg(not(feature = "loom"))]
pub use availability::{InjectedReadiness, Injection};
#[cfg(not(feature = "loom"))]
pub use registry::{Provision, Registry};
#[cfg(not(feature = "loom"))]
pub use resolver::ActivationResolver;
#[cfg(not(feature = "loom"))]
pub use store::LeaseGuard;
#[cfg(not(feature = "loom"))]
pub use vitality::Vitality;
