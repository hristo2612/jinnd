//! The Tier A WASM host and the transport-agnostic capability broker
//! (M1-P8; R7, R10's wasm-host lane).
//!
//! One broker dispatch point — grant check → ledger append → dispatch — is
//! shared by every transport (decision log 2026-08-25): the conformance
//! harness calls it natively, a component instance calls it over its
//! supervisor channel, and a future Tier B process would call it over a
//! socket. This crate never depends on the harness facade lane: it takes
//! `jinnd-api` with `default-features = false`, so a production build
//! contains no in-proc plugin surface (test-harness ruling closure;
//! prove with `cargo tree -p jinnd-wasm -e features`).

#![forbid(unsafe_code)]
#![cfg_attr(feature = "loom", allow(dead_code))]

mod artifact;
mod bindings;
mod broker;
mod broker_state;
#[cfg(all(test, not(feature = "loom")))]
mod broker_tests;
mod handle;
mod host;
mod hostcaps;
mod instance;
mod peer;
mod selector;
mod sha256;
mod slot;
mod surfaces;
mod swap;
mod sync;
mod topics;

pub use artifact::{PinnedArtifact, admit};
pub use broker::Broker;
pub use handle::{ActivationOutcome, InstanceHandle};
pub use host::{LoadedComponent, WasmHost};
pub use instance::Seat;
pub use peer::{HandleId, LedgerSink, Peer, PeerId};
pub use selector::{NoRealms, RealmOracle, Selector};
pub use sha256::hex_digest;
pub use slot::SharedSlot;
pub use swap::{SlotPhase, SwapCore, SwapOutcome, SwapSlots, swap_batch};
pub use topics::{EmitReport, EventTarget, LocalTopics};
