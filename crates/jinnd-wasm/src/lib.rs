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

mod alarms;
mod artifact;
mod bindings;
mod broker;
mod broker_state;
#[cfg(all(test, not(feature = "loom")))]
mod broker_tests;
mod entry;
mod grants;
mod handle;
mod host;
mod hostbase;
mod hostcaps;
mod hostclock;
mod hostfs;
mod hostkeystore;
mod hostnet;
mod hostprocess;
mod hostwire;
mod instance;
mod lane;
mod lane_swap;
mod peer;
mod selector;
mod settle;
mod sha256;
mod slot;
mod surfaces;
mod swap;
mod sync;
mod topics;

pub use alarms::{
    AlarmSpec, Alarms, ArmRequest, CLOCK_CONTRACT, DEFAULT_MIN_PERIOD_MS, WAKE_TOPIC, now_unix_ms,
};
pub use artifact::{PinnedArtifact, admit};
pub use broker::Broker;
pub use grants::{
    EnvPolicy, GrantScope, INTROSPECT_CONTRACT, NetScope, PROFILE_CONTRACT, ProcessScope,
    ScopeValue, grant_refusals,
};
pub use handle::{
    ActivationOutcome, AlarmRecord, HostRecord, InstanceHandle, ListenRecord, Registration,
};
pub use host::{LoadedComponent, WasmHost};
pub use hostcaps::{NET_CONTRACT, PROCESS_CONTRACT, registration_label};
pub use hostclock::HostClock;
pub use hostfs::wire::FileMeta;
pub use hostfs::{FS_CONTRACT, HostFs, UndoAction, effect_label};
pub use hostnet::{HostNet, READABLE_TOPIC};
pub use hostprocess::HostProcess;
pub use instance::Seat;
pub use lane::{Grant, LaneCore, SeatSpec, WasmBody, wasm_lane};
pub use lane_swap::swap_pinned;
pub use peer::{HandleId, LedgerSink, Peer, PeerId};
pub use selector::{NoRealms, RealmOracle, Selector};
pub use sha256::hex_digest;
pub use slot::{SeatState, SeatSummary, SharedSlot, commit_staged};
pub use swap::{SlotPhase, SwapCore, SwapOutcome, SwapSlots, swap_batch};
pub use topics::{EmitReport, EventTarget, LocalTopics, Rebind};
