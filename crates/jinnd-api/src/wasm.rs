//! The conformance-harness surface of the Tier A wasm lane (authorized
//! M1-P8 additive delta; R7, R8, Law 5).
//!
//! Like [`crate::Kernel`], this is harness-lane vocabulary: the verifier's
//! invariant suite drives wasm-backed profile entries and the capability
//! broker through it. It is compile-gated out of production builds with the
//! rest of the harness lane (test-harness ruling, 2026-08-25).

use crate::{EffectId, EntryId, KernelError, KernelFuture};

/// One component artifact offered to the kernel, pinned by content hash
/// (constitution 05: v0.1 pins exact hashes; a mismatch refuses to load —
/// Law 5's provenance floor for M1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WasmArtifact {
    /// The component's bytes.
    pub bytes: Vec<u8>,
    /// Lower-hex SHA-256 the bytes must match, exactly.
    pub expected_hash: String,
}

/// The observable outcome of one Mode-1 hot-swap batch (R8).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SwapReport {
    /// Entries whose instances were replaced, in commit order.
    pub swapped: Vec<EntryId>,
    /// True when the batch rolled back: every entry kept its old instance.
    pub rolled_back: bool,
}

/// Harness-lane driver for the Tier A host and the capability broker.
///
/// The harness lane and the wasm lane share ONE broker dispatch point
/// (decision log 2026-08-25): `broker_resolve`/`broker_call` make the
/// harness itself a broker peer, so verifier cases can pin that a native
/// caller and a guest caller cross the same choke point with the same
/// ledger events.
pub trait WasmLane: Send + Sync + 'static {
    /// Registers a wasm-backed package lane: profile entries referencing
    /// `package` instantiate `artifact` — one component instance per fiber,
    /// disposed instantly and completely with it (R7, I1). The artifact is
    /// admitted only under its pinned hash; a mismatch refuses registration,
    /// recorded (Law 5). `grants` are the names each such entry's instance
    /// holds authority over: the contracts it may resolve, provide, or call
    /// through a host-provider import, and the topics it may listen to —
    /// subscriptions are covered by the contract grant in v0.1
    /// (constitution 01: requests are not grants; the profile side grants).
    ///
    /// # Errors
    ///
    /// [`crate::ErrorCode::InvalidProfile`] on duplicate registration or a
    /// refused artifact.
    fn register_wasm_package(
        &self,
        package: &str,
        artifact: WasmArtifact,
        grants: Vec<String>,
    ) -> Result<EffectId, KernelError>;

    /// Grants the harness peer itself the named contract. Without it,
    /// `broker_resolve` refuses — refusal is the pinned behavior, not a
    /// harness convenience (constitution 01 §Grants).
    fn broker_grant(&self, contract: &str);

    /// Resolves `contract` as the harness peer, over the same broker
    /// dispatch point the wasm lane uses. Returns an opaque handle.
    ///
    /// # Errors
    ///
    /// The broker's refusal, exactly as a guest would observe it.
    fn broker_resolve(&self, contract: &str) -> Result<u64, KernelError>;

    /// Calls one operation on a resolved handle: grant-checked, ledgered,
    /// dispatched — the single choke point (R6, Law 2).
    fn broker_call(
        &self,
        handle: u64,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'_, Vec<u8>>;

    /// Mode-1 hot-swap (R8): replaces the artifact behind every live entry
    /// whose package lane is pinned to `old_hash` — the batch is by artifact
    /// hash, never per entry. Old instances stay warm until every new one
    /// reports healthy; any failure rolls the whole batch back. Every phase
    /// is a ledger event.
    fn swap_wasm_artifact(
        &self,
        old_hash: &str,
        artifact: WasmArtifact,
    ) -> KernelFuture<'_, SwapReport>;
}
