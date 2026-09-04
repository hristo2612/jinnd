//! The closed classification vocabularies that ride on event kinds (R3:
//! typed, never stringly). Split from the kind vocabulary by
//! responsibility when `ledger.rs` passed R10's 300-line cap (M2-K23,
//! COO ruling): what an event IS lives beside `LedgerEventKind`; the
//! closed sets a kind classifies WITH live here.

use serde::{Deserialize, Serialize};

/// Why a grant check refused (M2-K7, harness #19; R3): the closed set of
/// refusal classes every granted surface answers with — the broker's own
/// check and the base providers' per-call scope enforcement alike.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RefusalReason {
    /// The caller holds no grant for the contract at all.
    NotGranted,
    /// A granted contract, but the call's target lies outside the declared
    /// scope: an fs path beside every granted prefix, a port outside the
    /// granted range, a command under no granted exec prefix, a profile
    /// entry the `entry-ids` scope does not admit.
    ScopeMismatch,
    /// `jinn:net`: a bind address that is not loopback (v0.1 binds
    /// loopback only).
    NotLoopback,
    /// `jinn:fs`: a path that cannot be resolved to a containment verdict
    /// (an unreadable ancestor) — refused, never lexically guessed.
    Unresolvable,
    /// A handle minted for another peer: valid only for its owner (R4).
    ForeignHandle,
}

/// The ledgered phases of one Mode-1 hot-swap batch (R8; authorized M1-P8
/// additive delta): begun → per-entry healthy → committed, or rolled back
/// with the old instances still warm.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SwapPhaseKind {
    Began,
    InstanceHealthy,
    Committed,
    RolledBack,
}

/// Which of the five `jinn:profile-admin` writes a `ProfileAdministered`
/// row records (M2-K23; R3). The inverse is another admin write with the
/// row's `prior` as its payload: `Add` ↔ `Remove`, `SetDisabled` ↔ its
/// negation, `SetGrants` ↔ `prior.grants`, `SwapPlugin` ↔ `prior`'s pin.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ProfileWrite {
    Add,
    Remove,
    SetDisabled,
    SetGrants,
    SwapPlugin,
}
