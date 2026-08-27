//! Ledger read surface and revert entry points (authorized M1-P7 additive
//! delta; R3, R6, Law 2/3, constitution 02/03).
//!
//! These are contract types only: storage, sequencing, and the revert state
//! machine live behind the kernel boundary. Every kernel-boundary event lands
//! as one typed [`LedgerRecord`] with monotonic sequence and profile-entry
//! attribution; appends answer with a [`Receipt`] that is proof of durability.

use serde::{Deserialize, Serialize};

use crate::{EffectId, EntryId, FiberId, KernelError, Transition};

/// Proof that one appended event is durable: the receipt resolves only after
/// the event's commit returned, never before (constitution 02).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Receipt {
    /// The event's monotonic sequence in the device-local stream.
    pub sequence: u64,
}

/// The typed kernel-boundary event families of v0.1 (R3; constitution 02).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LedgerEventKind {
    /// A reversible effect's inverse became live at the kernel boundary.
    EffectRegistered { label: String },
    /// A withdrawal outcome: `clean` is false when any inverse did not
    /// complete — recorded, never a `last_error` string (R6).
    EffectWithdrawn { label: String, clean: bool },
    /// One committed fiber lifecycle transition.
    FiberTransition(Transition),
    /// A service value was provided into a realm.
    ServiceProvided { service: String },
    /// A service value was withdrawn from its realm.
    ServiceWithdrawn { service: String },
    /// A runtime-originated amendment was accepted and committed.
    AmendmentAccepted { detail: String },
    /// A runtime-originated amendment was refused; refusals are events too.
    AmendmentRefused { detail: String },
    /// The document of record was written back atomically.
    WriteBack { detail: String },
    /// A recorded error, attributed to the entry/fiber that caused it.
    ErrorRecorded { error: KernelError },
    /// Revert intent, durably recorded before any inverse runs (03 step 1).
    /// Carries the effect the branch concerns: the branch is reconstructible
    /// from the ledger alone (crash-safe exactly-once).
    RevertIntent { key: String, effect: EffectId },
    /// One inverse completion under its idempotency key; `clean` is true only
    /// when the executable witness passed (03 step 3).
    RevertCompleted {
        key: String,
        effect: EffectId,
        clean: bool,
    },
    /// A revert branch reached a resolution state.
    RevertResolved {
        effect: EffectId,
        resolution: RevertResolution,
    },
    /// RESERVED (M1-P7): the event-bus dispatch trace class. The variant and
    /// schema exist now; emission is wired when the bus gains its ledger tap.
    DispatchTrace { event: String },
    /// A capability grant was exercised: the broker resolved a granted
    /// contract to a caller-scoped handle (constitution 01 §Grants;
    /// authorized M1-P8 additive delta).
    ContractResolved { contract: String },
    /// The grant check refused a resolution — every denial is a ledger event
    /// (constitution 01 §Grants; authorized M1-P8 additive delta).
    GrantRefused { contract: String },
    /// One contract call crossed the broker's single dispatch point
    /// (Law 2, R6; decision log 2026-08-25; authorized M1-P8 additive delta).
    ContractCall { contract: String, operation: String },
    /// A call through a handle whose provider generation has changed was
    /// refused — epoch gating at the call site: there is no silent
    /// replacement, ever (R9; authorized M1-P8 additive delta, round-2
    /// blocker 2).
    StaleHandleRefused { contract: String },
    /// A component artifact was admitted under its pinned content hash
    /// (Law 5, constitution 05 pin-by-hash; authorized M1-P8 additive delta).
    ArtifactLoaded { hash: String },
    /// An artifact was refused — hash mismatch or malformed component; the
    /// refusal is recorded, never silent (authorized M1-P8 additive delta).
    ArtifactRefused { detail: String },
    /// One phase of a Mode-1 hot-swap (R8): every phase is a ledger event
    /// (authorized M1-P8 additive delta).
    SwapPhase {
        artifact: String,
        phase: SwapPhaseKind,
    },
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

/// One event as recorded: monotonic sequence, wall-clock timestamp, typed
/// kind, and attribution to the profile entry and/or fiber that caused it
/// (the error→entry rule; the card's timestamped-event requirement).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LedgerRecord {
    pub sequence: u64,
    /// Milliseconds since the Unix epoch, stamped by the writer as the event
    /// commits. Ordering authority stays with `sequence`: wall clocks may
    /// repeat or step; the sequence never does.
    pub timestamp: u64,
    pub kind: LedgerEventKind,
    pub entry: Option<EntryId>,
    pub fiber: Option<FiberId>,
}

/// A ledger read: by entry, by fiber, and/or from a sequence (inclusive).
/// Empty means the whole stream, in sequence order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LedgerQuery {
    pub entry: Option<EntryId>,
    pub fiber: Option<FiberId>,
    pub from_sequence: Option<u64>,
}

/// The idempotency key of one revert operation: a same-key retry of a
/// recorded completion returns that outcome without re-running the inverse,
/// a same-key retry of a crash-interrupted branch resumes it — running the
/// inverse to completion under this key — and a distinct key against the
/// same branch is refused (constitution 03, keyed exactly-once).
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct RevertKey(pub String);

/// The executable observational witness checked after an inverse or a
/// compensator: true when the contract's declared equality relation holds
/// against the pre-state (constitution 03).
pub type Witness = std::sync::Arc<dyn Fn() -> bool + Send + Sync + 'static>;

/// The normative v0.1 resolution states of a revert branch (constitution 03):
/// `PendingRevert` is not closable by declaration; it resolves only via
/// same-key inverse success with a passing witness (→ `Reverted`) or an
/// operator-confirmed declared compensator (→ `Compensated`, never
/// `Reverted`). A compensation that does not satisfy the original witness
/// leaves the branch marked unclean: `clean` is false.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RevertResolution {
    PendingRevert,
    Reverted,
    Compensated { clean: bool },
}
