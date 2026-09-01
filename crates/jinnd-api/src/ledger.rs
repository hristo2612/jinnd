//! Ledger read surface and revert entry points (authorized M1-P7 additive
//! delta; R3, R6, Law 2/3, constitution 02/03).
//!
//! These are contract types only: storage, sequencing, and the revert state
//! machine live behind the kernel boundary. Every kernel-boundary event lands
//! as one typed [`LedgerRecord`] with monotonic sequence and profile-entry
//! attribution; appends answer with a [`Receipt`] that is proof of durability.

use serde::{Deserialize, Serialize};

use crate::{DispatchMode, EffectId, EntryId, FiberId, KernelError, Owed, Transition};

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
    /// One fiber's seat suspended (M2-K4; Law 2): its kernel registrations
    /// released, `retained` world effects carried into the entry's live
    /// journal for the next incarnation — what happened is "suspended",
    /// never "withdrawn" (authorized suspend-vs-dispose facade delta).
    FiberSuspended { retained: u64 },
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
    /// One event-bus emit crossed the kernel (Law 2; the M1-P7 reserved
    /// class, wired at M2-K2 with its trace schema): the topic (a typed
    /// event's type name, or a byte-lane topic string), the declared
    /// dispatch mode, how many listeners the payload selected, and how many
    /// contained listener failures the walk recorded (R9 — failures are
    /// observed, never aborted on). `emitter` is the emitting context;
    /// entry/fiber attribution rides the record's own columns. Exactly one
    /// trace per emit; the append never alters dispatch outcomes (R11).
    DispatchTrace {
        topic: String,
        mode: DispatchMode,
        listeners: u32,
        failures: u32,
        emitter: u64,
    },
    /// Committed fiber transitions the kernel could NOT hand to its
    /// lifecycle publish queue (M2-K13; Law 2, R9): a listener slow enough
    /// to fill the bound loses transitions, and the loss is COUNTED here
    /// rather than absorbed. `topic` is the reserved topic the publish was
    /// for; `dropped` is how many transitions were lost. A listener sees
    /// exactly the same loss as a gap in the delivered `ordinal`, so the
    /// kernel's count and the listener's own reading never disagree.
    PublishDropped { topic: String, dropped: u64 },
    /// One `jinn:process` child spawned by its calling plugin (M2-K6; Law
    /// 2): a kernel registration, attributed to the calling fiber/entry
    /// via the record's columns. `pid` is the host process id.
    ProcessSpawned {
        handle: u64,
        command: String,
        pid: u32,
    },
    /// The host reaped one `jinn:process` child; a signal termination is
    /// the negated signal number (M2-K6).
    ProcessExited { handle: u64, code: i32 },
    /// One signal delivered to a `jinn:process` child — by the guest, or
    /// by the kernel releasing the registration (M2-K6).
    ProcessKilled { handle: u64, signal: String },
    /// The kernel killed a `jinn:process` child and its reap did not land
    /// within the bound (M2-K6 round 2; Law 2): never silent — the host
    /// task finishes the reap and `ProcessExited` follows when it lands.
    ProcessReapPending { handle: u64 },
    /// One `jinn:process` `run` produced more than the bundle's declared
    /// total-output cap (M2-K6 round 3; R9): the answer is a typed
    /// truncation, the read end is cut, the child killed and reaped.
    ProcessOutputTruncated { handle: u64, cap: u64 },
    /// One `jinn:net` loopback listener bound (M2-K6): a kernel
    /// registration, attributed like a spawn.
    NetListening { handle: u64, port: u16 },
    /// One connection accepted on a `jinn:net` listener (M2-K6).
    NetAccepted { listener: u64, handle: u64 },
    /// One `jinn:net` listener or connection closed — by the guest, or by
    /// the kernel releasing the registration (M2-K6).
    NetClosed { handle: u64 },
    /// One `jinn:net` OUTBOUND REQUEST that was sent (M2-K14; Law 2 vs
    /// constitution 02 §Redaction). The record is the call's SHAPE, never
    /// its content: no body and no header — an `Authorization` header
    /// carries exactly the credential the keystore exists to protect — and
    /// `path` stops at `?`, because a query string carries one just as
    /// readily. `status` is 0 when no response was read. The effect is
    /// declared IRREVERSIBLE: this row is the only trace a sent request
    /// can ever leave, so it is written whether the call succeeded or the
    /// response failed. (Authorized M2-K14 additive facade delta.)
    NetRequested {
        method: String,
        host: String,
        path: String,
        status: u16,
        request_bytes: u64,
        response_bytes: u64,
        duration_ms: u64,
    },
    /// One `jinn:net` readiness wake delivered to the plugin holding the
    /// socket (M2-K7, harness #23; Law 2): the listener has a pending
    /// connection, or the connection has bytes or EOF — one wake per
    /// readiness transition the guest has not yet acted on, never one per
    /// byte (R9). Ledgered exactly like `AlarmWake`.
    NetReadable { handle: u64 },
    /// One profile entry patched through `jinn:profile` (M2-K7, harness
    /// #21; Law 2, constitution 04): applied BY THE LOADER as operator
    /// intent — no fs inverse, no fiber journal entry. `entry` is the
    /// patched entry; `by` names the editing entry (or the calling peer).
    ProfilePatched { entry: EntryId, by: String },
    /// One `jinn:keystore` crossing (M2-K8, harness #5 remainder; Law 2,
    /// constitution 02 §Redaction — sensitivity class SECRET): the record
    /// carries the KEY NAME and the value's SHA-256 digest, never the
    /// value. `digest` is `None` when no value crossed (an absent key, a
    /// delete). Attributed to the calling entry/fiber via the columns.
    /// (Authorized M2-K8 additive facade delta.)
    KeystoreAccessed {
        operation: String,
        key: String,
        digest: Option<String>,
    },
    /// One consumption receipt for a `jinn:ledger` read (constitution 02,
    /// family 2; M2-K7 #20): the delivered sequence span and count,
    /// attributed to the reading entry/fiber — and excluded from that
    /// reader's own feed, so a read never feeds itself. A `last-seq` read
    /// delivers no events: its receipt is the consulted mark, count 0.
    LedgerConsumed { first: u64, last: u64, count: u32 },
    /// One `jinn:clock` alarm wake delivered to its requesting plugin
    /// (M2-K2; Law 2): every wake is a ledger event, attributed to the
    /// requesting fiber via the record's columns.
    AlarmWake { alarm: u64 },
    /// A reply-expecting dispatch was refused BEFORE any listener ran
    /// (M2-K9, harness finding 31; Law 2): `target`'s live incarnation
    /// already owes `owed`, so the walk never landed in it. Its own kind —
    /// a reader tells a dispatch refusal from a scope refusal by the kind
    /// alone, and one refusal from another by `owed`. A refused walk
    /// lands this row INSTEAD of a `DispatchTrace`. (Authorized delta.)
    DispatchRefused {
        topic: String,
        mode: DispatchMode,
        target: EntryId,
        incarnation: u64,
        owed: Owed,
    },
    /// A crossing was refused because it would CLOSE A WAIT CYCLE
    /// (M2-K10, harness finding 32; Law 2): `target` is, transitively,
    /// already awaiting the refused caller, so parking on it could only
    /// end at the guest deadline with both ends dead. Its own kind, like
    /// [`LedgerEventKind::DispatchRefused`]: a cycle is neither a pending
    /// transition nor a scope denial, and a reader tells them apart by the
    /// kind alone. The refused caller rides the record's own entry/fiber
    /// columns; `through` is the wait path from `target` back to it, in
    /// wait order. A refused crossing lands this row INSTEAD of the
    /// `ContractCall` or `DispatchTrace` it would have written.
    /// (Authorized M2-K10 additive delta.)
    CycleRefused {
        /// `"contract.operation"` for a contract call; the topic for a
        /// dispatch.
        on: String,
        target: FiberId,
        target_entry: Option<EntryId>,
        through: Vec<FiberId>,
    },
    /// A capability grant was exercised: the broker resolved a granted
    /// contract to a caller-scoped handle (constitution 01 §Grants;
    /// authorized M1-P8 additive delta).
    ContractResolved { contract: String },
    /// The grant check refused a resolution — every denial is a ledger event
    /// (constitution 01 §Grants; authorized M1-P8 additive delta). `reason`
    /// is the typed refusal class (M2-K7, harness #19: the record carries
    /// WHY, not only WHAT — R3, typed not stringly); `detail` is the prose
    /// the caller received, riding beside the class, never instead of it.
    GrantRefused {
        contract: String,
        reason: RefusalReason,
        detail: Option<String>,
    },
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
