//! Stable kernel identities (pre-work extraction, M1-P8; zero semantic change).

use serde::{Deserialize, Serialize};

/// Stable identity of a context in one kernel process.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContextId(pub u64);

/// Stable identity of a fiber while it is live.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct FiberId(pub u64);

/// Stable identity of a reversible effect. Serde exists so ledger events can
/// carry the effect they concern (R3, R6; authorized M1-P7 additive delta).
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct EffectId(pub u64);

/// Stable identity of a profile entry across reconciliations.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct EntryId(pub String);

/// Provider generation. Values are monotonic and never reused for one service slot.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Generation(pub u64);

/// Realm identity used to isolate typed service slots.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Realm {
    Root,
    Local(EntryId),
    Shared(String),
}
