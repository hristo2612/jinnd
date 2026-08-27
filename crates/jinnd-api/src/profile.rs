//! Profile documents, entries, and reconcile observations (pre-work
//! extraction, M1-P8; zero semantic change).

use crate::{EntryId, KernelError, Realm};

/// A dynamic plugin reference at the profile boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PluginRef {
    pub package: String,
    pub version: String,
    pub artifact_hash: String,
}

/// The reserved package naming a pure grouping entry: it spawns no fiber and
/// exists to carry children, disablement, and isolation directives (authorized
/// M1-P6 additive delta; LAW §3 "Profiles & loader").
pub const GROUP_PACKAGE: &str = "jinn.profile/group";

/// One contained per-entry failure of a reconciliation (R11: good entries
/// load, bad entries surface recorded errors; authorized M1-P6 additive delta).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntryFault {
    pub entry: EntryId,
    pub error: KernelError,
}

/// Isolation mapping applied to one profile entry or group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IsolationBinding {
    pub service: String,
    pub realm: Realm,
}

/// Typed profile entry used by reconcile-by-id.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProfileEntry<C> {
    pub id: EntryId,
    pub plugin: PluginRef,
    pub config: C,
    pub disabled: bool,
    pub parent: Option<EntryId>,
    pub isolation: Vec<IsolationBinding>,
}

/// Ordered profile document.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Profile<C> {
    pub entries: Vec<ProfileEntry<C>>,
}

/// Observable result of one profile reconciliation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReconcileReport {
    pub created: Vec<EntryId>,
    pub restarted: Vec<EntryId>,
    pub disposed: Vec<EntryId>,
    pub unchanged: Vec<EntryId>,
    /// Contained per-entry faults (R11); never a whole-reconcile failure
    /// (authorized M1-P6 additive delta).
    pub errors: Vec<EntryFault>,
}
