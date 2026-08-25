//! The loader object: one committed document, one runtime per entry, and the
//! operations that keep them two views of one truth (LAW §3).

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use jinnd_api::{
    EntryFault, EntryId, ErrorCode, FiberId, FiberState, KernelError, Profile, ProfileEntry,
    ReconcileReport,
};
use jinnd_context::Context;
use jinnd_registry::Registry;
use tokio_util::sync::CancellationToken;

use crate::lanes::PackageLane;
use crate::state::{State, error, lock};

/// A config type usable at the loader's typed boundary (R3): profile config
/// payloads are plain data, never behavior (R9) — exactly the facade's
/// `reconcile` bound (R12). Equality is attested separately, at lane
/// registration, where the concrete type is statically known.
pub trait LaneConfig: Clone + std::fmt::Debug + Send + Sync + 'static {}
impl<C: Clone + std::fmt::Debug + Send + Sync + 'static> LaneConfig for C {}

/// One config type's erased equality attestation: the type's own `PartialEq`,
/// applied under its `TypeId`.
pub(crate) type ConfigEq =
    Arc<dyn Fn(&(dyn Any + Send + Sync), &(dyn Any + Send + Sync)) -> bool + Send + Sync>;

/// The profile loader over one kernel assembly.
pub struct Loader {
    pub(crate) root: Context<()>,
    pub(crate) registry: Registry,
    pub(crate) on_context: Box<dyn Fn(Context<()>) + Send + Sync>,
    pub(crate) lanes: Mutex<HashMap<(String, TypeId), Arc<PackageLane>>>,
    /// Per config type, the equality attestation the diff compares under.
    pub(crate) eqs: Mutex<HashMap<TypeId, ConfigEq>>,
    pub(crate) state: Mutex<State>,
    /// The attached write-back store, if any (see [`Loader::attach_store`]).
    pub(crate) persist: Mutex<Option<Arc<crate::store::Persistence>>>,
    /// Admits reconcile/update/dispose single-flight. Admission, not a lock:
    /// no guard is held while plan steps run plugin-facing callbacks, and a
    /// re-entrant call from a callback is refused honestly (R1, M1-P6b).
    pub(crate) gate: crate::gate::Gate,
}

impl Loader {
    /// A loader deriving entry contexts under `root`, resolving readiness and
    /// provider realms through `registry`, and reporting every context it mints
    /// through `on_context`.
    pub fn new(
        root: Context<()>,
        registry: Registry,
        on_context: impl Fn(Context<()>) + Send + Sync + 'static,
    ) -> Self {
        Self {
            root,
            registry,
            on_context: Box::new(on_context),
            lanes: Mutex::new(HashMap::new()),
            eqs: Mutex::new(HashMap::new()),
            state: Mutex::new(State::default()),
            persist: Mutex::new(None),
            gate: crate::gate::Gate::new(),
        }
    }

    /// Registers the lane that instantiates `package` entries whose config
    /// payload is `C` (R3's string-keyed dynamic lane), and with it `C`'s
    /// equality attestation — the comparator reconcile-by-id diffs under.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] when the package is already registered for
    /// that config type — silent replacement stays dead (R9).
    pub fn register_lane<C: LaneConfig + PartialEq>(
        &self,
        package: &str,
        lane: PackageLane,
    ) -> Result<(), KernelError> {
        {
            let mut lanes = lock(&self.lanes);
            let key = (package.to_owned(), TypeId::of::<C>());
            if lanes.contains_key(&key) {
                return Err(error(
                    ErrorCode::InvalidProfile,
                    &format!("package {package:?} is already registered for this config type"),
                ));
            }
            lanes.insert(key, Arc::new(lane));
        }
        // The attestation is the type's own equality; it outlives the lane —
        // a type's equality does not change when a lane retires.
        lock(&self.eqs).entry(TypeId::of::<C>()).or_insert_with(|| {
            Arc::new(
                |a, b| match (a.downcast_ref::<C>(), b.downcast_ref::<C>()) {
                    (Some(a), Some(b)) => a == b,
                    _ => false,
                },
            )
        });
        Ok(())
    }

    /// Withdraws one registered lane; idempotent. The config type's equality
    /// attestation is deliberately retained.
    pub fn unregister_lane<C: 'static>(&self, package: &str) {
        lock(&self.lanes).remove(&(package.to_owned(), TypeId::of::<C>()));
    }

    /// Reconciles the runtime onto `profile` (see [`Loader::reconcile_with`]).
    ///
    /// # Errors
    ///
    /// As [`Loader::reconcile_with`].
    pub async fn reconcile<C: LaneConfig>(
        &self,
        profile: Profile<C>,
    ) -> Result<ReconcileReport, KernelError> {
        self.reconcile_with(profile, CancellationToken::new()).await
    }

    /// Reconciles by id: diffs the applied document against `profile`, commits
    /// `profile` as the document of record, and applies the minimal plan —
    /// only affected entries are touched. Application is cancellable between
    /// steps (R1): a cancelled reconcile leaves a consistent prefix applied,
    /// and the next reconcile converges on the document (I4).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] when `C` differs from the config type the
    /// loader is already committed to, when the attached store cannot
    /// write the document back — nothing is committed then — or when called
    /// re-entrantly from a loader operation's own callback (refused, never
    /// deadlocked; R1). Per-entry problems are not errors: they are contained
    /// faults in the report (R11).
    pub async fn reconcile_with<C: LaneConfig>(
        &self,
        profile: Profile<C>,
        cancel: CancellationToken,
    ) -> Result<ReconcileReport, KernelError> {
        self.gate
            .admit(self.reconcile_admitted(profile, cancel))
            .await
    }

    /// The admitted body of [`Loader::reconcile_with`]: runs single-flight,
    /// with no lock guard held across plan steps (R1, M1-P6b).
    async fn reconcile_admitted<C: LaneConfig>(
        &self,
        profile: Profile<C>,
        cancel: CancellationToken,
    ) -> Result<ReconcileReport, KernelError> {
        let old = self.applied::<C>()?;
        let erased = lock(&self.eqs).get(&TypeId::of::<C>()).cloned();
        let attested = erased.map(|eq| {
            move |a: &C, b: &C| eq(a as &(dyn Any + Send + Sync), b as &(dyn Any + Send + Sync))
        });
        let plan = crate::diff::plan(
            old.as_ref(),
            &profile,
            attested.as_ref().map(|eq| eq as &dyn Fn(&C, &C) -> bool),
        );
        let committed = Arc::new(profile.clone()) as Arc<dyn Any + Send + Sync>;
        // The document of record moves to disk before the runtime (LAW §3).
        self.persist(&committed).await?;
        // Committing the new document reconverges every recorded divergence:
        // the drained faults surface in the report, never dropped (LAW §3).
        let drained: Vec<EntryFault> = {
            let mut state = lock(&self.state);
            state.config_type = Some(TypeId::of::<C>());
            state.committed = Some(committed);
            state
                .entries
                .iter_mut()
                .filter_map(|(entry, runtime)| {
                    runtime.fault.take().map(|error| EntryFault {
                        entry: entry.clone(),
                        error,
                    })
                })
                .collect()
        };
        let mut report = self.apply(plan, &profile, &cancel).await;
        report.errors.extend(drained);
        self.settle().await;
        Ok(report)
    }

    /// The fiber currently hosting `entry`, if any.
    #[must_use]
    pub fn entry_fiber(&self, entry: &EntryId) -> Option<FiberId> {
        lock(&self.state)
            .entries
            .get(entry)?
            .live
            .as_ref()
            .map(|live| live.handle.id())
    }

    /// The last committed state of one loader-owned fiber.
    #[must_use]
    pub fn fiber_state(&self, fiber: FiberId) -> Option<FiberState> {
        lock(&self.state)
            .entries
            .values()
            .filter_map(|runtime| runtime.live.as_ref())
            .find(|live| live.handle.id() == fiber)
            .map(|live| live.handle.state())
    }

    /// The recorded divergence faults: entries whose last amendment
    /// double-failed, leaving the runtime and the committed document honestly
    /// apart until a reconcile reconverges them. After any failed amendment
    /// exactly one of three states holds: both views at the prior state, both
    /// at the new state, or a fault here naming the divergence (LAW §3).
    #[must_use]
    pub fn entry_faults(&self) -> Vec<EntryFault> {
        lock(&self.state)
            .entries
            .iter()
            .filter_map(|(entry, runtime)| {
                runtime.fault.clone().map(|error| EntryFault {
                    entry: entry.clone(),
                    error,
                })
            })
            .collect()
    }

    /// The committed document, as persisted.
    #[must_use]
    pub fn persisted<C: LaneConfig>(&self) -> Option<Profile<C>> {
        let committed = lock(&self.state).committed.clone()?;
        committed.downcast_ref::<Profile<C>>().cloned()
    }

    /// Settles every loader-owned fiber (see [`Loader::settle`]).
    pub async fn quiesce(&self) {
        self.settle().await;
    }

    /// The applied document reconstructed from entry runtimes.
    fn applied<C: LaneConfig>(&self) -> Result<Option<Profile<C>>, KernelError> {
        let state = lock(&self.state);
        if let Some(existing) = state.config_type
            && existing != TypeId::of::<C>()
        {
            return Err(error(
                ErrorCode::InvalidProfile,
                "the loader is committed to a different config type",
            ));
        }
        if state.entries.is_empty() {
            return Ok(None);
        }
        let mut entries = Vec::with_capacity(state.entries.len());
        for runtime in state.entries.values() {
            let Some(spec) = runtime.spec.downcast_ref::<ProfileEntry<C>>() else {
                return Err(error(
                    ErrorCode::InvalidProfile,
                    "an applied entry holds a foreign config type",
                ));
            };
            entries.push(spec.clone());
        }
        Ok(Some(Profile { entries }))
    }
}
