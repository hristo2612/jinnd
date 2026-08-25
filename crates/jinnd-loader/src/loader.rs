//! The loader object: one committed document, one runtime per entry, and the
//! operations that keep them two views of one truth (LAW §3).

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use jinnd_api::{
    EntryId, ErrorCode, FiberId, FiberState, KernelError, Profile, ProfileEntry, ReconcileReport,
    TransitionCause,
};
use jinnd_context::Context;
use jinnd_registry::Registry;
use tokio_util::sync::CancellationToken;

use crate::lanes::{EntryHandle, PackageLane};
use crate::proxy::ReadinessProxy;

/// A config type usable at the loader's typed boundary (R3): profile config
/// payloads are plain comparable data, never behavior (R9).
pub trait LaneConfig: Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static {}
impl<C: Clone + std::fmt::Debug + PartialEq + Send + Sync + 'static> LaneConfig for C {}

/// One entry's runtime.
pub(crate) struct EntryRuntime {
    /// The applied spec, an `Arc<ProfileEntry<C>>`.
    pub(crate) spec: Arc<dyn Any + Send + Sync>,
    /// The entry's derived context while it is effectively enabled.
    pub(crate) context: Option<Context<()>>,
    /// The spawned fiber while the entry is an enabled plugin.
    pub(crate) live: Option<Live>,
}

pub(crate) struct Live {
    pub(crate) lane: Arc<PackageLane>,
    pub(crate) handle: Arc<dyn EntryHandle>,
    pub(crate) proxy: ReadinessProxy,
}

#[derive(Default)]
pub(crate) struct State {
    /// The one config type this loader has been driven with.
    pub(crate) config_type: Option<TypeId>,
    /// The committed document, an `Arc<Profile<C>>` — the persisted view.
    pub(crate) committed: Option<Arc<dyn Any + Send + Sync>>,
    pub(crate) entries: HashMap<EntryId, EntryRuntime>,
}

/// The profile loader over one kernel assembly.
pub struct Loader {
    pub(crate) root: Context<()>,
    pub(crate) registry: Registry,
    pub(crate) on_context: Box<dyn Fn(Context<()>) + Send + Sync>,
    pub(crate) lanes: Mutex<HashMap<(String, TypeId), Arc<PackageLane>>>,
    pub(crate) state: Mutex<State>,
    /// Serializes reconcile/update/dispose. Held across plan application —
    /// loader operations are never reachable from plugin code, so no lock is
    /// ever held across a call into a plugin (R1).
    pub(crate) gate: tokio::sync::Mutex<()>,
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
            state: Mutex::new(State::default()),
            gate: tokio::sync::Mutex::new(()),
        }
    }

    /// Registers the lane that instantiates `package` entries whose config
    /// payload is `config` (R3's string-keyed dynamic lane).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] when the package is already registered for
    /// that config type — silent replacement stays dead (R9).
    pub fn register_lane(
        &self,
        package: &str,
        config: TypeId,
        lane: PackageLane,
    ) -> Result<(), KernelError> {
        let mut lanes = lock(&self.lanes);
        let key = (package.to_owned(), config);
        if lanes.contains_key(&key) {
            return Err(error(
                ErrorCode::InvalidProfile,
                &format!("package {package:?} is already registered for this config type"),
            ));
        }
        lanes.insert(key, Arc::new(lane));
        Ok(())
    }

    /// Withdraws one registered lane; idempotent.
    pub fn unregister_lane(&self, package: &str, config: TypeId) {
        lock(&self.lanes).remove(&(package.to_owned(), config));
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
    /// loader is already committed to. Per-entry problems are not errors: they
    /// are contained faults in the report (R11).
    pub async fn reconcile_with<C: LaneConfig>(
        &self,
        profile: Profile<C>,
        cancel: CancellationToken,
    ) -> Result<ReconcileReport, KernelError> {
        let _gate = self.gate.lock().await;
        let old = self.applied::<C>()?;
        let plan = crate::diff::plan(old.as_ref(), &profile);
        {
            let mut state = lock(&self.state);
            state.config_type = Some(TypeId::of::<C>());
            state.committed = Some(Arc::new(profile.clone()) as Arc<dyn Any + Send + Sync>);
        }
        let report = self.apply(plan, &profile, &cancel).await;
        self.settle().await;
        Ok(report)
    }

    /// A runtime-originated config change: writes back to the committed
    /// document first, then reloads the entry's fiber with the new config.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] for an unknown entry or a foreign config
    /// type; whatever the lane answers for an unstatable payload.
    pub async fn update_entry<C: LaneConfig>(
        &self,
        entry: &EntryId,
        config: C,
    ) -> Result<(), KernelError> {
        let _gate = self.gate.lock().await;
        let handle = {
            let mut state = lock(&self.state);
            let spec = amend_committed::<C>(&mut state, entry, |persisted| {
                persisted.config = config.clone();
            })?;
            let runtime = state
                .entries
                .get_mut(entry)
                .ok_or_else(|| error(ErrorCode::InvalidProfile, "the entry has no runtime"))?;
            runtime.spec = spec;
            runtime.live.as_ref().map(|live| Arc::clone(&live.handle))
        };
        if let Some(handle) = handle {
            handle.restate(&config)?;
            handle.restart(TransitionCause::ConfigChanged);
            handle.quiesce().await?;
        }
        Ok(())
    }

    /// A runtime-originated disposal: persists the entry as disabled (config
    /// retained), then disposes its fiber.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] for an unknown entry or a foreign config
    /// type.
    pub async fn dispose_entry<C: LaneConfig>(&self, entry: &EntryId) -> Result<(), KernelError> {
        let _gate = self.gate.lock().await;
        let live = {
            let mut state = lock(&self.state);
            let spec = amend_committed::<C>(&mut state, entry, |persisted| {
                persisted.disabled = true;
            })?;
            let runtime = state
                .entries
                .get_mut(entry)
                .ok_or_else(|| error(ErrorCode::InvalidProfile, "the entry has no runtime"))?;
            runtime.spec = spec;
            runtime.context = None;
            runtime.live.take()
        };
        if let Some(live) = live {
            live.handle.dispose().await?;
        }
        Ok(())
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

/// Amends one entry of the committed document in place and returns the new
/// spec `Arc` for the runtime.
fn amend_committed<C: LaneConfig>(
    state: &mut State,
    entry: &EntryId,
    change: impl FnOnce(&mut ProfileEntry<C>),
) -> Result<Arc<dyn Any + Send + Sync>, KernelError> {
    let committed = state
        .committed
        .as_ref()
        .and_then(|committed| committed.downcast_ref::<Profile<C>>())
        .ok_or_else(|| error(ErrorCode::InvalidProfile, "foreign config type"))?;
    let mut profile = committed.clone();
    let persisted = profile
        .entries
        .iter_mut()
        .find(|candidate| candidate.id == *entry)
        .ok_or_else(|| error(ErrorCode::InvalidProfile, "no such entry"))?;
    change(persisted);
    let spec = Arc::new(persisted.clone()) as Arc<dyn Any + Send + Sync>;
    state.committed = Some(Arc::new(profile) as Arc<dyn Any + Send + Sync>);
    Ok(spec)
}

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}

pub(crate) fn error(code: ErrorCode, message: &str) -> KernelError {
    KernelError {
        code,
        message: message.to_owned(),
        fiber: None,
    }
}
