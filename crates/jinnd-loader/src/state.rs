//! The loader's shared state: entry runtimes, the committed document, and the
//! settle loop that lets every consequence land (split from `loader.rs` by
//! responsibility, R10).

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use jinnd_api::{EntryId, ErrorCode, FiberId, FiberState, KernelError, Profile, ProfileEntry};
use jinnd_context::Context;

use crate::lanes::{EntryHandle, PackageLane};
use crate::loader::{LaneConfig, Loader};
use crate::proxy::ReadinessProxy;

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

/// Amends one entry of the committed document in place and returns the new
/// spec `Arc` for the runtime.
pub(crate) fn amend_committed<C: LaneConfig>(
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

impl Loader {
    /// Settles every loader-owned fiber: quiesce passes, with yields so
    /// readiness watchers publish, until two consecutive passes observe the
    /// same states. Termination is I3's promise for acyclic dependencies.
    pub(crate) async fn settle(&self) {
        let mut previous: Option<Vec<(FiberId, FiberState)>> = None;
        loop {
            let handles: Vec<_> = lock(&self.state)
                .entries
                .values()
                .filter_map(|runtime| runtime.live.as_ref())
                .map(|live| Arc::clone(&live.handle))
                .collect();
            for handle in &handles {
                let _ = handle.quiesce().await;
            }
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
            let mut snapshot: Vec<(FiberId, FiberState)> = handles
                .iter()
                .map(|handle| (handle.id(), handle.state()))
                .collect();
            snapshot.sort_by_key(|(id, _)| *id);
            if previous.as_ref() == Some(&snapshot) {
                return;
            }
            previous = Some(snapshot);
        }
    }
}
