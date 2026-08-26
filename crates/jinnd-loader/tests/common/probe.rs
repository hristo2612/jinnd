//! A probe lane for amendment-ordering tests: its handles can reject a
//! designated config at the restate boundary or refuse disposal, and every
//! config value the runtime was asked to observe is recorded in order (split
//! from `amend.rs` by responsibility, R10).

#![allow(dead_code)]

use std::any::Any;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use jinnd_api::{
    ErrorCode, FiberId, FiberState, KernelError, KernelFuture, PluginRef, Profile, ProfileEntry,
    TransitionCause,
};
use jinnd_loader::{DocumentEntry, EntryHandle, FileStore, Loader, PackageLane, SpawnRequest};

use super::{Grab, id};

pub const PACKAGE: &str = "probe/plugin";

/// Every config value the probe's fibers were asked to observe, in order.
pub type Stated = Arc<Mutex<Vec<u32>>>;

pub fn stated(log: &Stated) -> Vec<u32> {
    log.lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .clone()
}

/// How one probe lane misbehaves, per test.
#[derive(Clone, Copy, Default)]
pub struct Probe {
    /// A config value the runtime rejects at the restate boundary.
    pub reject: Option<u32>,
    /// Whether disposal fails instead of completing.
    pub refuse_disposal: bool,
}

struct ProbeHandle<C> {
    fiber: FiberId,
    log: Stated,
    value_of: fn(&C) -> u32,
    probe: Probe,
}

impl<C: Send + Sync + 'static> EntryHandle for ProbeHandle<C> {
    fn id(&self) -> FiberId {
        self.fiber
    }

    fn state(&self) -> FiberState {
        FiberState::Active
    }

    // The probe replays no plugin-owned inverses: nothing here can hold up
    // another operation's wait, so it is honestly never withdrawing.
    fn withdrawing(&self) -> bool {
        false
    }

    // The probe never transitions: honestly always at rest.
    fn resting(&self) -> bool {
        true
    }

    fn restart(&self, _cause: TransitionCause) {}

    fn restate(&self, config: &(dyn Any + Send + Sync)) -> Result<(), KernelError> {
        let value = (self.value_of)(config.downcast_ref::<C>().grab());
        if self.probe.reject == Some(value) {
            return Err(KernelError {
                code: ErrorCode::InvalidProfile,
                message: format!("the probe rejects config {value}"),
                fiber: Some(self.fiber),
            });
        }
        self.log
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(value);
        Ok(())
    }

    fn rebind(&self, _at: jinnd_context::Context<()>) {}

    fn dispose(&self) -> KernelFuture<'static, ()> {
        let outcome = if self.probe.refuse_disposal {
            Err(KernelError {
                code: ErrorCode::InvalidProfile,
                message: "the probe refuses disposal".to_owned(),
                fiber: Some(self.fiber),
            })
        } else {
            Ok(())
        };
        Box::pin(async move { outcome })
    }

    fn quiesce(&self) -> KernelFuture<'static, ()> {
        Box::pin(async { Ok(()) })
    }
}

fn lane<C: Clone + Send + Sync + 'static>(
    log: &Stated,
    value_of: fn(&C) -> u32,
    probe: Probe,
) -> PackageLane {
    static SERIAL: AtomicU64 = AtomicU64::new(1);
    let log = Arc::clone(log);
    PackageLane {
        injects: Vec::new(),
        provides: None,
        spawn: Box::new(move |request: SpawnRequest<'_>| {
            request.config.downcast_ref::<C>().grab();
            Ok(Arc::new(ProbeHandle {
                fiber: FiberId(SERIAL.fetch_add(1, Ordering::Relaxed)),
                log: Arc::clone(&log),
                value_of,
                probe,
            }) as Arc<dyn EntryHandle>)
        }),
    }
}

/// A loader with one probe package registered for config type `C`.
pub fn probe_loader<C: Clone + fmt::Debug + PartialEq + Send + Sync + 'static>(
    value_of: fn(&C) -> u32,
    probe: Probe,
) -> (Loader, Stated) {
    let tree = jinnd_context::ContextTree::new();
    let loader = Loader::new(tree.root(), jinnd_registry::Registry::new(), |_context| {});
    let log: Stated = Arc::new(Mutex::new(Vec::new()));
    loader
        .register_lane::<C>(PACKAGE, lane::<C>(&log, value_of, probe))
        .grab();
    (loader, log)
}

pub fn probe_entry<C>(name: &str, config: C) -> ProfileEntry<C> {
    ProfileEntry {
        id: id(name),
        plugin: PluginRef {
            package: PACKAGE.to_owned(),
            version: "1".to_owned(),
            artifact_hash: String::new(),
        },
        config,
        disabled: false,
        parent: None,
        isolation: Vec::new(),
    }
}

/// A store path whose directory does not exist, so every save fails.
pub fn broken_path() -> PathBuf {
    static SERIAL: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir()
        .join(format!(
            "jinnd-loader-amend-missing-{}-{}",
            std::process::id(),
            SERIAL.fetch_add(1, Ordering::Relaxed)
        ))
        .join("profile.json")
}

static SCRATCH: AtomicU64 = AtomicU64::new(0);

pub fn scratch_path() -> PathBuf {
    let unique = format!(
        "jinnd-loader-amend-{}-{}",
        std::process::id(),
        SCRATCH.fetch_add(1, Ordering::Relaxed)
    );
    let directory = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&directory).grab();
    directory.join("profile.json")
}

/// The named entry as the document on disk records it.
pub async fn disk_entry(path: &Path, name: &str) -> DocumentEntry {
    let document = FileStore::new(path.to_path_buf())
        .load()
        .await
        .grab()
        .grab();
    document
        .entries
        .iter()
        .find(|entry| entry.id == name)
        .cloned()
        .grab()
}

/// The named entry as the loader's committed document records it.
pub fn committed_entry(loader: &Loader, name: &str) -> ProfileEntry<u32> {
    let committed: Profile<u32> = loader.persisted().grab();
    committed
        .entries
        .iter()
        .find(|entry| entry.id == id(name))
        .cloned()
        .grab()
}
