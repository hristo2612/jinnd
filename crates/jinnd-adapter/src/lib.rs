//! Implementer-owned wiring between the stable facade and kernel subsystem crates.
//!
//! **This is the conformance-harness lane** (SOURCE-OF-TRUTH decision log,
//! 2026-08-25): the in-proc, statically-typed [`jinnd_api::Kernel`] exists so
//! the verifier-owned invariant suite can drive kernel semantics. It is never a
//! plugin host and never ships in the daemon binary (Law 1). Wired as of M1-P6:
//! context, effects, fiber, registry, events, and the profile loader; M1-P7
//! adds the ledger, revert, and forward-effect lanes.
//!
//! Layout (R10 hygiene): this file holds the kernel value and its state;
//! `facade.rs` the trait impl; `run.rs` and `support.rs` the method bodies;
//! `boundary.rs` the forward-effect and revert seams; `body.rs`, `wiring.rs`,
//! and `providing.rs` the fiber-body and package-lane machinery.
//!
//! Harness conventions, stated once:
//!
//! * [`KERNEL_SCOPE`] (`FiberId(0)`) is the pseudo-fiber that owns facade-level
//!   provisions and effects; real fiber uids start at 1 and are never reused.
//! * An unknown fiber id reads as `Disposed` — a fiber this kernel never spawned
//!   is not live, and uids are never reused (R3) — with an empty history.
//! * A context id this kernel did not mint is refused with `InactiveContext`
//!   wherever the facade can express an error.

#![forbid(unsafe_code)]

mod body;
mod boundary;
mod facade;
mod providing;
mod run;
mod support;
mod wiring;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use jinnd_api::{ContextId, EffectId, ErrorCode, FiberId, Kernel, KernelError};
use jinnd_context::{Context, ContextTree};
use jinnd_effects::EffectScope;
use jinnd_events::{EventBus, Registration};
use jinnd_fiber::Fiber;
use jinnd_ledger::{Ledger, RevertLane};
use jinnd_loader::Loader;
use jinnd_registry::{Registry, Vitality};

/// The pseudo-fiber facade-level provisions and effects are charged to.
pub const KERNEL_SCOPE: FiberId = FiberId(0);

/// One spawned fiber and the body whose config the facade may re-state.
pub(crate) struct FiberEntry {
    pub(crate) fiber: Arc<Fiber>,
    pub(crate) body: Arc<dyn std::any::Any + Send + Sync>,
}

/// The fiber map, shared with loader-lane spawners (uids are never reused, R3).
pub(crate) type SharedFibers = Arc<Mutex<HashMap<FiberId, Arc<FiberEntry>>>>;

pub(crate) struct Adapter {
    root: Context<()>,
    contexts: Arc<Mutex<HashMap<ContextId, Context<()>>>>,
    fibers: SharedFibers,
    registry: Registry,
    loader: Arc<Loader>,
    events: EventBus,
    /// Removal handles for live listener effects, so `unlisten` can withdraw
    /// one registration by its effect id; removal stays idempotent with the
    /// same undo held by the kernel scope.
    listeners: Mutex<HashMap<EffectId, Registration>>,
    kernel_scope: Arc<Mutex<EffectScope>>,
    /// The kernel pseudo-fiber's vitality: always Active, never reported away.
    kernel_vitality: Vitality,
    /// The device-local append-only event stream (R6; in-memory for the
    /// conformance-harness lane — same semantics, no device durability).
    ledger: Ledger,
    /// The keyed exactly-once revert lane over the ledger (constitution 03).
    revert: RevertLane,
    /// Forward effects begun and not yet disposed (M1-P7).
    pending: boundary::PendingMap,
    /// How many of each fiber's transitions the ledger has already seen.
    recorded_transitions: Mutex<HashMap<FiberId, usize>>,
    /// Where the raw document of record persists, once attached.
    document_path: Mutex<Option<std::path::PathBuf>>,
}

/// Returns the facade kernel used by verifier-owned invariant tests.
///
/// Implementation packets replace subsystem stubs here without changing the tests.
pub fn kernel() -> impl Kernel {
    let tree: ContextTree = ContextTree::new();
    let root = tree.root();
    let contexts = Arc::new(Mutex::new(HashMap::from([(root.id(), root.clone())])));
    let registry = Registry::new();
    let kernel_vitality = registry.vitality(true);
    // Every context the loader mints joins the facade's map, so entry context
    // ids stay first-class facade citizens.
    let minted = Arc::clone(&contexts);
    let loader = Arc::new(Loader::new(
        root.clone(),
        registry.clone(),
        move |context| {
            lock(&minted).insert(context.id(), context);
        },
    ));
    // The harness ledger opens in memory; a refusal here is a broken harness,
    // not a recoverable kernel state, so the panic is the honest answer.
    let ledger = Ledger::open_in_memory()
        .unwrap_or_else(|error| panic!("the harness ledger must open: {error}"));
    Adapter {
        root,
        contexts,
        fibers: Arc::new(Mutex::new(HashMap::new())),
        registry,
        loader,
        events: EventBus::new(),
        listeners: Mutex::new(HashMap::new()),
        kernel_scope: Arc::new(Mutex::new(EffectScope::new())),
        kernel_vitality,
        revert: RevertLane::new(ledger.clone()),
        ledger,
        pending: Mutex::new(HashMap::new()),
        recorded_transitions: Mutex::new(HashMap::new()),
        document_path: Mutex::new(None),
    }
}

/// Lock helper recovering from poisoning (R11): the maps and the kernel scope
/// hold valid data whatever thread panicked while touching them. No guard taken
/// here is ever held across an `await` or a call into plugin code (R1).
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
