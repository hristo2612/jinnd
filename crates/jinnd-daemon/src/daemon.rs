//! The production kernel assembly (M1-P9): everything the `jinnd` shell and
//! the headless acceptance demo drive. One loader over one context tree and
//! registry, one real SQLite ledger, one wasm lane behind the one broker —
//! and no harness lane anywhere in the build (Law 1).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use jinnd_api::{
    EffectId, EntryId, ErrorCode, FiberId, FiberState, KernelError, LedgerEventKind, LedgerQuery,
    LedgerRecord, ReconcileReport, RevertKey, RevertResolution,
};
use jinnd_context::ContextTree;
use jinnd_ledger::{Ledger, RevertLane};
use jinnd_loader::{Document, FileStore, Loader};
use jinnd_registry::Registry;
use jinnd_wasm::{HostFs, LaneCore, LedgerSink};

use crate::support::{SharedFibers, Sink, error, lock};

/// The daemon's whole configuration (M1-P9 card: ledger path and profile
/// path are the only required config; the rest defaults beside the profile).
#[derive(Clone, Debug)]
pub struct DaemonPaths {
    /// The profile document of record (LAW §3).
    pub profile: PathBuf,
    /// The append-only ledger's SQLite file (R6).
    pub ledger: PathBuf,
    /// Where `<package-basename>.wasm` artifacts (and `.sha256` pin
    /// sidecars) live.
    pub artifacts: PathBuf,
    /// The `jinn:fs` provider's containment root.
    pub data: PathBuf,
}

fn storage(refused: jinnd_ledger::LedgerError) -> KernelError {
    error(ErrorCode::EffectFailed, refused.to_string())
}

/// The daemon-assembled kernel.
pub struct Daemon {
    pub(crate) paths: DaemonPaths,
    ledger: Ledger,
    revert: RevertLane,
    pub(crate) loader: Arc<Loader>,
    pub(crate) lane: Arc<LaneCore>,
    hostfs: Arc<HostFs>,
    pub(crate) fibers: SharedFibers,
    /// The last committed document text: the daemon's own write-back must
    /// not re-trigger a reconcile through the file watcher.
    committed_text: Mutex<Option<String>>,
    /// Per package, the pin last applied FROM THE PROFILE (see
    /// `packages.rs`).
    pub(crate) applied_pins: Mutex<HashMap<String, String>>,
    /// Keeps the context tree (and every context derived under it) alive.
    _root: jinnd_context::Context<()>,
}

impl Daemon {
    /// Assembles the kernel over `paths`. Blocking (SQLite open belongs in
    /// construction, never on an async path); call before serving.
    ///
    /// # Errors
    ///
    /// Ledger storage refusals and wasm engine construction failures.
    pub fn open(paths: DaemonPaths) -> Result<Self, KernelError> {
        std::fs::create_dir_all(&paths.data)
            .map_err(|refused| error(ErrorCode::InvalidProfile, refused.to_string()))?;
        let ledger = Ledger::open(&paths.ledger).map_err(storage)?;
        // ONE sink: every broker crossing and provider event lands on the
        // same ordered ledger record lane (R6).
        let sink: Arc<dyn LedgerSink> = Arc::new(Sink(ledger.clone()));
        let lane = Arc::new(LaneCore::new(Arc::clone(&sink))?);
        let hostfs = Arc::new(HostFs::new(paths.data.clone(), sink));
        hostfs.register(&lane.broker)?;
        let tree = ContextTree::new();
        let root = tree.root();
        let registry = Registry::new();
        let loader = Arc::new(Loader::new(root.clone(), registry, |_context| {}));
        Ok(Self {
            paths,
            revert: RevertLane::new(ledger.clone()),
            ledger,
            loader,
            lane,
            hostfs,
            fibers: Arc::new(Mutex::new(HashMap::new())),
            committed_text: Mutex::new(None),
            applied_pins: Mutex::new(HashMap::new()),
            _root: root,
        })
    }

    /// Boots from the profile document of record: parse → lanes → reconcile
    /// → quiescence, with write-back attached so the running system and the
    /// file stay two views of one truth (LAW §3).
    ///
    /// # Errors
    ///
    /// Whole-document failures only; per-entry problems are contained
    /// faults in the report (R11).
    pub async fn boot(&self) -> Result<ReconcileReport, KernelError> {
        let store = FileStore::new(self.paths.profile.clone());
        let document = store.load().await?.unwrap_or_default();
        self.loader
            .attach_store::<serde_json::Value>(self.paths.profile.clone(), document.clone());
        self.apply(document).await
    }

    /// Re-reads the profile file and reconciles by id — the file watcher's
    /// (and the runbook's) edit path. The daemon's own write-back text is
    /// recognized and skipped.
    ///
    /// # Errors
    ///
    /// As [`Daemon::boot`].
    pub async fn reload(&self) -> Result<ReconcileReport, KernelError> {
        let text = tokio::fs::read_to_string(&self.paths.profile)
            .await
            .map_err(|refused| error(ErrorCode::InvalidProfile, refused.to_string()))?;
        if lock(&self.committed_text).as_deref() == Some(text.as_str()) {
            return Ok(ReconcileReport::default());
        }
        let document = Document::parse(&text)?;
        self.apply(document).await
    }

    async fn apply(&self, document: Document) -> Result<ReconcileReport, KernelError> {
        let (profile, mut faults) = document.resolve();
        faults.extend(self.ensure_lanes(&profile));
        let mut report = self.loader.reconcile(profile).await?;
        report.errors.extend(faults);
        for fault in &report.errors {
            self.ledger.record(
                LedgerEventKind::ErrorRecorded {
                    error: fault.error.clone(),
                },
                Some(fault.entry.clone()),
                None,
            );
        }
        // Remember the committed text as written back, so the watcher's echo
        // of our own save is a no-op.
        if let Ok(written) = std::fs::read_to_string(&self.paths.profile) {
            *lock(&self.committed_text) = Some(written);
        }
        self.sync_transitions();
        Ok(report)
    }

    /// Keyed exactly-once revert of one recorded fs write effect (Law 3):
    /// the inverse restores the prior content or absence; the witness reads
    /// the file back. Receipts land in the ledger either way.
    ///
    /// # Errors
    ///
    /// An effect this daemon's provider does not own, a distinct key for an
    /// already-claimed effect, or a storage refusal.
    pub async fn revert(
        &self,
        effect: EffectId,
        key: &str,
    ) -> Result<RevertResolution, KernelError> {
        // The provider that captured the write's inverse builds the action
        // (Law 3, M2-K1 seam); this daemon feeds it to the ledger's keyed
        // exactly-once protocol.
        let (witness, inverse) = self.hostfs.undo_action(effect).ok_or_else(|| {
            error(
                ErrorCode::EffectFailed,
                format!("no revertible effect {}", effect.0),
            )
        })?;
        self.revert
            .revert(
                effect,
                RevertKey(key.to_owned()),
                witness,
                inverse,
                None,
                None,
            )
            .await
    }

    /// Every recorded fs write effect, in registration order.
    #[must_use]
    pub fn fs_effects(&self) -> Vec<(EffectId, String)> {
        self.hostfs.effects()
    }

    /// The fiber currently hosting `entry`, if any.
    #[must_use]
    pub fn entry_fiber(&self, entry: &str) -> Option<FiberId> {
        self.loader.entry_fiber(&EntryId(entry.to_owned()))
    }

    /// The last committed state of one loader-owned fiber.
    #[must_use]
    pub fn fiber_state(&self, fiber: FiberId) -> Option<FiberState> {
        self.loader.fiber_state(fiber)
    }

    /// The committed entry ids, for operator status output.
    #[must_use]
    pub fn entries(&self) -> Vec<String> {
        self.loader
            .persisted::<serde_json::Value>()
            .map(|profile| {
                profile
                    .entries
                    .iter()
                    .map(|entry| entry.id.0.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The whole ledger stream, in sequence order.
    ///
    /// # Errors
    ///
    /// Storage refusals.
    pub async fn ledger_events(&self) -> Result<Vec<LedgerRecord>, KernelError> {
        self.ledger
            .events(LedgerQuery::default())
            .await
            .map_err(storage)
    }

    /// Emits every committed fiber transition the ledger has not yet seen
    /// (R6: transitions are ledger events; ordered, unreceipted lane).
    pub fn sync_transitions(&self) {
        let mut fibers = lock(&self.fibers);
        for tracked in fibers.values_mut() {
            let transitions = tracked.fiber.record().transitions;
            for transition in transitions.iter().skip(tracked.recorded) {
                self.ledger.record(
                    LedgerEventKind::FiberTransition(transition.clone()),
                    None,
                    Some(tracked.fiber.id()),
                );
            }
            tracked.recorded = transitions.len();
        }
    }

    /// Graceful shutdown (M1-P9 card): dispose every fiber (each seat's
    /// withdrawal is ledgered), reach quiescence, then flush the ledger —
    /// the barrier is a read through the single writer, so every event sent
    /// before it is durably committed when this returns `Ok`.
    ///
    /// # Errors
    ///
    /// A storage refusal at the flush barrier: recorded events may not be
    /// durable, and the caller must say so — never "ledger flushed"
    /// (honest failure; round-2 major).
    pub async fn shutdown(&self) -> Result<(), KernelError> {
        let handles: Vec<Arc<jinnd_fiber::Fiber>> = lock(&self.fibers)
            .values()
            .map(|tracked| Arc::clone(&tracked.fiber))
            .collect();
        for fiber in &handles {
            fiber.dispose().await;
        }
        self.loader.quiesce().await;
        self.sync_transitions();
        self.ledger
            .events(LedgerQuery {
                from_sequence: Some(u64::MAX),
                ..LedgerQuery::default()
            })
            .await
            .map(|_| ())
            .map_err(storage)
    }
}
