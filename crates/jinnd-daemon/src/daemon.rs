//! The production kernel assembly (M1-P9): everything the `jinnd` shell and
//! the headless acceptance demo drive. One loader over one context tree and
//! registry, one real SQLite ledger, one wasm lane behind the one broker —
//! and no harness lane anywhere in the build (Law 1).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use jinnd_api::{EntryId, ErrorCode, KernelError, LedgerEventKind, LedgerQuery, ReconcileReport};
use jinnd_context::ContextTree;
use jinnd_ledger::{Ledger, RevertLane};
use jinnd_loader::{Document, FileStore, Loader};
use jinnd_registry::Registry;
use jinnd_wasm::{HostClock, HostFs, LaneCore, LedgerSink};

use crate::support::{SharedFibers, Sink, error, lock};

mod observe;

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

impl DaemonPaths {
    /// The `jinn:fs` inverse spill (M2-K3): beside the root, never inside.
    #[must_use]
    pub fn inverses(&self) -> PathBuf {
        self.data.with_extension("inverses")
    }
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
        // Inverses spill OUTSIDE the guests' containment root (M2-K3).
        let hostfs = Arc::new(HostFs::open(paths.data.clone(), paths.inverses(), sink)?);
        hostfs.register(&lane.broker)?;
        // The jinn:clock read provider (M2-K2): time enters through the
        // same choke point; alarm machinery lives in the lane's registry.
        HostClock::register(&lane.broker)?;
        // Every entry's retained journal crosses the process boundary
        // through the retention store (M2-K4 ruling 3): the lane inherits
        // it here, so a successor incarnation's dispose withdraws the
        // whole trail and a removal-while-down withdraws at boot.
        for (entry, records) in hostfs.journals() {
            lane.inherit(&EntryId(entry), records);
        }
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
        let report = self.apply(document).await?;
        self.withdraw_orphaned_journals().await;
        Ok(report)
    }

    /// An entry that left the profile while the daemon was down left the
    /// composition (M2-K4; I4): its retained journal withdraws at boot,
    /// LIFO, every withdrawal ledgered under the entry — a fresh boot of
    /// the final configuration shows no trace of it. Entries still named
    /// (faulted or not) keep theirs.
    async fn withdraw_orphaned_journals(&self) {
        let named: Vec<String> = self.entries();
        for entry in self.lane.journaled_entries() {
            if named.contains(&entry.0) {
                continue;
            }
            if let Err(error) = self.lane.withdraw_journal(&entry, None).await {
                self.ledger
                    .record(LedgerEventKind::ErrorRecorded { error }, Some(entry), None);
            }
        }
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

    /// Graceful shutdown (M1-P9 card; M2-K4 ruling 2: shutdown = SUSPEND):
    /// suspend every fiber — kernel registrations release, world mutations
    /// stay on disk for the entries that persist in the profile, each
    /// suspension a typed ledger event — reach quiescence, then flush the
    /// ledger: the barrier is a read through the single writer, so every
    /// event sent before it is durably committed when this returns `Ok`.
    /// Crash and clean shutdown agree on the disk outcome; only the clean
    /// path reaches quiescence and flushes.
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
            fiber.suspend().await;
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
