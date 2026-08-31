//! The production kernel assembly (M1-P9): everything the `jinnd` shell and
//! the headless acceptance demo drive. One loader over one context tree and
//! registry, one real SQLite ledger, one wasm lane behind the one broker —
//! and no harness lane anywhere in the build (Law 1).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use jinnd_api::{EntryId, ErrorCode, KernelError, LedgerEventKind, LedgerQuery, ReconcileReport};
use jinnd_context::ContextTree;
use jinnd_ledger::{Ledger, RevertLane};
use jinnd_loader::{Document, FileStore, Loader};
use jinnd_registry::Registry;
use jinnd_wasm::{
    HostClock, HostFs, HostKeystore, HostNet, HostProcess, LaneCore, LedgerSink, MasterKeySource,
};

pub use crate::paths::DaemonPaths;
use crate::support::{SharedFibers, Sink, error, lock};
pub(crate) use lifecycle::Lifecycle;

mod introspect;
mod ledger_cap;
mod lifecycle;
mod observe;
mod profile_cap;
mod profile_read;
mod restarts;
mod waits;
mod wire;

/// What `jinn:introspect.readiness` answers (M2-K7, harness #19/#12).
#[derive(Default)]
pub(crate) struct Readiness {
    pub(crate) boot_reconciled: AtomicBool,
    pub(crate) watcher_armed: AtomicBool,
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
    pub(crate) keystore: Arc<HostKeystore>,
    pub(crate) fibers: SharedFibers,
    /// Per package, the pin last applied FROM THE PROFILE (see
    /// `packages.rs`).
    pub(crate) applied_pins: Mutex<HashMap<String, String>>,
    pub(crate) readiness: Arc<Readiness>,
    /// The kernel's lifecycle publish (M2-K13): every transition the
    /// ledger sync commits is offered here and pushed to the listeners a
    /// `jinn:introspect` grant admits.
    pub(crate) lifecycle: Arc<Lifecycle>,
    /// Keeps the context tree (and every context derived under it) alive.
    _root: jinnd_context::Context<()>,
}

impl Daemon {
    /// Assembles the kernel over `paths`. Blocking (SQLite open belongs in
    /// construction, never on an async path); call before serving.
    ///
    /// # Errors
    ///
    /// Ledger storage refusals, wasm engine construction failures, and a
    /// configured `jinn:keystore` master-key source that cannot be read
    /// (fail-closed: the daemon refuses to start rather than fall through
    /// to a different key).
    pub fn open(paths: DaemonPaths) -> Result<Self, KernelError> {
        Self::open_with(paths, MasterKeySource::from_env()?)
    }

    /// [`Daemon::open`] with an explicit `jinn:keystore` master-key source
    /// (M2-K8 round-2 ruling 1): the key is never under the data root, so
    /// the daemon is told where it comes from.
    ///
    /// # Errors
    ///
    /// As [`Daemon::open`]; also an existing sealed store the source
    /// cannot open (fail-closed).
    pub fn open_with(paths: DaemonPaths, master: MasterKeySource) -> Result<Self, KernelError> {
        std::fs::create_dir_all(&paths.data)
            .map_err(|refused| error(ErrorCode::InvalidProfile, refused.to_string()))?;
        let ledger = Ledger::open(&paths.ledger).map_err(storage)?;
        let fibers: SharedFibers = Arc::new(Mutex::new(HashMap::new()));
        // ONE sink: every broker crossing and provider event lands on the
        // same ordered ledger record lane (R6), entry-attributed (M2-K7).
        let sink: Arc<dyn LedgerSink> = Arc::new(Sink {
            ledger: ledger.clone(),
            fibers: Arc::clone(&fibers),
        });
        let lane = Arc::new(LaneCore::new(Arc::clone(&sink))?);
        // The kernel's lifecycle publish (M2-K13, harness #40/#41): the
        // transitions the ledger sync commits are pushed from here to the
        // listeners a `jinn:introspect` grant admits.
        let lifecycle = Lifecycle::new(ledger.clone(), Arc::clone(&lane));
        // Inverses spill OUTSIDE the guests' containment root (M2-K3).
        let hostfs = Arc::new(HostFs::open(paths.data.clone(), paths.inverses(), sink)?);
        hostfs.register(&lane.broker)?;
        // The jinn:clock read provider (M2-K2): time enters through the
        // same choke point; alarm machinery lives in the lane's registry.
        HostClock::register(&lane.broker)?;
        // The jinn:process and jinn:net providers (M2-K6, finding 5): the
        // same choke point; their registrations release on suspend.
        HostProcess::new(Arc::clone(&lane.sink)).register(&lane.broker)?;
        HostNet::new(Arc::clone(&lane.sink)).register(&lane.broker)?;
        // The jinn:keystore provider (M2-K8, finding 5's remainder): the
        // sealed store sits beside the data root, out of every guest's
        // reach; its retained journal spans processes like the fs one.
        let keystore = Arc::new(HostKeystore::open(
            paths.keystore(),
            master,
            Arc::clone(&lane.sink),
        )?);
        keystore.register(&lane.broker)?;
        // Every entry's retained journal crosses the process boundary
        // through the retention store (M2-K4 ruling 3): the lane inherits
        // it here, so a successor incarnation's dispose withdraws the
        // whole trail and a removal-while-down withdraws at boot.
        for (entry, records) in hostfs.journals() {
            lane.inherit(&EntryId(entry), records);
        }
        for (entry, records) in keystore.journals() {
            lane.inherit(&EntryId(entry), records);
        }
        // The pending-restart oracle (M2-K9, harness #31): ONE snapshot
        // source, installed on the topic registry and handed to
        // `jinn:introspect`.
        let restarts = restarts::Restarts::install(&lane, &fibers);
        // The wait graph (M2-K10, harness #32): ONE graph across the
        // broker and the topic registry, so a cycle that closes through a
        // contract call and a dispatch is seen as the one cycle it is.
        let waits = waits::Names::install(&lane, &fibers);
        let tree = ContextTree::new();
        let root = tree.root();
        let registry = Registry::new();
        let loader = Arc::new(Loader::new(root.clone(), registry, |_context| {}));
        let readiness = Arc::new(Readiness::default());
        // The operator contracts (M2-K7, harness #19/#20/#21): kernel-owned
        // facts behind the same choke point as every other provider —
        // granted, ledgered per read, never a side door (Law 1).
        introspect::HostIntrospect::register(
            &lane.broker,
            Arc::clone(&loader),
            Arc::clone(&lane),
            Arc::clone(&readiness),
            restarts,
            waits,
        )?;
        ledger_cap::HostLedger::register(&lane.broker, ledger.clone())?;
        profile_cap::HostProfile::register(
            &lane.broker,
            Arc::clone(&loader),
            ledger.clone(),
            Arc::clone(&fibers),
            Arc::clone(&lifecycle),
        )?;
        Ok(Self {
            paths,
            revert: RevertLane::new(ledger.clone()),
            ledger,
            loader,
            lane,
            hostfs,
            keystore,
            fibers,
            applied_pins: Mutex::new(HashMap::new()),
            readiness,
            lifecycle,
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
        self.readiness.boot_reconciled.store(true, Ordering::SeqCst);
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

    /// Re-reads the profile file and reconciles by id — the runbook's
    /// explicit edit path: always reconciles, the loader's diff answering
    /// `unchanged` when nothing differs.
    ///
    /// # Errors
    ///
    /// As [`Daemon::boot`].
    pub async fn reload(&self) -> Result<ReconcileReport, KernelError> {
        let document = Document::parse(&self.delivered().await?)?;
        self.apply(document).await
    }

    /// The file watcher's delivery (M2-K5 #17): the daemon's own write-back
    /// echo is recognized by the exact bytes the loader WROTE — remembered
    /// at the save, never re-read from a file another writer may have
    /// replaced meanwhile — and answers `None`; anything else reconciles,
    /// an operator's identical rewrite included (Law 2: the log records
    /// what happened, an edit is never lost under a success line).
    /// Recognition is ONE-SHOT (round 2): the remembered bytes name exactly
    /// one expected delivery and are consumed by it, so a later delivery of
    /// the same bytes is the operator's and reconciles `unchanged`.
    ///
    /// # Errors
    ///
    /// As [`Daemon::boot`].
    pub async fn deliver(&self) -> Result<Option<ReconcileReport>, KernelError> {
        let text = self.delivered().await?;
        if self.loader.retire_echo(&text) {
            return Ok(None);
        }
        let document = Document::parse(&text)?;
        self.apply(document).await.map(Some)
    }

    async fn delivered(&self) -> Result<String, KernelError> {
        tokio::fs::read_to_string(&self.paths.profile)
            .await
            .map_err(|refused| error(ErrorCode::InvalidProfile, refused.to_string()))
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
