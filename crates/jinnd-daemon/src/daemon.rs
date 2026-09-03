//! The production kernel (M1-P9): everything the `jinnd` shell and the
//! headless acceptance demo drive. One loader over one context tree and
//! registry, one real SQLite ledger, one wasm lane behind the one broker —
//! and no harness lane anywhere in the build (Law 1).
//!
//! This file is the OPERATING surface — boot, reload, deliver, shutdown.
//! Wiring the kernel up is `assemble` (R10 file hygiene): building it and
//! running it are two jobs, and only one of them ever runs twice.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use jinnd_api::{ErrorCode, KernelError, LedgerEventKind, LedgerQuery, ReconcileReport};
use jinnd_ledger::{Ledger, RevertLane};
use jinnd_loader::{Document, FileStore, Loader};
use jinnd_wasm::{HostFs, HostKeystore, HostNet, LaneCore};

pub use crate::paths::DaemonPaths;
use crate::support::{SharedFibers, error, lock};
pub(crate) use lifecycle::Lifecycle;
pub use observe::UnitMember;

mod assemble;
mod auth_cap;
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
    pub(crate) ledger: Ledger,
    revert: RevertLane,
    pub(crate) loader: Arc<Loader>,
    pub(crate) lane: Arc<LaneCore>,
    hostfs: Arc<HostFs>,
    /// The `jinn:net` provider, kept so the daemon can name the
    /// IRREVERSIBLE outbound calls a revert unit must be refused for
    /// (M2-K14; Law 3).
    hostnet: Arc<HostNet>,
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
        // Before any entry may call: an outbound effect id is irreversible
        // forever, so it must name ONE call forever (M2-K14).
        self.seed_net_effects().await?;
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
