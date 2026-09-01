//! The production kernel ASSEMBLY: everything `Daemon::open` wires up
//! once, before anything runs — one ledger, one wasm lane behind one
//! broker, every host provider registered at that single choke point, and
//! the kernel-owned operator contracts.
//!
//! Split from `daemon.rs` by responsibility (R10 file hygiene): building
//! the kernel and OPERATING it (boot, reload, deliver, shutdown) are two
//! jobs, and only one of them ever runs twice.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use jinnd_api::{EntryId, ErrorCode, KernelError};
use jinnd_context::ContextTree;
use jinnd_ledger::{Ledger, RevertLane};
use jinnd_loader::Loader;
use jinnd_registry::Registry;
use jinnd_wasm::{
    HostClock, HostFs, HostKeystore, HostNet, HostProcess, LaneCore, LedgerSink, MasterKeySource,
};

use super::{
    Daemon, DaemonPaths, Lifecycle, Readiness, introspect, ledger_cap, profile_cap, restarts,
    storage, waits,
};
use crate::support::{SharedFibers, Sink, error};

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
        let hostnet = HostNet::new(Arc::clone(&lane.sink));
        hostnet.register(&lane.broker)?;
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
            hostnet,
            keystore,
            fibers,
            applied_pins: Mutex::new(HashMap::new()),
            readiness,
            lifecycle,
            _root: root,
        })
    }
}
