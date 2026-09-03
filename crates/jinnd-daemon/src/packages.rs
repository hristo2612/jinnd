//! Package admission (split by responsibility, R10): every wasm package the
//! profile names gets a lane over its pinned artifact file. Admission is
//! pin-by-hash (Law 5): the profile states the pin, the kernel verifies it,
//! and either outcome is a ledger event.

use std::sync::{Arc, Mutex};

use jinnd_api::{EntryFault, ErrorCode, GROUP_PACKAGE, KernelError, Profile, Transition};
use jinnd_fiber::{Fiber, TransitionObserver};
use jinnd_ledger::Ledger;
use jinnd_loader::PackageLane;
use jinnd_wasm::{LaneCore, LoadedComponent, WasmBody, wasm_lane_declaring};

use crate::daemon::{Daemon, Lifecycle};
use crate::seat::{seat_config, seat_declaration};
use crate::support::{SharedFibers, error, lock, tracked};

/// The daemon's lane over one wasm package (the lifted generic lane, M2-K1):
/// seats and their `injects` declarations (M2-K24) decode from the
/// profile's JSON config, the guest's registrations land the daemon's
/// Law-2 ledger trail, and every spawned fiber is tracked for the
/// transition-ledger bridge (R6).
fn lane(
    core: Arc<LaneCore>,
    fibers: SharedFibers,
    component: Arc<Mutex<LoadedComponent>>,
    ledger: Ledger,
    lifecycle: Arc<Lifecycle>,
) -> PackageLane {
    let track = move |body: Arc<WasmBody>, signal| {
        let entry = body.entry().clone();
        let observer = Arc::new(LedgerTransitions {
            entry: entry.clone(),
            ledger: ledger.clone(),
            lifecycle: Arc::clone(&lifecycle),
        });
        tracked(&fibers, entry, None, || {
            Arc::new(Fiber::spawn_observed(body, signal, observer))
        })
    };
    wasm_lane_declaring::<serde_json::Value, _, _>(
        core,
        component,
        true,
        seat_config,
        seat_declaration,
        track,
    )
}

struct LedgerTransitions {
    entry: jinnd_api::EntryId,
    ledger: Ledger,
    lifecycle: Arc<Lifecycle>,
}

impl TransitionObserver for LedgerTransitions {
    fn committed(&self, transition: &Transition) {
        self.ledger.record(
            jinnd_api::LedgerEventKind::FiberTransition(transition.clone()),
            Some(self.entry.clone()),
            Some(transition.fiber),
        );
        self.lifecycle.offer(&self.entry, transition);
    }
}

/// The last path segment of a package name keys its artifact file.
pub(crate) fn basename(package: &str) -> &str {
    package.rsplit('/').next().unwrap_or(package)
}

impl Daemon {
    /// Every registered package whose artifact basename is `stem` — the
    /// watched-file lane's join from a changed `<stem>.wasm` file back to
    /// the package name(s) it serves (round-2: a stem is not a package).
    #[must_use]
    pub fn packages_for_artifact(&self, stem: &str) -> Vec<String> {
        let mut packages: Vec<String> = lock(&self.lane.packages)
            .keys()
            .filter(|package| basename(package) == stem)
            .cloned()
            .collect();
        packages.sort();
        packages
    }

    /// Registers (or re-pins) the wasm lane of every package the profile
    /// names: the artifact file is admitted under the entry's pinned hash
    /// (Law 5 — a mismatch refuses the entry, recorded, siblings untouched).
    pub(crate) fn ensure_lanes(&self, profile: &Profile<serde_json::Value>) -> Vec<EntryFault> {
        let mut faults = Vec::new();
        for entry in &profile.entries {
            let package = &entry.plugin.package;
            if package == GROUP_PACKAGE || entry.disabled {
                continue;
            }
            let pin = &entry.plugin.artifact_hash;
            if pin.is_empty() {
                faults.push(EntryFault {
                    entry: entry.id.clone(),
                    error: error(
                        ErrorCode::InvalidProfile,
                        format!("entry {:?} names no artifact pin (Law 5)", entry.id.0),
                    ),
                });
                continue;
            }
            let current = lock(&self.lane.packages).get(package).cloned();
            let applied = lock(&self.applied_pins).get(package).cloned();
            let outcome = match current {
                // A live Mode-1 swap moves the cell ahead of the profile pin
                // deliberately (runtime-led, R8); only a profile-led pin
                // change re-admits from disk.
                Some(_) if applied.as_deref() == Some(pin.as_str()) => Ok(()),
                Some(cell) => self.admit(package, pin).map(|component| {
                    *lock(&cell) = component;
                    lock(&self.applied_pins).insert(package.clone(), pin.clone());
                }),
                None => self.admit(package, pin).and_then(|component| {
                    let cell = Arc::new(Mutex::new(component));
                    let lane = lane(
                        Arc::clone(&self.lane),
                        Arc::clone(&self.fibers),
                        Arc::clone(&cell),
                        self.ledger.clone(),
                        Arc::clone(&self.lifecycle),
                    );
                    self.loader
                        .register_lane::<serde_json::Value>(package, lane)
                        .map(|()| {
                            lock(&self.lane.packages).insert(package.clone(), cell);
                            lock(&self.applied_pins).insert(package.clone(), pin.clone());
                        })
                }),
            };
            if let Err(refused) = outcome {
                faults.push(EntryFault {
                    entry: entry.id.clone(),
                    error: refused,
                });
            }
        }
        faults
    }

    /// Reads and admits one package's artifact under `pin` (ledgered either
    /// way: `ArtifactLoaded` or `ArtifactRefused`).
    pub(crate) fn admit(&self, package: &str, pin: &str) -> Result<LoadedComponent, KernelError> {
        let file = self
            .paths
            .artifacts
            .join(format!("{}.wasm", basename(package)));
        let bytes = std::fs::read(&file).map_err(|refused| {
            error(
                ErrorCode::InvalidProfile,
                format!("artifact {} unreadable: {refused}", file.display()),
            )
        })?;
        self.lane.host.load(bytes, pin, self.lane.sink.as_ref())
    }
}
