//! The daemon's watched-file lane (M1-P9 card): a profile edit triggers
//! reconcile-by-id and an artifact (or pin-sidecar) replacement triggers a
//! Mode-1 hot-swap — debounced, served by a supervised task (R1). Both the
//! watched directories and every delivered path are canonicalized before
//! comparison: macOS FSEvents reports real paths while operators name
//! profiles through symlinked roots like `/tmp` and `/var`, and the
//! uncanonicalized comparison is what left round 1's watcher provably dead
//! (round-2 blocker 1).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use jinnd_api::{ErrorCode, KernelError, ReconcileReport};
use notify::Watcher as _;
use tokio::sync::mpsc;

use crate::daemon::{Daemon, DaemonPaths};
use crate::support::error;

/// How long a save burst settles before the daemon acts (card: debounced —
/// a malformed or atomic-rename save often arrives as several events).
const DEBOUNCE: Duration = Duration::from_millis(300);

/// One delivered path, classified against the daemon's watched files.
#[derive(Debug, PartialEq, Eq)]
enum Change {
    /// The profile document changed: reconcile-by-id.
    Profile,
    /// One package artifact (or its pin sidecar) changed: swap this stem.
    Artifact(String),
}

/// The daemon's file watcher: classify-then-debounce over the profile's
/// directory and the artifacts directory (the directories are watched, not
/// the files, so atomic-rename saves are seen).
pub struct Watch {
    events: mpsc::UnboundedReceiver<PathBuf>,
    profile: PathBuf,
    artifacts: PathBuf,
    _watcher: notify::RecommendedWatcher,
}

/// The canonical form when the path resolves; the path itself when it does
/// not (a just-deleted file still classifies by parent and name).
fn canonical(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_owned())
}

impl Watch {
    /// Starts watching (canonically) beside the profile and the artifacts.
    ///
    /// # Errors
    ///
    /// A watcher-backend refusal for either directory: the card's edit and
    /// swap lanes would be silently dead, and the daemon must not pretend
    /// to serve them (honest failure).
    pub fn start(paths: &DaemonPaths) -> Result<Self, KernelError> {
        let refuse = |refused: notify::Error| error(ErrorCode::EffectFailed, refused.to_string());
        let (tx, events) = mpsc::unbounded_channel();
        let mut watcher =
            notify::recommended_watcher(move |outcome: Result<notify::Event, notify::Error>| {
                if let Ok(event) = outcome {
                    for path in event.paths {
                        let _ = tx.send(path);
                    }
                }
            })
            .map_err(refuse)?;
        let profile_dir = canonical(paths.profile.parent().unwrap_or(Path::new(".")));
        let artifacts = canonical(&paths.artifacts);
        let shallow = notify::RecursiveMode::NonRecursive;
        watcher.watch(&profile_dir, shallow).map_err(refuse)?;
        if artifacts != profile_dir {
            watcher.watch(&artifacts, shallow).map_err(refuse)?;
        }
        Ok(Self {
            profile: profile_dir.join(paths.profile.file_name().unwrap_or_default()),
            events,
            artifacts,
            _watcher: watcher,
        })
    }

    /// Classifies one delivered path; unrelated paths are `None`.
    fn classify(&self, delivered: &Path) -> Option<Change> {
        let path = canonical(delivered);
        if path == self.profile {
            return Some(Change::Profile);
        }
        if path.parent() != Some(self.artifacts.as_path()) {
            return None;
        }
        // `clock.wasm` and `clock.wasm.sha256` both mean "swap clock": the
        // pair is written together, and the swap reads both at apply time.
        let name = path.file_name()?.to_str()?;
        let stem = name.strip_suffix(".sha256").unwrap_or(name);
        Some(Change::Artifact(stem.strip_suffix(".wasm")?.to_owned()))
    }

    /// Serves the watched lane onto `daemon` until the backend closes the
    /// channel: relevant events gather while a burst settles, then one
    /// batch applies — at most one reconcile, plus each named swap once.
    pub async fn serve(mut self, daemon: Arc<Daemon>) {
        let mut pending: Vec<Change> = Vec::new();
        loop {
            tokio::select! {
                delivered = self.events.recv() => match delivered {
                    Some(path) => {
                        if let Some(change) = self.classify(&path) {
                            pending.push(change);
                        }
                    }
                    None => return,
                },
                () = tokio::time::sleep(DEBOUNCE), if !pending.is_empty() => {
                    apply(&daemon, std::mem::take(&mut pending)).await;
                }
            }
        }
    }
}

/// Applies one settled batch of watched changes.
async fn apply(daemon: &Daemon, changes: Vec<Change>) {
    if changes.contains(&Change::Profile) {
        match daemon.reload().await {
            Ok(report) => log_report(&report, daemon),
            Err(refused) => tracing::error!(?refused, "reconcile refused"),
        }
    }
    let mut swaps: Vec<String> = changes
        .into_iter()
        .filter_map(|change| match change {
            Change::Artifact(stem) => Some(stem),
            Change::Profile => None,
        })
        .collect();
    swaps.sort();
    swaps.dedup();
    for stem in swaps {
        let packages = daemon.packages_for_artifact(&stem);
        if packages.is_empty() {
            tracing::debug!(stem, "no registered package uses this artifact");
        }
        for package in packages {
            match daemon.swap(&package).await {
                Ok(outcome) if outcome.rolled_back => {
                    tracing::warn!(package, "hot-swap rolled back; old instances serving");
                }
                Ok(outcome) if outcome.swapped.is_empty() => {
                    tracing::debug!(package, "artifact unchanged; nothing to swap");
                }
                Ok(outcome) => {
                    tracing::info!(package, swapped = ?outcome.swapped, "hot-swap committed");
                }
                Err(refused) => tracing::error!(package, ?refused, "hot-swap refused"),
            }
        }
    }
}

/// Renders one reconcile report into the operator log, then the entries.
pub fn log_report(report: &ReconcileReport, daemon: &Daemon) {
    tracing::info!(
        created = ?report.created,
        restarted = ?report.restarted,
        disposed = ?report.disposed,
        unchanged = ?report.unchanged,
        faults = ?report.errors,
        "reconciled"
    );
    log_status(daemon);
}

/// Renders each entry's fiber and state into the operator log.
pub fn log_status(daemon: &Daemon) {
    for entry in daemon.entries() {
        match daemon.entry_fiber(&entry) {
            Some(fiber) => {
                let state = daemon.fiber_state(fiber);
                tracing::info!(entry, fiber = fiber.0, state = ?state, "entry");
            }
            None => tracing::info!(entry, "entry has no live fiber"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Change, Watch, canonical};
    use crate::DaemonPaths;

    /// Round-2 blocker 1 pinned: the backend delivers REAL paths while the
    /// operator named the profile through a symlinked root (`/tmp`,
    /// `/var`); classification must see through the alias — including for
    /// a sidecar path that does not exist yet.
    #[cfg(unix)]
    #[test]
    fn classify_sees_through_a_symlinked_root() {
        let fatal = |refused: std::io::Error| panic!("test tree builds: {refused:?}");
        let base = std::env::temp_dir().join(format!("jinnd-watch-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let real = base.join("real");
        std::fs::create_dir_all(real.join("artifacts")).unwrap_or_else(fatal);
        std::fs::write(real.join("profile.json"), b"{}").unwrap_or_else(fatal);
        std::fs::write(real.join("artifacts/clock.wasm"), b"\0asm").unwrap_or_else(fatal);
        let link = base.join("link");
        std::os::unix::fs::symlink(&real, &link).unwrap_or_else(fatal);
        let watch = Watch::start(&DaemonPaths {
            profile: link.join("profile.json"),
            ledger: link.join("ledger.sqlite"),
            artifacts: link.join("artifacts"),
            data: link.join("data"),
        })
        .unwrap_or_else(|refused| panic!("the watcher starts through the alias: {refused:?}"));
        let delivered = canonical(&real);
        assert_eq!(
            watch.classify(&delivered.join("profile.json")),
            Some(Change::Profile)
        );
        assert_eq!(
            watch.classify(&delivered.join("artifacts/clock.wasm")),
            Some(Change::Artifact("clock".into()))
        );
        assert_eq!(
            watch.classify(&delivered.join("artifacts/clock.wasm.sha256")),
            Some(Change::Artifact("clock".into())),
            "a not-yet-existing sidecar still classifies by parent and name"
        );
        assert_eq!(watch.classify(&delivered.join("artifacts/notes.txt")), None);
        assert_eq!(watch.classify(&delivered.join("ledger.sqlite")), None);
        drop(watch);
        let _ = std::fs::remove_dir_all(&base);
    }
}
