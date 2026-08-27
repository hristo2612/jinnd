//! The `jinnd` shell (M1-P9; R10 — the daemon has no features): parse the
//! two required paths, assemble the kernel, boot, then serve three inputs
//! until SIGINT — profile-file edits (reconcile-by-id), artifact-file
//! replacements (Mode-1 hot-swap under the `.sha256` pin sidecar), and
//! operator `revert` lines on stdin (keyed exactly-once revert).

use std::path::PathBuf;
use std::time::Duration;

use jinnd_daemon::{Daemon, DaemonPaths};
use tokio::io::AsyncBufReadExt;

fn usage() -> ! {
    eprintln!(
        "usage: jinnd --profile <profile.json> --ledger <ledger.sqlite> \
         [--artifacts <dir>] [--data <dir>]\n\
         stdin: revert <effect-id> <key> | status"
    );
    std::process::exit(2);
}

fn parse_paths() -> DaemonPaths {
    let mut profile = None;
    let mut ledger = None;
    let mut artifacts = None;
    let mut data = None;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().map(PathBuf::from);
        match (flag.as_str(), value) {
            ("--profile", Some(path)) => profile = Some(path),
            ("--ledger", Some(path)) => ledger = Some(path),
            ("--artifacts", Some(path)) => artifacts = Some(path),
            ("--data", Some(path)) => data = Some(path),
            _ => usage(),
        }
    }
    let Some(profile) = profile else { usage() };
    let Some(ledger) = ledger else { usage() };
    let beside = |name: &str| {
        profile
            .parent()
            .map(|parent| parent.join(name))
            .unwrap_or_else(|| PathBuf::from(name))
    };
    DaemonPaths {
        artifacts: artifacts.unwrap_or_else(|| beside("artifacts")),
        data: data.unwrap_or_else(|| beside("data")),
        profile,
        ledger,
    }
}

fn log_report(report: &jinnd_api::ReconcileReport, daemon: &Daemon) {
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

fn log_status(daemon: &Daemon) {
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

/// Watched filesystem changes, debounced into daemon operations.
async fn handle_paths(daemon: &Daemon, paths: &DaemonPaths, changed: Vec<PathBuf>) {
    let mut reload = false;
    let mut swaps: Vec<String> = Vec::new();
    for path in changed {
        if path == paths.profile {
            reload = true;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "wasm")
            && path.parent() == Some(paths.artifacts.as_path())
            && let Some(stem) = path.file_stem().and_then(|stem| stem.to_str())
        {
            swaps.push(stem.to_owned());
        }
    }
    if reload {
        match daemon.reload().await {
            Ok(report) => log_report(&report, daemon),
            Err(refused) => tracing::error!(?refused, "reconcile refused"),
        }
    }
    swaps.sort();
    swaps.dedup();
    for stem in swaps {
        match daemon.swap(&stem).await {
            Ok(outcome) if outcome.rolled_back => {
                tracing::warn!(
                    package = stem,
                    "hot-swap rolled back; old instances serving"
                );
            }
            Ok(outcome) if outcome.swapped.is_empty() => {
                tracing::debug!(package = stem, "artifact unchanged; nothing to swap");
            }
            Ok(outcome) => {
                tracing::info!(package = stem, swapped = ?outcome.swapped, "hot-swap committed");
            }
            Err(refused) => tracing::error!(package = stem, ?refused, "hot-swap refused"),
        }
    }
}

async fn handle_line(daemon: &Daemon, line: &str) {
    let mut words = line.split_whitespace();
    match (words.next(), words.next(), words.next()) {
        (Some("revert"), Some(effect), Some(key)) => match effect.parse::<u64>() {
            Ok(id) => match daemon.revert(jinnd_api::EffectId(id), key).await {
                Ok(resolution) => tracing::info!(effect = id, key, ?resolution, "revert"),
                Err(refused) => tracing::error!(effect = id, key, ?refused, "revert refused"),
            },
            Err(_) => tracing::error!(effect, "revert wants a numeric effect id"),
        },
        (Some("status"), None, None) => log_status(daemon),
        _ => tracing::warn!(line, "unknown command (revert <effect-id> <key> | status)"),
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let paths = parse_paths();
    let daemon = match Daemon::open(paths.clone()) {
        Ok(daemon) => daemon,
        Err(refused) => {
            tracing::error!(?refused, "the kernel did not assemble");
            std::process::exit(1);
        }
    };
    match daemon.boot().await {
        Ok(report) => log_report(&report, &daemon),
        Err(refused) => {
            tracing::error!(?refused, "boot refused");
            std::process::exit(1);
        }
    }

    // File watching (debounced): the profile's directory and the artifacts
    // directory. The watcher thread only forwards; every operation runs here.
    let (events_tx, mut events) = tokio::sync::mpsc::unbounded_channel::<PathBuf>();
    let watcher = {
        use notify::Watcher;
        let mut watcher = match notify::recommended_watcher(
            move |outcome: Result<notify::Event, notify::Error>| {
                if let Ok(event) = outcome {
                    for path in event.paths {
                        let _ = events_tx.send(path);
                    }
                }
            },
        ) {
            Ok(watcher) => watcher,
            Err(refused) => {
                tracing::error!(?refused, "file watcher unavailable");
                std::process::exit(1);
            }
        };
        for directory in [
            paths.profile.parent().unwrap_or(std::path::Path::new(".")),
            paths.artifacts.as_path(),
        ] {
            if let Err(refused) = watcher.watch(directory, notify::RecursiveMode::NonRecursive) {
                tracing::warn!(?refused, directory = %directory.display(), "not watching");
            }
        }
        watcher
    };

    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut stdin_open = true;
    let mut pending: Vec<PathBuf> = Vec::new();
    loop {
        let debounce = async {
            // A malformed save often arrives as several events; let the
            // burst settle before reconciling (card: debounced).
            tokio::time::sleep(Duration::from_millis(300)).await;
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            changed = events.recv() => {
                if let Some(path) = changed {
                    pending.push(path);
                }
            }
            line = lines.next_line(), if stdin_open => {
                match line {
                    Ok(Some(line)) if !line.trim().is_empty() => {
                        handle_line(&daemon, line.trim()).await;
                    }
                    Ok(Some(_)) => {}
                    // Stdin closed; keep serving files and signals.
                    Ok(None) | Err(_) => stdin_open = false,
                }
            }
            _ = debounce, if !pending.is_empty() => {
                let changed = std::mem::take(&mut pending);
                handle_paths(&daemon, &paths, changed).await;
            }
        }
    }

    tracing::info!("SIGINT: disposing all, then quiescence, then ledger flush");
    drop(watcher);
    daemon.shutdown().await;
    tracing::info!("quiescent; ledger flushed; bye");
    std::process::exit(0);
}
