//! The `jinnd` shell (M1-P9; R10 — the daemon has no features): parse the
//! two required paths, arm the file watcher, assemble the kernel, boot,
//! announce readiness (M2-K5 #18/#12), then serve three inputs until SIGINT — profile-file edits (reconcile-by-id), artifact-file
//! replacements (Mode-1 hot-swap under the `.sha256` pin sidecar), and
//! operator `revert` lines on stdin (keyed exactly-once revert).

use std::path::PathBuf;
use std::sync::Arc;

use jinnd_daemon::{Daemon, DaemonPaths, Watch, log_report, log_status};
use tokio::io::AsyncBufReadExt;

fn usage() -> ! {
    eprintln!(
        "usage: jinnd --profile <profile.json> --ledger <ledger.sqlite> \
         [--artifacts <dir>] [--data <dir>]\n\
         env: JINND_KEYSTORE_PASSPHRASE | JINND_KEYSTORE_PASSPHRASE_FILE \
         (keystore master key; macOS default: the keychain)\n\
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
    // The interrupt handler is installed BEFORE boot (M2-K4): a SIGINT that
    // lands while the kernel assembles must reach the suspend path, never
    // the default action — clean shutdown is a promise from the first
    // instant, not from the first poll of the serve loop.
    let mut interrupt =
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()) {
            Ok(signal) => signal,
            Err(refused) => {
                tracing::error!(?refused, "the interrupt handler did not install");
                std::process::exit(1);
            }
        };
    // Watch BEFORE evidence (M2-K5 #18): paths canonicalize against the
    // working directory and the file watcher arms — or refuses, with the
    // error — before the kernel assembles and the boot reconcile writes
    // anything; a refused start leaves no trace an operator could mistake
    // for a running daemon.
    let cwd = match std::env::current_dir() {
        Ok(cwd) => cwd,
        Err(refused) => {
            tracing::error!(?refused, "the working directory is unreadable");
            std::process::exit(1);
        }
    };
    let paths = match paths.canonical(&cwd) {
        Ok(paths) => paths,
        Err(refused) => {
            tracing::error!(?refused, "the profile path does not resolve");
            std::process::exit(1);
        }
    };
    let watch = match Watch::start(&paths) {
        Ok(watch) => watch,
        Err(refused) => {
            tracing::error!(?refused, "file watcher unavailable");
            std::process::exit(1);
        }
    };
    let daemon = match Daemon::open(paths.clone()) {
        Ok(daemon) => Arc::new(daemon),
        Err(refused) => {
            tracing::error!(?refused, "the kernel did not assemble");
            std::process::exit(1);
        }
    };
    // The watcher armed above; `jinn:introspect.readiness` may say so.
    daemon.mark_watcher_armed();
    match daemon.boot().await {
        Ok(report) => log_report(&report, &daemon),
        Err(refused) => {
            tracing::error!(?refused, "boot refused");
            std::process::exit(1);
        }
    }
    // The readiness line (FINDINGS #12 minimum): one machine-readable line
    // on stderr, only once the watcher is armed AND the boot reconcile is
    // done — a launcher keys on this, never on boot evidence.
    eprintln!(
        r#"{{"jinnd":"ready","watcher":"armed","profile":{}}}"#,
        serde_json::Value::String(paths.profile.display().to_string())
    );
    // The watched-file lane runs as its own supervised task (R1); this
    // loop keeps stdin and the shutdown signal.
    let serving = tokio::spawn(watch.serve(Arc::clone(&daemon)));

    let stdin = tokio::io::BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    let mut stdin_open = true;
    loop {
        tokio::select! {
            _ = interrupt.recv() => break,
            line = lines.next_line(), if stdin_open => match line {
                Ok(Some(line)) if !line.trim().is_empty() => {
                    handle_line(&daemon, line.trim()).await;
                }
                Ok(Some(_)) => {}
                // Stdin closed; keep serving files and signals.
                Ok(None) | Err(_) => stdin_open = false,
            }
        }
    }

    tracing::info!("SIGINT: suspending all, then quiescence, then ledger flush");
    serving.abort();
    match daemon.shutdown().await {
        Ok(()) => {
            tracing::info!("quiescent; ledger flushed; bye");
            std::process::exit(0);
        }
        Err(refused) => {
            // Honest failure (round-2 major): a failed flush barrier means
            // recorded events may not be durable — say so and exit nonzero.
            tracing::error!(?refused, "flush barrier failed; ledger may not be durable");
            std::process::exit(1);
        }
    }
}
