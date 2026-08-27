//! The M1 acceptance demo, headless (M1-P9): the same five steps the
//! operator drives from docs/demo/M1-DEMO.md, against the daemon-assembled
//! production kernel — boot, reconcile-by-id, Mode-1 hot-swap with rollback,
//! ledger-visible dispose, keyed revert with receipts (SOURCE-OF-TRUTH §7 M1).

#[path = "support.rs"]
mod support;

use std::sync::Arc;

use jinnd_api::{FiberState, LedgerEventKind, RevertResolution, SwapPhaseKind};
use jinnd_daemon::{Daemon, DaemonPaths, Watch};

fn active(daemon: &Daemon, entry: &str) -> jinnd_api::FiberId {
    let fiber = daemon
        .entry_fiber(entry)
        .unwrap_or_else(|| panic!("{entry} has no fiber"));
    assert_eq!(
        daemon.fiber_state(fiber),
        Some(FiberState::Active),
        "{entry} is not active"
    );
    fiber
}

async fn ledger_kinds(daemon: &Daemon) -> Vec<LedgerEventKind> {
    daemon
        .ledger_events()
        .await
        .unwrap_or_else(|error| panic!("ledger readable: {error:?}"))
        .into_iter()
        .map(|record| record.kind)
        .collect()
}

fn swap_phases(kinds: &[LedgerEventKind], artifact: &str) -> Vec<SwapPhaseKind> {
    kinds
        .iter()
        .filter_map(|kind| match kind {
            LedgerEventKind::SwapPhase {
                artifact: hash,
                phase,
            } if hash == artifact => Some(*phase),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_m1_acceptance_demo_runs_headlessly() {
    let (root, _cleanup) = support::fresh_root();
    support::build_kit(&root);
    let profile = root.join("profile.json");
    let artifacts = root.join("artifacts");
    let data = root.join("data");
    let paths = DaemonPaths {
        profile: profile.clone(),
        ledger: root.join("ledger.sqlite"),
        artifacts: artifacts.clone(),
        data: data.clone(),
    };
    let daemon = Arc::new(
        Daemon::open(paths.clone())
            .unwrap_or_else(|error| panic!("the daemon assembles: {error:?}")),
    );

    // Step 1 — boot: three wasm plugins from the profile, all Active.
    let report = daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot reconciles: {error:?}"));
    assert!(report.errors.is_empty(), "boot faults: {:?}", report.errors);
    assert_eq!(report.created.len(), 3, "three entries: {report:?}");
    let clock = active(&daemon, "clock");
    let greeter = active(&daemon, "greeter");
    let scribe = active(&daemon, "scribe");

    // The real watcher lane (round-2 blocker 1): steps 2 and 3a arrive as
    // file events through the daemon's own watch task, exactly as the
    // operator drives them in the runbook — no reload()/swap() bypass.
    let watch =
        Watch::start(&paths).unwrap_or_else(|error| panic!("the watcher starts: {error:?}"));
    let serving = tokio::spawn(watch.serve(Arc::clone(&daemon)));

    // Step 2 — edit ONE entry's config ON DISK: the watcher reconciles;
    // exactly that fiber restarts in place and the sibling uids are
    // unchanged (reconcile-by-id). The scribe's journal carrying the new
    // greeting is the proof the new config was applied.
    support::edit_profile_data(&profile, "greeter", "kernel");
    support::eventually("the watcher-driven reconcile to land the greeting", || {
        std::fs::read_to_string(data.join("journal.txt"))
            .is_ok_and(|journal| journal.contains("hello, kernel (tick"))
    })
    .await;
    assert_eq!(daemon.entry_fiber("clock"), Some(clock), "clock untouched");
    assert_eq!(
        daemon.entry_fiber("scribe"),
        Some(scribe),
        "scribe untouched"
    );
    assert_eq!(
        daemon.entry_fiber("greeter"),
        Some(greeter),
        "a config edit restarts the fiber in place; the uid survives"
    );

    // Step 3a — Mode-1 hot-swap, healthy, THROUGH THE WATCHER: replacing
    // the artifact file (with its pin sidecar) is the whole operator
    // gesture. The seat swaps warm; the fiber uid does not change; the
    // ledger shows Began → InstanceHealthy → Committed for the new
    // artifact.
    support::clock_variant("v2", &artifacts);
    let v2_hash = std::fs::read_to_string(artifacts.join("clock.wasm.sha256"))
        .unwrap_or_else(|error| panic!("v2 sidecar pin: {error:?}"));
    let committed = [
        SwapPhaseKind::Began,
        SwapPhaseKind::InstanceHealthy,
        SwapPhaseKind::Committed,
    ];
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let kinds = ledger_kinds(&daemon).await;
        let phases = swap_phases(&kinds, v2_hash.trim());
        if phases == committed {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the watcher-driven swap is ledger-recorded phase by phase: {phases:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert_eq!(daemon.entry_fiber("clock"), Some(clock), "no fiber restart");
    assert_eq!(daemon.fiber_state(clock), Some(FiberState::Active));

    // The watcher lane is proven; stop it so the remaining steps drive the
    // same daemon surface directly (the fast unit lane) without two
    // appliers racing over one profile file.
    serving.abort();

    // Step 3b — a deliberately-broken artifact: the health gate fails, the
    // batch rolls back, the old instance keeps serving, ledger-recorded.
    support::clock_variant("broken", &artifacts);
    let broken_hash = std::fs::read_to_string(artifacts.join("clock.wasm.sha256"))
        .unwrap_or_else(|error| panic!("broken sidecar pin: {error:?}"));
    let outcome = daemon
        .swap("demo/clock")
        .await
        .unwrap_or_else(|error| panic!("swap runs to rollback: {error:?}"));
    assert!(outcome.rolled_back, "the broken artifact rolls back");
    assert!(outcome.swapped.is_empty());
    let kinds = ledger_kinds(&daemon).await;
    let phases = swap_phases(&kinds, broken_hash.trim());
    assert_eq!(
        phases.first(),
        Some(&SwapPhaseKind::Began),
        "the rollback is ledger-recorded from Began"
    );
    assert_eq!(phases.last(), Some(&SwapPhaseKind::RolledBack));
    // The old instance still serves: another config edit greets again
    // through the surviving clock.
    support::edit_profile_data(&profile, "greeter", "rollback");
    let report = daemon
        .reload()
        .await
        .unwrap_or_else(|error| panic!("post-rollback reload: {error:?}"));
    assert!(report.errors.is_empty());
    let journal = std::fs::read_to_string(data.join("journal.txt"))
        .unwrap_or_else(|error| panic!("journal: {error:?}"));
    assert!(journal.contains("hello, rollback (tick"));

    // Step 4 — dispose one plugin: the ledger shows exactly what was undone.
    support::remove_profile_entry(&profile, "scribe");
    let report = daemon
        .reload()
        .await
        .unwrap_or_else(|error| panic!("dispose reconciles: {error:?}"));
    assert_eq!(report.disposed, vec![jinnd_api::EntryId("scribe".into())]);
    assert_eq!(daemon.entry_fiber("scribe"), None, "scribe is gone");
    let kinds = ledger_kinds(&daemon).await;
    assert!(
        kinds.iter().any(|kind| matches!(
            kind,
            LedgerEventKind::EffectWithdrawn { label, clean: true } if label == "scribe on duty"
        )),
        "the scribe's guest effect withdrawal is ledger-recorded"
    );
    assert!(
        kinds.iter().any(|kind| matches!(
            kind,
            LedgerEventKind::EffectWithdrawn { label, clean: true } if label == "listen demo:announce"
        )),
        "the scribe's listener withdrawal is ledger-recorded (Law 2: \
         every registration's undo shows in the dispose trail)"
    );

    // Step 5 — keyed revert with receipts: revert the last journal write;
    // the file returns to its prior content and the ledger shows the
    // intent → completed → resolved receipt trail.
    let (effect, path) = daemon
        .fs_effects()
        .pop()
        .unwrap_or_else(|| panic!("the journal writes registered revertible effects"));
    assert!(path.ends_with("journal.txt"));
    let before = std::fs::read_to_string(data.join("journal.txt"))
        .unwrap_or_else(|error| panic!("journal: {error:?}"));
    let resolution = daemon
        .revert(effect, "demo-revert")
        .await
        .unwrap_or_else(|error| panic!("the revert protocol runs: {error:?}"));
    assert_eq!(resolution, RevertResolution::Reverted);
    let after = std::fs::read_to_string(data.join("journal.txt"))
        .unwrap_or_else(|error| panic!("journal: {error:?}"));
    assert_ne!(before, after, "the last write is undone");
    assert!(
        !after.contains("hello, rollback"),
        "the reverted write's line is gone: {after:?}"
    );
    let kinds = ledger_kinds(&daemon).await;
    let trail: Vec<&LedgerEventKind> = kinds
        .iter()
        .filter(|kind| {
            matches!(
                kind,
                LedgerEventKind::RevertIntent { .. }
                    | LedgerEventKind::RevertCompleted { .. }
                    | LedgerEventKind::RevertResolved { .. }
            )
        })
        .collect();
    assert_eq!(trail.len(), 3, "intent, completed, resolved: {trail:?}");

    // Shutdown: dispose all, quiescence, ledger flushed — and the flush
    // barrier's outcome is reported, never assumed (round-2 major).
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("the flush barrier holds: {error:?}"));
}
