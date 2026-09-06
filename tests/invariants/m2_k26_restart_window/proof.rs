//! K26's request identity and restart-window proof. Reuse the daemon's
//! opt-in guest markers and ledger observer unchanged (R2, R10): an earlier
//! delivered notice is not evidence that the selected refused request ran.

use super::{NOTICE, dispatch, dispatch_observation as observation, shutdown};
use jinnd_api::{DispatchMode, EntryId, FiberState, LedgerEventKind, LedgerRecord, Owed};
use jinnd_daemon::{Daemon, DaemonPaths};
use std::time::{Duration, Instant};

pub(super) fn refusal(paths: &DaemonPaths, outcome: &[u8], records: &[LedgerRecord]) -> String {
    assert_eq!(outcome.first(), Some(&1), "typed restarting: {outcome:?}");
    let reply = dispatch::json(&outcome[1..]);
    assert_eq!(reply["case"], "restarting");
    assert_eq!(reply["entry"], "consumer");
    assert_eq!(reply["topic"], NOTICE);
    assert!(reply["incarnation"].as_u64().is_some_and(|id| id > 0));

    let attempt = observation::attempt(&paths.data, "dispatch-refused");
    let walk = observation::walk(records, &attempt);
    let refused: Vec<_> = walk
        .iter()
        .filter(|record| matches!(record.kind, LedgerEventKind::DispatchRefused { .. }))
        .collect();
    assert_eq!(refused.len(), 1, "one refusal for {attempt}: {walk:?}");
    assert_eq!(refused[0].entry, Some(EntryId("provider".to_owned())));
    assert!(
        matches!(&refused[0].kind,
            LedgerEventKind::DispatchRefused { topic, mode: DispatchMode::Serial, target, incarnation, owed: Owed::Reload }
                if topic == NOTICE && target.0 == "consumer"
                    && Some(*incarnation) == reply["incarnation"].as_u64()
        ),
        "typed reply and causal row agree: {reply}, {walk:?}"
    );
    assert_no_delivery(paths, records, &attempt);

    // Each negative predicate must independently reject a REAL earlier
    // delivery. The consumer retains its notice append and callback; that
    // callback may refuse a wait cycle, but the handler actually ran.
    let control = observation::attempt(&paths.data, "dispatch-control");
    assert_ne!(control, attempt);
    let earlier = observation::walk(records, &control);
    assert!(
        earlier
            .last()
            .zip(walk.first())
            .is_some_and(|(a, b)| a.sequence < b.sequence)
    );
    assert_eq!(
        observation::no_delivery(records, &paths.data, &control),
        Err("request reached a listener")
    );
    assert_eq!(
        observation::no_trace(&earlier),
        Err("request dispatched a traced walk")
    );
    assert!(earlier.iter().any(|record| matches!(&record.kind,
        LedgerEventKind::DispatchTrace { topic, listeners, .. } if topic == NOTICE && *listeners > 0
    )));
    println!(
        "K26 request={attempt} incarnation={} control={control}: exact refusal and both real-delivery negative controls passed; refusal row={:?}",
        reply["incarnation"], refused[0]
    );
    attempt
}

fn assert_no_delivery(paths: &DaemonPaths, records: &[LedgerRecord], attempt: &str) {
    // Receipt absence is checked across ALL incarnations and the complete
    // ledger, not just within the bracket. Any trace, even an empty one,
    // contradicts refusal before dispatch (no partial delivery or empty success).
    assert_eq!(
        observation::no_delivery(records, &paths.data, attempt),
        Ok(())
    );
    assert_eq!(
        observation::no_trace(&observation::walk(records, attempt)),
        Ok(())
    );
}

pub(super) async fn finish(daemon: &Daemon, paths: &DaemonPaths, attempt: &str) {
    let fiber = daemon
        .entry_fiber("consumer")
        .unwrap_or_else(|| panic!("consumer has a fiber"));
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        daemon.sync_transitions();
        let log = std::fs::read_to_string(paths.data.join("consumer.log")).unwrap_or_default();
        if daemon.fiber_state(fiber) == Some(FiberState::Active)
            && log.lines().filter(|line| *line == "act").count() >= 2
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the replacement commits Active: {log}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let document =
        dispatch::json(&std::fs::read(&paths.profile).unwrap_or_else(|error| panic!("{error}")));
    let patched = document["entries"]
        .as_array()
        .and_then(|entries| entries.iter().find(|entry| entry["id"] == "consumer"))
        .map(|entry| entry["config"]["data"].clone());
    assert_eq!(patched, Some(serde_json::json!("notify-consumer:v2")));
    shutdown(daemon).await;
    let records = dispatch::events(daemon).await;
    assert_no_delivery(paths, &records, attempt);
    assert!(
        !records.iter().any(|record| matches!(&record.kind,
            LedgerEventKind::FiberTransition(transition) if transition.to == FiberState::Failed
        )),
        "no failed fiber through replacement and shutdown: {records:?}"
    );

    let walk = observation::walk(&records, attempt);
    let refused = walk
        .iter()
        .find(|record| matches!(record.kind, LedgerEventKind::DispatchRefused { .. }))
        .unwrap_or_else(|| panic!("the refused walk stays ledgered"));
    let transition_to = |record: &&LedgerRecord, state| {
        record.fiber == Some(fiber)
            && matches!(&record.kind,
                LedgerEventKind::FiberTransition(transition)
                    if transition.to == state && transition.cause == jinnd_api::TransitionCause::ConfigChanged
            )
    };
    let patch = records
        .iter()
        .rev()
        .find(|record| {
            record.sequence < refused.sequence && matches!(&record.kind,
                LedgerEventKind::ProfilePatched { entry, by } if entry.0 == "consumer" && by.0 == "provider"
            )
        })
        .unwrap_or_else(|| panic!("the refusal follows the accepted config patch"));
    // Restarting is also truthful while Unloading is owed but not yet
    // committed. Do not require the refusal to follow that later transition.
    let start = records
        .iter()
        .find(|record| {
            record.sequence > patch.sequence && transition_to(record, FiberState::Unloading)
        })
        .unwrap_or_else(|| panic!("the patched consumer unloads"));
    let end = records
        .iter()
        .find(|record| {
            record.sequence > start.sequence && transition_to(record, FiberState::Active)
        })
        .unwrap_or_else(|| panic!("the refused replacement commits"));
    assert!(
        refused.sequence < end.sequence,
        "refusal precedes replacement commit"
    );
    // K26's no-empty-topic guarantee covers the restart window, including
    // attempts other than the selected refusal. Initial boot/control history
    // is outside this window; it may honestly have no listener yet.
    assert!(!records.iter().any(|record| patch.sequence < record.sequence && record.sequence < end.sequence
        && matches!(&record.kind, LedgerEventKind::DispatchTrace { topic, listeners: 0, .. } if topic == NOTICE)
    ), "no empty successful walk inside the restart: {records:?}");
    println!(
        "K26 restart window sequence={}..{}, refusal={}; no empty success, late delivery or failed fiber through shutdown",
        start.sequence, end.sequence, refused.sequence
    );
}
