//! Guest progress is the witness; asynchronous ledger INSERT times are not.
//! Keep the activation-time case, and use a post-activation call here so the
//! runtime's fatal cause is observable through its existing error/death path.
use super::*;

pub(super) async fn prove() {
    let test_home = home("m2-k25-clock-witness");
    let mut initial: Vec<_> = (0..43)
        .map(|index| clocked_bus_entry(&format!("slow-{index}"), "listener-slow"))
        .collect();
    let (daemon_paths, hash) = paths(&test_home, &initial);
    let daemon = booted(daemon_paths).await;
    initial.push(entry(
        "emitter",
        serde_json::json!([TOPIC, CLOCK, "jinn:fs"]),
        serde_json::json!([]),
        "emitter-deadline-witness",
    ));
    reload(&daemon, &test_home, &initial, &hash).await;
    until_state(&daemon, "emitter", FiberState::Failed).await;
    let records = events(&daemon).await;
    assert_eq!(trace(&records, "emitter"), Some((43, 0)));
    let labels: Vec<_> = records
        .iter()
        .filter_map(|record| {
            if record
                .entry
                .as_ref()
                .is_none_or(|entry| entry.0 != "emitter")
            {
                return None;
            }
            match &record.kind {
                LedgerEventKind::EffectRegistered { label } => Some(label.as_str()),
                _ => None,
            }
        })
        .collect();
    let walks: Vec<u64> = labels
        .iter()
        .filter_map(|label| {
            label
                .strip_prefix("fs write deadline-walk-ms-")
                .and_then(|suffix| suffix.split_once(" [effect "))
                .and_then(|(ms, _)| ms.parse().ok())
        })
        .collect();
    assert_eq!(walks.len(), 1, "one completed walk: {labels:?}");
    assert!(
        walks[0] > 5_000,
        "the actual guest walk exceeds five seconds"
    );
    assert!(
        labels
            .iter()
            .any(|label| label.starts_with("fs write deadline-survived-4500ms [effect ")),
        "the guest executed 4.5 seconds of its remaining active budget: {labels:?}"
    );
    assert!(
        !labels
            .iter()
            .any(|label| label.starts_with("fs write deadline-exceeded-5500ms [effect ")),
        "the walk did not grant the guest another active budget: {labels:?}"
    );
    let fatal: Vec<_> = records
        .iter()
        .filter_map(|record| {
            if record
                .entry
                .as_ref()
                .is_none_or(|entry| entry.0 != "emitter")
            {
                return None;
            }
            match &record.kind {
                LedgerEventKind::ErrorRecorded { error } => Some(error),
                _ => None,
            }
        })
        .collect();
    assert!(!fatal.is_empty(), "the emitter's fatal cause is recorded");
    assert!(
        fatal
            .iter()
            .all(|error| error.code == jinnd_api::ErrorCode::PluginFailed
                && error.message == "guest exceeded its call deadline"),
        "{fatal:?}"
    );
    assert!(
        transitions(&records, "emitter")
            .iter()
            .any(|transition| transition.to == FiberState::Failed
                && format!("{:?}", transition.cause) == "BodyFaulted")
    );
    for index in 0..43 {
        let id = format!("slow-{index}");
        assert_eq!(state(&daemon, &id), Some(FiberState::Active));
        assert!(errors(&records, &id).is_empty());
    }
    println!(
        "guest progress: walk={}ms, 43 successful listeners, survived 4500ms, no 5500ms overrun; fatal={fatal:?}",
        walks[0]
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}
