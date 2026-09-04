//! Verifier-owned M2-K26 cases. The daemon cases drive the checked-in Tier A
//! guest; the registry cases isolate the commit instant that a process-level
//! schedule cannot pause inside without changing the production mechanism.

#![allow(dead_code)]

#[path = "../../crates/jinnd-daemon/tests/dispatch/harness.rs"]
mod dispatch_harness;
#[path = "string_lane_injects/fixture.rs"]
mod fixture;
#[path = "string_lane_injects/harness.rs"]
mod harness;
#[path = "string_lane_injects/ledger.rs"]
mod ledger;

use std::num::NonZeroU64;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dispatch_harness as dispatch;
use jinnd_api::{
    DispatchMode, EntryId, FiberId, FiberState, KernelFuture, LedgerEventKind, LedgerRecord, Owed,
};
use jinnd_daemon::{Daemon, DaemonPaths};
use jinnd_wasm::{
    EventTarget, LedgerSink, LocalTopics, NoRealms, Rebind, RestartOracle, Selector, Unserved,
};

const NOTICE: &str = "jinn:test/settings-changed";
const TOPIC: &str = "jinn:test/topic";

#[derive(Default)]
struct Recording(Mutex<Vec<(LedgerEventKind, Option<FiberId>)>>);

impl LedgerSink for Recording {
    fn append(&self, kind: LedgerEventKind, fiber: Option<FiberId>) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((kind, fiber));
    }
}

impl Recording {
    fn events(&self) -> Vec<(LedgerEventKind, Option<FiberId>)> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }
}

struct Answer {
    calls: AtomicUsize,
    value: &'static [u8],
}

impl Answer {
    fn new(value: &'static [u8]) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            value,
        }
    }
}

impl EventTarget for Answer {
    fn deliver(
        &self,
        _: u64,
        _: &str,
        _: Vec<u8>,
        _: Option<NonZeroU64>,
    ) -> KernelFuture<'static, Vec<u8>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let answer = self.value.to_vec();
        Box::pin(async move { Ok(answer) })
    }
}

struct Replacing(FiberId);

impl RestartOracle for Replacing {
    fn unserved(&self, fiber: FiberId) -> Option<Unserved> {
        (fiber == self.0).then(|| Unserved {
            entry: EntryId("consumer".to_owned()),
            incarnation: 7,
            owed: Owed::Reload,
        })
    }
}

fn consumer() -> EntryId {
    EntryId("consumer".to_owned())
}

fn notify_entries() -> Vec<serde_json::Value> {
    vec![
        dispatch::entry(
            "provider",
            serde_json::json!([
                "jinn:test/settings",
                NOTICE,
                "jinn:introspect",
                "jinn:fs",
                "jinn:clock",
                { "contract": "jinn:profile", "scope": ["consumer"] }
            ]),
            "notify-provider",
        ),
        dispatch::entry(
            "consumer",
            serde_json::json!(["jinn:test/settings", NOTICE, "jinn:fs", "jinn:clock"]),
            "notify-consumer",
        ),
        dispatch::entry(
            "trigger",
            serde_json::json!(["jinn:test/settings", "jinn:fs", "jinn:clock"]),
            "notify-trigger:consumer",
        ),
    ]
}

async fn observed_restart(
    name: &str,
) -> (
    dispatch::Home,
    Daemon,
    DaemonPaths,
    Vec<u8>,
    Vec<LedgerRecord>,
) {
    let home = dispatch::home(name);
    let paths = dispatch::paths(&home, notify_entries());
    let daemon = dispatch::booted(paths.clone()).await;
    let outcome =
        dispatch::wait_for(&paths.data.join("notify.out"), |bytes| !bytes.is_empty()).await;
    let records = dispatch::events(&daemon).await;
    (home, daemon, paths, outcome, records)
}

async fn shutdown(daemon: &Daemon) {
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

fn assert_restart_refusal(outcome: &[u8], records: &[LedgerRecord]) {
    assert_eq!(
        outcome.first(),
        Some(&1),
        "the guest received the typed restarting case: {outcome:?}"
    );
    assert!(records.iter().any(|record| matches!(
        &record.kind,
        LedgerEventKind::DispatchRefused {
            topic,
            mode: DispatchMode::Serial,
            target,
            owed: Owed::Reload,
            ..
        } if topic == NOTICE && target.0 == "consumer"
    )));
    assert!(
        !records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::DispatchTrace { topic, listeners: 0, .. } if topic == NOTICE
        )),
        "the restart window never looks like an honestly empty topic: {records:?}"
    );
}

async fn wait_for_state(daemon: &Daemon, entry: &str, wanted: FiberState) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        daemon.sync_transitions();
        if daemon
            .entry_fiber(entry)
            .and_then(|fiber| daemon.fiber_state(fiber))
            == Some(wanted)
        {
            return;
        }
        assert!(Instant::now() < deadline, "{entry} reaches {wanted:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reply_expecting_walk_inside_a_config_restart_is_refused_restarting_never_answered_unmodified()
 {
    let (_home, daemon, paths, outcome, records) = observed_restart("m2-k26-refusal").await;
    assert_restart_refusal(&outcome, &records);
    let consumer_log = std::fs::read(paths.data.join("consumer.log")).unwrap_or_default();
    assert!(
        !String::from_utf8_lossy(&consumer_log).contains("notice"),
        "the selected old incarnation never ran"
    );

    let sink = Arc::new(Recording::default());
    let topics = LocalTopics::traced(Arc::clone(&sink) as Arc<dyn LedgerSink>);
    topics.watch_restarts(Arc::new(Replacing(FiberId(9))));
    let old = Arc::new(Answer::new(b"old"));
    let id = topics.listen("before", 1, 1, Some(FiberId(9)), old.clone());
    topics.entomb(id, consumer(), 7);
    let report = topics
        .emit(
            2,
            "before",
            DispatchMode::Waterfall,
            &Selector::All,
            b"unmodified".to_vec(),
            Some(FiberId(2)),
            &NoRealms,
        )
        .await;
    assert_eq!(report.refused.map(|target| target.owed), Some(Owed::Reload));
    assert!(report.outputs.is_empty());
    assert_eq!(old.calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        sink.events().as_slice(),
        [(
            LedgerEventKind::DispatchRefused {
                mode: DispatchMode::Waterfall,
                ..
            },
            _
        )]
    ));
    shutdown(&daemon).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_commit_is_atomic_no_walk_sees_neither() {
    let topics = Arc::new(LocalTopics::default());
    topics.watch_restarts(Arc::new(Replacing(FiberId(9))));
    let old = Arc::new(Answer::new(b"old"));
    let id = topics.listen("t", 1, 1, Some(FiberId(9)), old.clone());
    topics.entomb(id, consumer(), 7);
    let fresh = Arc::new(Answer::new(b"new"));

    let mut walks = Vec::new();
    for _ in 0..200 {
        let topics = Arc::clone(&topics);
        walks.push(tokio::spawn(async move {
            topics
                .emit(
                    2,
                    "t",
                    DispatchMode::Serial,
                    &Selector::All,
                    Vec::new(),
                    None,
                    &NoRealms,
                )
                .await
        }));
    }
    tokio::task::yield_now().await;
    topics.rebind(
        &[id],
        vec![Rebind {
            topic: "t".to_owned(),
            context: 1,
            token: 2,
            fiber: Some(FiberId(10)),
            budget: None,
            target: fresh.clone(),
        }],
    );
    for walk in walks {
        let report = walk.await.unwrap_or_else(|error| panic!("walk: {error}"));
        assert!(
            report.refused.is_some() || report.outputs == [b"new".to_vec()],
            "every snapshot is the tombstone or the successor: {report:?}"
        );
    }
    assert_eq!(old.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_replacement_withdraws_its_tombstones_on_the_record() {
    let home = harness::home("m2-k26-failed");
    let initial = [harness::entry(
        "consumer",
        serde_json::json!([NOTICE, "jinn:fs", "jinn:clock"]),
        serde_json::json!([]),
        "notify-consumer",
    )];
    let (paths, hash) = harness::paths(&home, &initial);
    let daemon = harness::booted(paths).await;
    wait_for_state(&daemon, "consumer", FiberState::Active).await;
    harness::write_profile(
        &home,
        &[harness::entry(
            "consumer",
            serde_json::json!([NOTICE]),
            serde_json::json!([]),
            "trap",
        )],
        &hash,
    );
    let _ = daemon.reload().await;
    wait_for_state(&daemon, "consumer", FiberState::Failed).await;
    let records = ledger::events(&daemon).await;
    let failed = records
        .iter()
        .position(|record| {
            matches!(
                &record.kind,
                LedgerEventKind::FiberTransition(transition) if transition.to == FiberState::Failed
            )
        })
        .unwrap_or_else(|| panic!("Failed is recorded: {records:?}"));
    let withdrawn = records
        .iter()
        .position(|record| matches!(
            &record.kind,
            LedgerEventKind::EffectWithdrawn { label, .. } if label == &format!("listen {NOTICE}")
        ))
        .unwrap_or_else(|| panic!("the tombstone withdrawal is recorded: {records:?}"));
    assert!(
        withdrawn > failed,
        "withdrawal {withdrawn} follows Failed {failed}"
    );
    shutdown(&daemon).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_disposed_entry_leaves_no_tombstone() {
    let home = harness::home("m2-k26-disposed");
    let listener = harness::entry(
        "listener",
        serde_json::json!([TOPIC]),
        serde_json::json!([]),
        "listener",
    );
    let (paths, hash) = harness::paths(&home, std::slice::from_ref(&listener));
    let daemon = harness::booted(paths).await;
    wait_for_state(&daemon, "listener", FiberState::Active).await;
    harness::reload(&daemon, &home, &[], &hash).await;
    let emitter = harness::entry(
        "emitter",
        serde_json::json!([TOPIC]),
        serde_json::json!([]),
        "emitter",
    );
    harness::reload(&daemon, &home, &[emitter], &hash).await;
    wait_for_state(&daemon, "emitter", FiberState::Active).await;
    let records = ledger::events(&daemon).await;
    assert!(records.iter().any(|record| matches!(
        &record.kind,
        LedgerEventKind::DispatchTrace { topic, listeners: 0, .. } if topic == TOPIC
    )));
    assert!(!records.iter().any(|record| matches!(
        &record.kind,
        LedgerEventKind::DispatchRefused { topic, .. } if topic == TOPIC
    )));
    shutdown(&daemon).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_in_flight_load_answers_restarting_on_introspect_and_on_the_walk() {
    let (_home, daemon, paths, outcome, records) = observed_restart("m2-k26-introspect").await;
    assert_restart_refusal(&outcome, &records);
    let entries = dispatch::json(
        &std::fs::read(paths.data.join("notify-introspect.json"))
            .unwrap_or_else(|error| panic!("introspect snapshot: {error}")),
    );
    let consumer = entries
        .as_array()
        .and_then(|entries| entries.iter().find(|entry| entry["id"] == "consumer"))
        .unwrap_or_else(|| panic!("consumer is present: {entries}"));
    assert_eq!(consumer["state"], "loading");
    assert_eq!(consumer["unserved"], "restarting");
    shutdown(&daemon).await;
}

#[tokio::test]
async fn an_emit_mode_walk_in_the_window_is_lost_and_traced_as_today() {
    let sink = Arc::new(Recording::default());
    let topics = LocalTopics::traced(Arc::clone(&sink) as Arc<dyn LedgerSink>);
    topics.watch_restarts(Arc::new(Replacing(FiberId(9))));
    let old = Arc::new(Answer::new(b"old"));
    let id = topics.listen("t", 1, 1, Some(FiberId(9)), old.clone());
    topics.entomb(id, consumer(), 7);
    let report = topics
        .emit(
            2,
            "t",
            DispatchMode::Emit,
            &Selector::All,
            Vec::new(),
            None,
            &NoRealms,
        )
        .await;
    assert!(report.refused.is_none());
    assert_eq!(old.calls.load(Ordering::SeqCst), 0);
    assert!(matches!(
        sink.events().as_slice(),
        [(
            LedgerEventKind::DispatchTrace {
                mode: DispatchMode::Emit,
                listeners: 0,
                ..
            },
            _
        )]
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_emit_without_the_topics_grant_is_refused_on_the_record() {
    let denied_home = harness::home("m2-k26-ungranted");
    let denied = harness::entry(
        "emitter",
        serde_json::json!([]),
        serde_json::json!([]),
        "emitter",
    );
    let (denied_paths, _) = harness::paths(&denied_home, &[denied]);
    let denied_daemon =
        Daemon::open(denied_paths).unwrap_or_else(|error| panic!("open: {error:?}"));
    let _report = denied_daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot: {error:?}"));
    wait_for_state(&denied_daemon, "emitter", FiberState::Failed).await;
    let records = ledger::events(&denied_daemon).await;
    assert!(records.iter().any(|record| matches!(
        &record.kind,
        LedgerEventKind::GrantRefused { contract, .. } if contract == TOPIC
    )));
    assert!(!records.iter().any(|record| matches!(
        &record.kind,
        LedgerEventKind::DispatchTrace { topic, .. } if topic == TOPIC
    )));
    shutdown(&denied_daemon).await;

    let granted_home = harness::home("m2-k26-granted");
    let granted = harness::entry(
        "emitter",
        serde_json::json!([TOPIC]),
        serde_json::json!([]),
        "emitter",
    );
    let (granted_paths, _) = harness::paths(&granted_home, &[granted]);
    let granted_daemon = harness::booted(granted_paths).await;
    wait_for_state(&granted_daemon, "emitter", FiberState::Active).await;
    assert!(
        ledger::events(&granted_daemon)
            .await
            .iter()
            .any(|record| matches!(
                &record.kind,
                LedgerEventKind::DispatchTrace { topic, listeners: 0, .. } if topic == TOPIC
            ))
    );
    shutdown(&granted_daemon).await;
}

#[tokio::test]
async fn the_mode_1_swap_is_unchanged() {
    let topics = LocalTopics::default();
    let old = Arc::new(Answer::new(b"old"));
    let id = topics.listen("t", 1, 1, Some(FiberId(9)), old.clone());
    let fresh = Arc::new(Answer::new(b"new"));
    topics.rebind(
        &[id],
        vec![Rebind {
            topic: "t".to_owned(),
            context: 1,
            token: 2,
            fiber: Some(FiberId(9)),
            budget: None,
            target: fresh.clone(),
        }],
    );
    let report = topics
        .emit(
            2,
            "t",
            DispatchMode::Serial,
            &Selector::All,
            Vec::new(),
            None,
            &NoRealms,
        )
        .await;
    assert_eq!(report.outputs, [b"new".to_vec()]);
    assert_eq!(old.calls.load(Ordering::SeqCst), 0);
    assert_eq!(fresh.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_first_activation_is_never_refused() {
    let home = harness::home("m2-k26-first");
    let listener = harness::entry(
        "listener",
        serde_json::json!([TOPIC]),
        serde_json::json!([]),
        "listener",
    );
    let (paths, hash) = harness::paths(&home, std::slice::from_ref(&listener));
    let daemon = harness::booted(paths).await;
    wait_for_state(&daemon, "listener", FiberState::Active).await;
    let emitter = harness::entry(
        "emitter",
        serde_json::json!([TOPIC]),
        serde_json::json!([]),
        "emitter",
    );
    harness::reload(&daemon, &home, &[listener, emitter], &hash).await;
    wait_for_state(&daemon, "emitter", FiberState::Active).await;
    let records = ledger::events(&daemon).await;
    assert!(records.iter().any(|record| matches!(
        &record.kind,
        LedgerEventKind::DispatchTrace { topic, listeners: 1, .. } if topic == TOPIC
    )));
    assert!(!records.iter().any(|record| matches!(
        &record.kind,
        LedgerEventKind::DispatchRefused { topic, .. } if topic == TOPIC
    )));
    shutdown(&daemon).await;
}
