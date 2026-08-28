//! Alarm registry unit tests (crate lane). Paused tokio time drives the
//! timers deterministically; wakes land on a channel target.

use std::sync::{Arc, Mutex};

use jinnd_api::{FiberId, KernelError, KernelFuture, LedgerEventKind};
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

use super::{AlarmSpec, Alarms, ArmRequest, WAKE_TOPIC, now_unix_ms};
use crate::peer::LedgerSink;
use crate::topics::EventTarget;

#[derive(Default)]
struct RecordingSink(Mutex<Vec<(LedgerEventKind, Option<FiberId>)>>);

impl LedgerSink for RecordingSink {
    fn append(&self, kind: LedgerEventKind, fiber: Option<FiberId>) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((kind, fiber));
    }
}

impl RecordingSink {
    fn wakes(&self) -> Vec<(LedgerEventKind, Option<FiberId>)> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .filter(|(kind, _)| matches!(kind, LedgerEventKind::AlarmWake { .. }))
            .cloned()
            .collect()
    }
}

struct ChannelTarget(mpsc::UnboundedSender<(u64, String, Vec<u8>)>);

impl EventTarget for ChannelTarget {
    fn deliver(&self, token: u64, topic: &str, payload: Vec<u8>) -> KernelFuture<'static, Vec<u8>> {
        let _ = self.0.send((token, topic.to_owned(), payload));
        Box::pin(async { Ok(Vec::new()) })
    }
}

fn fixture() -> (
    Arc<Alarms>,
    Arc<RecordingSink>,
    Arc<ChannelTarget>,
    mpsc::UnboundedReceiver<(u64, String, Vec<u8>)>,
) {
    let sink = Arc::new(RecordingSink::default());
    let alarms = Arc::new(Alarms::new(
        Arc::clone(&sink) as Arc<dyn LedgerSink>,
        250,
    ));
    let (tx, rx) = mpsc::unbounded_channel();
    (alarms, sink, Arc::new(ChannelTarget(tx)), rx)
}

fn request(spec: AlarmSpec, target: &Arc<ChannelTarget>) -> ArmRequest {
    ArmRequest {
        spec,
        token: 9,
        fiber: Some(FiberId(3)),
        target: Arc::clone(target) as Arc<dyn EventTarget>,
    }
}

#[tokio::test(start_paused = true)]
async fn a_periodic_alarm_delivers_typed_wakes_and_ledgers_each_one() {
    let (alarms, sink, target, mut rx) = fixture();
    let id = alarms.arm(request(AlarmSpec::Every(250), &target));

    for _ in 0..3 {
        let (token, topic, payload) = rx.recv().await.unwrap_or_else(|| panic!("a wake"));
        assert_eq!(token, 9, "delivered under the requesting token");
        assert_eq!(topic, WAKE_TOPIC);
        assert_eq!(payload.len(), 8, "8-byte LE wake instant, per the contract");
    }
    let wakes = sink.wakes();
    assert!(wakes.len() >= 3, "every wake is a ledger event (Law 2)");
    assert!(
        wakes
            .iter()
            .all(|(kind, fiber)| *kind == LedgerEventKind::AlarmWake { alarm: id }
                && *fiber == Some(FiberId(3))),
        "attributed to the requesting fiber"
    );
}

#[tokio::test(start_paused = true)]
async fn cancel_stops_wakes_and_is_idempotent() {
    let (alarms, sink, target, mut rx) = fixture();
    let id = alarms.arm(request(AlarmSpec::Every(250), &target));
    rx.recv().await.unwrap_or_else(|| panic!("the first wake"));

    assert!(alarms.cancel(id), "the undo cancels the alarm (R5)");
    assert!(!alarms.cancel(id), "idempotent: the second cancel is a no-op");

    let ledgered = sink.wakes().len();
    assert!(
        timeout(Duration::from_secs(60), rx.recv()).await.is_err(),
        "no wake is delivered after cancel"
    );
    assert_eq!(sink.wakes().len(), ledgered, "and none is ledgered either");
}

#[tokio::test(start_paused = true)]
async fn an_at_alarm_fires_exactly_once_then_completes() {
    let (alarms, sink, target, mut rx) = fixture();
    let id = alarms.arm(request(AlarmSpec::At(now_unix_ms().saturating_sub(1)), &target));

    rx.recv().await.unwrap_or_else(|| panic!("the one wake"));
    assert!(
        timeout(Duration::from_secs(60), rx.recv()).await.is_err(),
        "a one-shot never fires twice"
    );
    assert_eq!(sink.wakes().len(), 1);
    assert!(!alarms.cancel(id), "completion already took the row");
}

#[tokio::test(start_paused = true)]
async fn rebind_cancels_the_displaced_alarms_and_arms_the_staged_ones() {
    let (alarms, sink, old_target, mut old_rx) = fixture();
    let old = alarms.arm(request(AlarmSpec::Every(250), &old_target));
    old_rx.recv().await.unwrap_or_else(|| panic!("a wake"));

    let (tx, mut new_rx) = mpsc::unbounded_channel();
    let new_target = Arc::new(ChannelTarget(tx));
    let minted = alarms.rebind(&[old], vec![request(AlarmSpec::Every(250), &new_target)]);
    assert_eq!(minted.len(), 1);
    assert_ne!(minted[0], old, "ids are never reused");

    new_rx
        .recv()
        .await
        .unwrap_or_else(|| panic!("the new seat's wake"));
    let before = sink.wakes().len();
    assert!(
        timeout(Duration::from_secs(60), old_rx.recv()).await.is_err(),
        "the displaced seat's alarm is gone"
    );
    assert!(sink.wakes().len() > before, "the new alarm keeps waking");
}

#[test]
fn a_period_finer_than_the_floor_is_refused() {
    let sink = Arc::new(RecordingSink::default());
    let alarms = Alarms::new(Arc::clone(&sink) as Arc<dyn LedgerSink>, 250);
    let refused: KernelError = alarms
        .validate(&AlarmSpec::Every(10))
        .expect_err("finer than the floor (R9)");
    assert!(refused.message.contains("250ms"), "{}", refused.message);
    alarms
        .validate(&AlarmSpec::Every(250))
        .unwrap_or_else(|error| panic!("the floor itself is grantable: {error:?}"));
    alarms
        .validate(&AlarmSpec::At(0))
        .unwrap_or_else(|error| panic!("a one-shot has no period to floor: {error:?}"));
}

#[test]
fn arming_without_a_timer_runtime_is_a_recorded_failure_never_a_dead_alarm() {
    let sink = Arc::new(RecordingSink::default());
    let alarms = Arc::new(Alarms::new(Arc::clone(&sink) as Arc<dyn LedgerSink>, 250));
    let (tx, _rx) = mpsc::unbounded_channel();
    let target = Arc::new(ChannelTarget(tx));

    let id = alarms.arm(request(AlarmSpec::Every(250), &target));
    assert!(!alarms.cancel(id), "the row was withdrawn, not left dead");
    let recorded = sink
        .0
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
        .iter()
        .any(|(kind, _)| matches!(kind, LedgerEventKind::ErrorRecorded { .. }));
    assert!(recorded, "the refusal is a ledger event (R6)");
}
