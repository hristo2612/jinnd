//! Alarm registry unit tests (crate lane). Paused tokio time drives the
//! timers deterministically; wakes land on a channel target.

use std::sync::{Arc, Mutex};

use jinnd_api::{FiberId, KernelError, KernelFuture, LedgerEventKind};
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

use super::{
    AlarmSpec, Alarms, ArmRequest, DEFAULT_MIN_PERIOD_MS, WAKE_TOPIC, clock_floor, now_unix_ms,
    validate,
};
use crate::lane::Grant;
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

type Wakes = mpsc::UnboundedReceiver<(u64, String, Vec<u8>)>;

fn fixture() -> (Arc<Alarms>, Arc<RecordingSink>, Arc<ChannelTarget>, Wakes) {
    let sink = Arc::new(RecordingSink::default());
    let alarms = Arc::new(Alarms::new(Arc::clone(&sink) as Arc<dyn LedgerSink>));
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
        wakes.iter().all(
            |(kind, fiber)| *kind == LedgerEventKind::AlarmWake { alarm: id }
                && *fiber == Some(FiberId(3))
        ),
        "attributed to the requesting fiber"
    );
}

#[tokio::test(start_paused = true)]
async fn cancel_stops_wakes_and_is_idempotent() {
    let (alarms, sink, target, mut rx) = fixture();
    let id = alarms.arm(request(AlarmSpec::Every(250), &target));
    rx.recv().await.unwrap_or_else(|| panic!("the first wake"));

    assert!(alarms.cancel(id), "the undo cancels the alarm (R5)");
    assert!(
        !alarms.cancel(id),
        "idempotent: the second cancel is a no-op"
    );

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
    let id = alarms.arm(request(
        AlarmSpec::At(now_unix_ms().saturating_sub(1)),
        &target,
    ));

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
        timeout(Duration::from_secs(60), old_rx.recv())
            .await
            .is_err(),
        "the displaced seat's alarm is gone"
    );
    assert!(sink.wakes().len() > before, "the new alarm keeps waking");
}

#[test]
fn a_period_finer_than_the_granted_floor_is_refused() {
    let refused: KernelError = match validate(&AlarmSpec::Every(10), 250) {
        Err(refused) => refused,
        Ok(()) => panic!("a period finer than the floor must refuse (R9)"),
    };
    assert!(refused.message.contains("250ms"), "{}", refused.message);
    validate(&AlarmSpec::Every(250), 250)
        .unwrap_or_else(|error| panic!("the floor itself is grantable: {error:?}"));
    validate(&AlarmSpec::At(0), 250)
        .unwrap_or_else(|error| panic!("a one-shot has no period to floor: {error:?}"));

    // The floor is the ENTRY's own grant scope (M2-K2): a coarser scoped
    // floor refuses a period the default floor would admit.
    let scoped: KernelError = match validate(&AlarmSpec::Every(250), 1000) {
        Err(refused) => refused,
        Ok(()) => panic!("a coarser scoped floor must bind its entry (R9)"),
    };
    assert!(scoped.message.contains("1000ms"), "{}", scoped.message);
    validate(&AlarmSpec::Every(1000), 1000)
        .unwrap_or_else(|error| panic!("the scoped floor itself is grantable: {error:?}"));
}

/// M2-K2: the effective floor comes from the entry's `jinn:clock` grant
/// scope — default when unscoped or ungranted, clamped so a zero scope
/// cannot nullify the floor into a busy-wake hazard (R9).
#[test]
fn the_effective_floor_comes_from_the_clock_grants_scope() {
    let scoped = |scope| Grant {
        contract: "jinn:clock".to_owned(),
        scope,
    };
    assert_eq!(clock_floor(&[scoped(None)]), DEFAULT_MIN_PERIOD_MS);
    assert_eq!(clock_floor(&[scoped(Some(1000))]), 1000);
    assert_eq!(clock_floor(&[scoped(Some(50))]), 50);
    assert_eq!(
        clock_floor(&[scoped(Some(0))]),
        1,
        "the absolute floor is 1ms"
    );
    assert_eq!(
        clock_floor(&[Grant {
            contract: "jinn:fs".to_owned(),
            scope: Some(9),
        }]),
        DEFAULT_MIN_PERIOD_MS,
        "another contract's scope never floors the clock"
    );
    assert_eq!(clock_floor(&[]), DEFAULT_MIN_PERIOD_MS);
}

#[test]
fn arming_without_a_timer_runtime_is_a_recorded_failure_never_a_dead_alarm() {
    let sink = Arc::new(RecordingSink::default());
    let alarms = Arc::new(Alarms::new(Arc::clone(&sink) as Arc<dyn LedgerSink>));
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
