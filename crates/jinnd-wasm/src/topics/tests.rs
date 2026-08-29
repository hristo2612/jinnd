//! Topic registry unit tests (crate lane).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use jinnd_api::{
    DispatchMode, EntryId, ErrorCode, FiberId, KernelError, KernelFuture, LedgerEventKind,
};

use super::{EventTarget, LocalTopics, RestartOracle, Restarting};
use crate::peer::LedgerSink;
use crate::selector::{NoRealms, Selector};

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
    fn recorded(&self) -> Vec<(LedgerEventKind, Option<FiberId>)> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }
}

struct Answer(Vec<u8>);

impl EventTarget for Answer {
    fn deliver(&self, _: u64, _: &str, _: Vec<u8>) -> KernelFuture<'static, Vec<u8>> {
        let answer = self.0.clone();
        Box::pin(async move { Ok(answer) })
    }
}

struct Failing;

impl EventTarget for Failing {
    fn deliver(&self, _: u64, _: &str, _: Vec<u8>) -> KernelFuture<'static, Vec<u8>> {
        Box::pin(async {
            Err(KernelError {
                code: ErrorCode::ListenerFailed,
                message: "listener failed".into(),
                fiber: None,
            })
        })
    }
}

#[tokio::test]
async fn every_traced_emit_lands_exactly_one_dispatch_trace() {
    let sink = Arc::new(RecordingSink::default());
    let topics = LocalTopics::traced(Arc::clone(&sink) as Arc<dyn LedgerSink>);
    topics.listen("t", 1, 0, None, Arc::new(Failing));
    topics.listen("t", 2, 0, None, Arc::new(Answer(b"ok".to_vec())));
    topics.listen("other", 3, 0, None, Arc::new(Answer(b"off-topic".to_vec())));

    let report = topics
        .emit(
            7,
            "t",
            DispatchMode::Parallel,
            &Selector::All,
            Vec::new(),
            Some(FiberId(3)),
            &NoRealms,
        )
        .await;
    assert_eq!(report.outputs, vec![b"ok".to_vec()]);

    let recorded = sink.recorded();
    assert_eq!(recorded.len(), 1, "exactly one trace per emit");
    let (kind, fiber) = &recorded[0];
    assert_eq!(
        *fiber,
        Some(FiberId(3)),
        "emitter fiber attribution (Law 2)"
    );
    assert_eq!(
        *kind,
        LedgerEventKind::DispatchTrace {
            topic: "t".to_owned(),
            mode: DispatchMode::Parallel,
            listeners: 2,
            failures: 1,
            emitter: 7,
        }
    );
}

#[tokio::test]
async fn a_listenerless_traced_emit_still_traces() {
    let sink = Arc::new(RecordingSink::default());
    let topics = LocalTopics::traced(Arc::clone(&sink) as Arc<dyn LedgerSink>);
    topics
        .emit(
            1,
            "quiet",
            DispatchMode::Emit,
            &Selector::All,
            Vec::new(),
            None,
            &NoRealms,
        )
        .await;
    assert_eq!(
        sink.recorded(),
        vec![(
            LedgerEventKind::DispatchTrace {
                topic: "quiet".to_owned(),
                mode: DispatchMode::Emit,
                listeners: 0,
                failures: 0,
                emitter: 1,
            },
            None
        )]
    );
}

#[tokio::test]
async fn failing_listener_never_aborts_a_collecting_walk() {
    let topics = LocalTopics::default();
    topics.listen("t", 1, 0, None, Arc::new(Failing));
    topics.listen("t", 2, 0, None, Arc::new(Answer(b"ok".to_vec())));
    let report = topics
        .emit(
            0,
            "t",
            DispatchMode::Parallel,
            &Selector::All,
            Vec::new(),
            None,
            &NoRealms,
        )
        .await;
    assert_eq!(report.outputs, vec![b"ok".to_vec()]);
    assert_eq!(report.failures.len(), 1, "contained and recorded (R9)");
}

#[tokio::test]
async fn bail_takes_the_first_non_empty_output_only() {
    let topics = LocalTopics::default();
    topics.listen("t", 1, 0, None, Arc::new(Answer(Vec::new())));
    topics.listen("t", 2, 0, None, Arc::new(Answer(b"first".to_vec())));
    topics.listen("t", 3, 0, None, Arc::new(Answer(b"second".to_vec())));
    let report = topics
        .emit(
            0,
            "t",
            DispatchMode::Bail,
            &Selector::All,
            Vec::new(),
            None,
            &NoRealms,
        )
        .await;
    assert_eq!(report.outputs, vec![b"first".to_vec()]);
}

#[tokio::test]
async fn waterfall_folds_non_empty_outputs_into_the_payload() {
    let topics = LocalTopics::default();
    topics.listen("t", 1, 0, None, Arc::new(Answer(b"a".to_vec())));
    topics.listen("t", 2, 0, None, Arc::new(Answer(Vec::new())));
    topics.listen("t", 3, 0, None, Arc::new(Answer(b"b".to_vec())));
    let report = topics
        .emit(
            0,
            "t",
            DispatchMode::Waterfall,
            &Selector::All,
            b"seed".to_vec(),
            None,
            &NoRealms,
        )
        .await;
    assert_eq!(report.outputs, vec![b"b".to_vec()]);
}

#[tokio::test]
async fn selector_and_unlisten_gate_delivery() {
    let topics = LocalTopics::default();
    let selected = topics.listen("t", 1, 0, None, Arc::new(Answer(b"in".to_vec())));
    topics.listen("t", 2, 0, None, Arc::new(Answer(b"out".to_vec())));
    let report = topics
        .emit(
            0,
            "t",
            DispatchMode::Serial,
            &Selector::ContextSet(vec![1]),
            Vec::new(),
            None,
            &NoRealms,
        )
        .await;
    assert_eq!(report.outputs, vec![b"in".to_vec()]);

    assert_eq!(
        topics.unlisten(selected),
        Some("t".to_owned()),
        "withdrawal returns the topic — the caller's Law-2 label"
    );
    assert_eq!(
        topics.unlisten(selected),
        None,
        "idempotent: the second withdrawal is a no-op"
    );
    let after = topics
        .emit(
            0,
            "t",
            DispatchMode::Serial,
            &Selector::ContextSet(vec![1]),
            Vec::new(),
            None,
            &NoRealms,
        )
        .await;
    assert!(after.outputs.is_empty(), "withdrawn, idempotently");
}

/// A counting target: what it answered, and how often it was entered at
/// all — a refused walk must never enter one.
#[derive(Default)]
struct Counted(AtomicUsize);

impl EventTarget for Counted {
    fn deliver(&self, _: u64, _: &str, _: Vec<u8>) -> KernelFuture<'static, Vec<u8>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(b"served".to_vec()) })
    }
}

/// A fixed oracle: `doomed` names the one fiber being replaced.
struct Doomed {
    doomed: FiberId,
    asked: AtomicUsize,
}

impl RestartOracle for Doomed {
    fn restarting(&self, fiber: FiberId) -> Option<Restarting> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        (fiber == self.doomed).then(|| Restarting {
            entry: EntryId("consumer".to_owned()),
            incarnation: 7,
        })
    }
}

fn doomed(fiber: FiberId) -> Arc<Doomed> {
    Arc::new(Doomed {
        doomed: fiber,
        asked: AtomicUsize::new(0),
    })
}

/// The M2-K9 rule: a reply-expecting walk selecting a listener whose
/// incarnation is being replaced refuses WHOLE — nothing delivered, the
/// caller told typed, the refusal on the record naming target and
/// incarnation, and no trace, because nothing dispatched.
#[tokio::test]
async fn a_reply_expecting_walk_refuses_before_it_dispatches() {
    for mode in [
        DispatchMode::Serial,
        DispatchMode::Parallel,
        DispatchMode::Bail,
        DispatchMode::Waterfall,
    ] {
        let sink = Arc::new(RecordingSink::default());
        let topics = LocalTopics::traced(Arc::clone(&sink) as Arc<dyn LedgerSink>);
        topics.watch_restarts(doomed(FiberId(9)) as Arc<dyn RestartOracle>);
        let healthy = Arc::new(Counted::default());
        let replaced = Arc::new(Counted::default());
        topics.listen(
            "t",
            1,
            0,
            Some(FiberId(4)),
            Arc::clone(&healthy) as Arc<dyn EventTarget>,
        );
        topics.listen(
            "t",
            2,
            0,
            Some(FiberId(9)),
            Arc::clone(&replaced) as Arc<dyn EventTarget>,
        );

        let report = topics
            .emit(
                7,
                "t",
                mode,
                &Selector::All,
                Vec::new(),
                Some(FiberId(4)),
                &NoRealms,
            )
            .await;

        let refused = report
            .refused
            .clone()
            .unwrap_or_else(|| panic!("{mode:?} refuses: {report:?}"));
        assert_eq!(refused.code, ErrorCode::Restarting, "{mode:?}");
        assert!(refused.message.contains("consumer"), "{}", refused.message);
        assert!(
            report.outputs.is_empty() && report.failures.is_empty(),
            "{mode:?}"
        );
        // Never HALF-landed: the healthy listener selected first is not
        // delivered to either — the walk is decided, then dispatched.
        assert_eq!(healthy.0.load(Ordering::SeqCst), 0, "{mode:?}");
        assert_eq!(replaced.0.load(Ordering::SeqCst), 0, "{mode:?}");
        assert_eq!(
            sink.recorded(),
            vec![(
                LedgerEventKind::DispatchRefused {
                    topic: "t".to_owned(),
                    mode,
                    target: EntryId("consumer".to_owned()),
                    incarnation: 7,
                },
                Some(FiberId(4)),
            )],
            "{mode:?}: the refusal is the row, and a walk that dispatched \
             nothing traces nothing"
        );
    }
}

/// Fire-and-forget is UNAFFECTED, and proven so: `emit` still delivers to
/// a listener whose incarnation is being replaced, and traces as always.
/// Its outputs are discarded, so its caller waits on no listener's answer.
#[tokio::test]
async fn fire_and_forget_still_delivers_into_a_replaced_incarnation() {
    let sink = Arc::new(RecordingSink::default());
    let topics = LocalTopics::traced(Arc::clone(&sink) as Arc<dyn LedgerSink>);
    topics.watch_restarts(doomed(FiberId(9)) as Arc<dyn RestartOracle>);
    let replaced = Arc::new(Counted::default());
    topics.listen(
        "t",
        1,
        0,
        Some(FiberId(9)),
        Arc::clone(&replaced) as Arc<dyn EventTarget>,
    );

    let report = topics
        .emit(
            1,
            "t",
            DispatchMode::Emit,
            &Selector::All,
            Vec::new(),
            None,
            &NoRealms,
        )
        .await;

    assert!(report.refused.is_none(), "emit is never refused");
    assert_eq!(replaced.0.load(Ordering::SeqCst), 1, "delivered, as before");
    assert_eq!(
        sink.recorded(),
        vec![(
            LedgerEventKind::DispatchTrace {
                topic: "t".to_owned(),
                mode: DispatchMode::Emit,
                listeners: 1,
                failures: 0,
                emitter: 1,
            },
            None
        )]
    );
}

/// The rule is cause-agnostic: the oracle is asked about the FIBER, never
/// about why it owes a transition — a config patch, a dependency-epoch
/// change, an operator restart and a disposal are one case here. And a
/// listener the oracle does not name is served normally, so a restart
/// elsewhere never quarantines the topic.
#[tokio::test]
async fn only_the_replaced_listener_gates_the_walk() {
    let topics = LocalTopics::default();
    topics.watch_restarts(doomed(FiberId(9)) as Arc<dyn RestartOracle>);
    let healthy = Arc::new(Counted::default());
    topics.listen(
        "t",
        1,
        0,
        Some(FiberId(4)),
        Arc::clone(&healthy) as Arc<dyn EventTarget>,
    );
    // A listener with no fiber at all (the harness lane) is never gated.
    topics.listen("t", 2, 0, None, Arc::new(Answer(b"anon".to_vec())));
    let report = topics
        .emit(
            0,
            "t",
            DispatchMode::Serial,
            &Selector::All,
            Vec::new(),
            None,
            &NoRealms,
        )
        .await;
    assert!(report.refused.is_none());
    assert_eq!(report.outputs, vec![b"served".to_vec(), b"anon".to_vec()]);
    assert_eq!(healthy.0.load(Ordering::SeqCst), 1);
}

/// A refusal storm during one replacement stays cheap and honest: every
/// attempt is refused and recorded, none reaches the guest, and the oracle
/// is the only thing consulted (no lock is held across a delivery, because
/// there is no delivery).
#[tokio::test]
async fn a_refusal_storm_records_every_attempt_and_reaches_no_guest() {
    let sink = Arc::new(RecordingSink::default());
    let topics = LocalTopics::traced(Arc::clone(&sink) as Arc<dyn LedgerSink>);
    topics.watch_restarts(doomed(FiberId(9)) as Arc<dyn RestartOracle>);
    let replaced = Arc::new(Counted::default());
    topics.listen(
        "t",
        1,
        0,
        Some(FiberId(9)),
        Arc::clone(&replaced) as Arc<dyn EventTarget>,
    );
    for _ in 0..32 {
        let report = topics
            .emit(
                1,
                "t",
                DispatchMode::Serial,
                &Selector::All,
                Vec::new(),
                None,
                &NoRealms,
            )
            .await;
        assert!(report.refused.is_some());
    }
    assert_eq!(replaced.0.load(Ordering::SeqCst), 0, "no guest was entered");
    assert_eq!(
        sink.recorded().len(),
        32,
        "every refusal is history (Law 2)"
    );
}

/// The race the check cannot close, claimed honestly: a walk ADMITTED an
/// instant before the swap commits still settles. The seat's own gate
/// answers a delivery it can no longer serve with its typed sealed
/// refusal, which the walk contains and reports (R9/R11) — accepted, then
/// answered; never accepted, then orphaned.
#[tokio::test]
async fn a_walk_admitted_just_before_the_swap_settles_rather_than_orphaning() {
    let topics = LocalTopics::default();
    // The oracle answers None: the check passed a moment too early.
    topics.watch_restarts(doomed(FiberId(9)) as Arc<dyn RestartOracle>);
    topics.listen("t", 1, 0, Some(FiberId(4)), Arc::new(Failing));
    let report = topics
        .emit(
            0,
            "t",
            DispatchMode::Serial,
            &Selector::All,
            Vec::new(),
            None,
            &NoRealms,
        )
        .await;
    assert!(report.refused.is_none(), "the check admitted it");
    assert_eq!(report.failures.len(), 1, "answered, not orphaned");
    assert!(report.outputs.is_empty());
}
