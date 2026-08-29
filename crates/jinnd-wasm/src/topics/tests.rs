//! Topic registry unit tests (crate lane).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use jinnd_api::{
    DispatchMode, EntryId, ErrorCode, FiberId, KernelError, KernelFuture, LedgerEventKind, Owed,
};

use super::{EventTarget, LocalTopics, Rebind, RestartOracle, Unserved};
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

/// A fixed oracle: `doomed` names the one fiber that owes `owed`.
struct Doomed {
    doomed: FiberId,
    owed: Owed,
    asked: AtomicUsize,
}

impl RestartOracle for Doomed {
    fn unserved(&self, fiber: FiberId) -> Option<Unserved> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        (fiber == self.doomed).then(|| Unserved {
            entry: EntryId("consumer".to_owned()),
            incarnation: 7,
            owed: self.owed,
        })
    }
}

fn owing(fiber: FiberId, owed: Owed) -> Arc<Doomed> {
    Arc::new(Doomed {
        doomed: fiber,
        owed,
        asked: AtomicUsize::new(0),
    })
}

fn doomed(fiber: FiberId) -> Arc<Doomed> {
    owing(fiber, Owed::Reload)
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
        assert_eq!(
            refused,
            Unserved {
                entry: EntryId("consumer".to_owned()),
                incarnation: 7,
                owed: Owed::Reload,
            },
            "{mode:?}: the refusal is typed — entry, incarnation, next move"
        );
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
                    owed: Owed::Reload,
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

/// A listener the oracle does not name is served normally, so a transition
/// elsewhere never quarantines the topic — and a listener with no fiber at
/// all (the harness lane) is never gated.
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

/// Each disposition is REFUSED UNDER ITS OWN NAME, on the wire and on the
/// record (M2-K9). This is the whole point of the reason: a caller refused
/// by a fiber being DISPOSED must never be told to wait for a restart,
/// because a well-behaved caller obeying that instruction waits forever —
/// disposal is terminal. Suspension is its own answer too: a resume may
/// never come on its own. So is a STALL (round 3): nothing is scheduled
/// and nothing will be until the environment moves.
#[tokio::test]
async fn each_disposition_is_refused_under_its_own_name() {
    for owed in [
        Owed::Reload,
        Owed::Disposal,
        Owed::Suspension,
        Owed::Stalled,
    ] {
        let sink = Arc::new(RecordingSink::default());
        let topics = LocalTopics::traced(Arc::clone(&sink) as Arc<dyn LedgerSink>);
        topics.watch_restarts(owing(FiberId(9), owed) as Arc<dyn RestartOracle>);
        let target = Arc::new(Counted::default());
        topics.listen(
            "t",
            1,
            0,
            Some(FiberId(9)),
            Arc::clone(&target) as Arc<dyn EventTarget>,
        );

        let report = topics
            .emit(
                7,
                "t",
                DispatchMode::Serial,
                &Selector::All,
                Vec::new(),
                Some(FiberId(4)),
                &NoRealms,
            )
            .await;

        let refused = report
            .refused
            .clone()
            .unwrap_or_else(|| panic!("{owed:?} refuses: {report:?}"));
        assert_eq!(
            refused.owed, owed,
            "the refusal carries what the target ACTUALLY owes, never an \
             optimistic default: a caller acts on this"
        );
        assert_eq!(target.0.load(Ordering::SeqCst), 0, "{owed:?}: nothing ran");
        assert_eq!(
            sink.recorded(),
            vec![(
                LedgerEventKind::DispatchRefused {
                    topic: "t".to_owned(),
                    mode: DispatchMode::Serial,
                    target: EntryId("consumer".to_owned()),
                    incarnation: 7,
                    owed,
                },
                Some(FiberId(4)),
            )],
            "{owed:?}: the ledger reader tells the four apart too (Law 2)"
        );
    }
}

/// A target that blocks until released, so a delivery can be held in
/// flight across a swap commit.
struct Held {
    entered: tokio::sync::Semaphore,
    release: Arc<tokio::sync::Semaphore>,
    answer: Vec<u8>,
}

impl EventTarget for Held {
    fn deliver(&self, _: u64, _: &str, _: Vec<u8>) -> KernelFuture<'static, Vec<u8>> {
        self.entered.add_permits(1);
        let release = Arc::clone(&self.release);
        let answer = self.answer.clone();
        Box::pin(async move {
            let permit = release.acquire().await;
            drop(permit.map(tokio::sync::SemaphorePermit::forget));
            Ok(answer)
        })
    }
}

fn held(answer: &[u8]) -> Arc<Held> {
    Arc::new(Held {
        entered: tokio::sync::Semaphore::new(0),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
        answer: answer.to_vec(),
    })
}

/// An oracle that COMMITS THE SWAP as its answer, then admits the walk:
/// the hostile interleaving the card names, forced rather than raced for.
/// By the time the walk dispatches, the registry has already been rebound
/// to the successor incarnation — exactly the instant a check "one moment
/// too early" leaves behind.
struct SwapsThenAdmits {
    topics: Arc<LocalTopics>,
    old: Mutex<Vec<u64>>,
    successor: Arc<Counted>,
    swapped: AtomicUsize,
}

impl RestartOracle for SwapsThenAdmits {
    fn unserved(&self, _: FiberId) -> Option<Unserved> {
        if self.swapped.fetch_add(1, Ordering::SeqCst) == 0 {
            let old = std::mem::take(&mut *self.old.lock().unwrap_or_else(|p| p.into_inner()));
            self.topics.rebind(
                &old,
                vec![Rebind {
                    topic: "t".to_owned(),
                    context: 1,
                    token: 0,
                    fiber: Some(FiberId(4)),
                    target: Arc::clone(&self.successor) as Arc<dyn EventTarget>,
                }],
            );
        }
        None
    }
}

/// The card's hostile race, closed by construction: a walk ADMITTED an
/// instant before the swap commits is never accepted-then-orphaned. The
/// listener set the walk acts on is SNAPSHOTTED under the registry lock
/// before the check, so a `rebind` landing between the check and the
/// dispatch can neither steal the walk nor half-land it: the admitted walk
/// settles against the incarnation it was admitted for, exactly once, and
/// the successor — which the emitter never selected — is not entered.
///
/// The swap here is the real production commit primitive
/// ([`LocalTopics::rebind`], the one Mode-1 uses), driven from inside the
/// check itself so the interleaving is forced and not hoped for.
#[tokio::test]
async fn a_swap_committed_between_the_check_and_the_dispatch_never_orphans_the_walk() {
    let sink = Arc::new(RecordingSink::default());
    let topics = Arc::new(LocalTopics::traced(Arc::clone(&sink) as Arc<dyn LedgerSink>));
    let admitted = Arc::new(Counted::default());
    let successor = Arc::new(Counted::default());
    let id = topics.listen(
        "t",
        1,
        0,
        Some(FiberId(4)),
        Arc::clone(&admitted) as Arc<dyn EventTarget>,
    );
    topics.watch_restarts(Arc::new(SwapsThenAdmits {
        topics: Arc::clone(&topics),
        old: Mutex::new(vec![id]),
        successor: Arc::clone(&successor),
        swapped: AtomicUsize::new(0),
    }) as Arc<dyn RestartOracle>);

    let report = topics
        .emit(
            7,
            "t",
            DispatchMode::Serial,
            &Selector::All,
            Vec::new(),
            Some(FiberId(4)),
            &NoRealms,
        )
        .await;

    assert!(report.refused.is_none(), "the check admitted it");
    assert_eq!(
        report.outputs,
        vec![b"served".to_vec()],
        "the admitted walk settled with an answer, never orphaned"
    );
    assert_eq!(
        admitted.0.load(Ordering::SeqCst),
        1,
        "delivered to the incarnation it was admitted for, exactly once"
    );
    assert_eq!(
        successor.0.load(Ordering::SeqCst),
        0,
        "a walk decided before the swap never re-targets across it"
    );
    assert!(
        matches!(
            sink.recorded().as_slice(),
            [(LedgerEventKind::DispatchTrace { listeners: 1, .. }, _)]
        ),
        "an admitted walk traces what it dispatched: {:?}",
        sink.recorded()
    );
}

/// The other half of "never orphaned": a delivery held IN FLIGHT while the
/// swap commits still returns to its emitter — `rebind` withdraws a
/// registration, it never cancels a delivery already inside one.
///
/// Stated plainly: this is a CHARACTERIZATION pin, not a guard probe.
/// There is no production guard to revert here — the property holds
/// because no cancellation path exists — so it carries no red-first
/// evidence, and it is here to fail the day someone adds one.
#[tokio::test]
async fn a_delivery_in_flight_across_a_swap_still_answers_its_emitter() {
    let topics = Arc::new(LocalTopics::default());
    let target = held(b"late");
    let id = topics.listen(
        "t",
        1,
        0,
        Some(FiberId(4)),
        Arc::clone(&target) as Arc<dyn EventTarget>,
    );
    let walk = {
        let topics = Arc::clone(&topics);
        tokio::spawn(async move {
            topics
                .emit(
                    7,
                    "t",
                    DispatchMode::Serial,
                    &Selector::All,
                    Vec::new(),
                    Some(FiberId(4)),
                    &NoRealms,
                )
                .await
        })
    };
    // The delivery is inside the target; now commit the swap under it.
    let entered = target
        .entered
        .acquire()
        .await
        .unwrap_or_else(|error| panic!("{error}"));
    entered.forget();
    topics.rebind(
        &[id],
        vec![Rebind {
            topic: "t".to_owned(),
            context: 1,
            token: 0,
            fiber: Some(FiberId(4)),
            target: Arc::new(Answer(b"successor".to_vec())) as Arc<dyn EventTarget>,
        }],
    );
    target.release.add_permits(1);
    let report = walk.await.unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        report.outputs,
        vec![b"late".to_vec()],
        "the in-flight delivery answered the emitter that was waiting on it"
    );
}
