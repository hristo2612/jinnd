//! The M2-K9 rule itself: which walks refuse, which still deliver, and
//! what the ledger keeps of either. The disposition VOCABULARY is pinned
//! next door; this half is about the refusal's shape and its blast radius.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use jinnd_api::{DispatchMode, EntryId, FiberId, LedgerEventKind, Owed};

use super::{Answer, Counted, EventTarget, RecordingSink, RestartOracle, Unserved, doomed};
use crate::peer::LedgerSink;
use crate::selector::{NoRealms, Selector};
use crate::topics::LocalTopics;

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
