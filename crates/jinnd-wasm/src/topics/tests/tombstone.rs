//! The restart-window rule (M2-K26; harness FINDINGS #47): a listen
//! registration outlives its instance's suspension as a TOMBSTONE — the
//! same row, no delivery target — selected exactly as the registration
//! was, refused by the M2-K9 rule for every reply-expecting walk, skipped
//! by fire-and-forget, replaced atomically by `rebind`, and withdrawn on
//! the record when the fiber rests without a successor.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use jinnd_api::{DispatchMode, EntryId, FiberId, LedgerEventKind, Owed};

use super::{Counted, EventTarget, RecordingSink, RestartOracle, Unserved, doomed};
use crate::peer::LedgerSink;
use crate::selector::{NoRealms, Selector};
use crate::topics::{LocalTopics, Rebind};

fn consumer() -> EntryId {
    EntryId("consumer".to_owned())
}

/// The #47 shape closed at the port: with the registration entombed, a
/// reply-expecting walk still SELECTS it and is refused whole by the
/// oracle's answer — never `listeners: 0`, never answered unmodified.
#[tokio::test]
async fn a_tombstone_is_selected_and_refused_exactly_as_its_registration_was() {
    for mode in [
        DispatchMode::Serial,
        DispatchMode::Parallel,
        DispatchMode::Bail,
        DispatchMode::Waterfall,
    ] {
        let sink = Arc::new(RecordingSink::default());
        let topics = LocalTopics::traced(Arc::clone(&sink) as Arc<dyn LedgerSink>);
        topics.watch_restarts(doomed(FiberId(9)) as Arc<dyn RestartOracle>);
        let replaced = Arc::new(Counted::default());
        let id = topics.listen(
            "t",
            2,
            0,
            Some(FiberId(9)),
            Arc::clone(&replaced) as Arc<dyn EventTarget>,
        );
        assert_eq!(topics.entomb(id, consumer(), 7), Some("t".to_owned()));

        let report = topics
            .emit(
                7,
                "t",
                mode,
                &Selector::All,
                b"hello".to_vec(),
                Some(FiberId(4)),
                &NoRealms,
            )
            .await;

        assert_eq!(
            report.refused,
            Some(Unserved {
                entry: consumer(),
                incarnation: 7,
                owed: Owed::Reload,
            }),
            "{mode:?}: the tombstone is refused with the oracle's answer"
        );
        assert!(
            report.outputs.is_empty(),
            "{mode:?}: never answered unmodified"
        );
        assert_eq!(replaced.0.load(Ordering::SeqCst), 0, "{mode:?}");
        assert_eq!(
            sink.recorded(),
            vec![(
                LedgerEventKind::DispatchRefused {
                    topic: "t".to_owned(),
                    mode,
                    target: consumer(),
                    incarnation: 7,
                    owed: Owed::Reload,
                },
                Some(FiberId(4)),
            )],
            "{mode:?}: the refusal is the row; no `listeners: 0` trace"
        );
    }
}

/// The named limit, pinned: fire-and-forget skips tombstones and traces
/// the empty walk as it does today.
#[tokio::test]
async fn an_emit_mode_walk_skips_a_tombstone_and_traces_as_today() {
    let sink = Arc::new(RecordingSink::default());
    let topics = LocalTopics::traced(Arc::clone(&sink) as Arc<dyn LedgerSink>);
    topics.watch_restarts(doomed(FiberId(9)) as Arc<dyn RestartOracle>);
    let id = topics.listen("t", 2, 0, Some(FiberId(9)), Arc::new(Counted::default()));
    topics.entomb(id, consumer(), 7);

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

    assert!(
        report.refused.is_none(),
        "emit is never refused for a tombstone"
    );
    assert_eq!(
        sink.recorded(),
        vec![(
            LedgerEventKind::DispatchTrace {
                topic: "t".to_owned(),
                mode: DispatchMode::Emit,
                listeners: 0,
                failures: 0,
                emitter: 1,
            },
            None
        )]
    );
}

/// A tombstone the oracle no longer explains — the instant between a rest
/// commit and the withdrawal, or a fiber outside the oracle's
/// jurisdiction — is refused `stalled` from the row's own identity: never
/// a delivery to nobody (R9), and never a `Reload` nobody earned.
#[tokio::test]
async fn a_tombstone_the_oracle_cannot_explain_is_refused_stalled() {
    let sink = Arc::new(RecordingSink::default());
    let topics = LocalTopics::traced(Arc::clone(&sink) as Arc<dyn LedgerSink>);
    topics.watch_restarts(doomed(FiberId(1)) as Arc<dyn RestartOracle>);
    let healthy = Arc::new(Counted::default());
    topics.listen(
        "t",
        1,
        0,
        Some(FiberId(4)),
        Arc::clone(&healthy) as Arc<dyn EventTarget>,
    );
    let id = topics.listen("t", 2, 0, Some(FiberId(9)), Arc::new(Counted::default()));
    topics.entomb(id, consumer(), 3);

    let report = topics
        .emit(
            7,
            "t",
            DispatchMode::Waterfall,
            &Selector::All,
            b"x".to_vec(),
            None,
            &NoRealms,
        )
        .await;

    assert_eq!(
        report.refused,
        Some(Unserved {
            entry: consumer(),
            incarnation: 3,
            owed: Owed::Stalled,
        })
    );
    assert_eq!(
        healthy.0.load(Ordering::SeqCst),
        0,
        "decided, then dispatched: nobody ran"
    );
    assert!(matches!(
        sink.recorded().as_slice(),
        [(
            LedgerEventKind::DispatchRefused {
                owed: Owed::Stalled,
                incarnation: 3,
                ..
            },
            None
        )]
    ));
}

/// The Mode-1 commit shape for Mode 0 (R8): ONE `rebind` replaces the
/// entry's tombstones with its staged listens under one lock, so a walk
/// sees tombstones (refused) before it and live listeners after — never
/// neither.
#[tokio::test]
async fn rebind_replaces_a_fibers_tombstones_with_its_staged_listens() {
    let topics = LocalTopics::default();
    topics.watch_restarts(doomed(FiberId(9)) as Arc<dyn RestartOracle>);
    let a = topics.listen("t", 2, 0, Some(FiberId(9)), Arc::new(Counted::default()));
    let b = topics.listen("u", 2, 1, Some(FiberId(9)), Arc::new(Counted::default()));
    topics.listen("t", 3, 0, Some(FiberId(5)), Arc::new(Counted::default()));
    topics.entomb(a, consumer(), 7);
    topics.entomb(b, consumer(), 7);
    assert_eq!(
        topics.entombed(FiberId(9)),
        vec![(a, "t".to_owned()), (b, "u".to_owned())],
        "exactly this fiber's tombstones, in registration order (I1)"
    );
    assert!(topics.entombed(FiberId(5)).is_empty());

    let successor = Arc::new(Counted::default());
    let ids: Vec<u64> = topics
        .entombed(FiberId(9))
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    topics.rebind(
        &ids,
        vec![Rebind {
            topic: "t".to_owned(),
            context: 2,
            token: 0,
            fiber: Some(FiberId(9)),
            budget: None,
            target: Arc::clone(&successor) as Arc<dyn EventTarget>,
        }],
    );
    assert!(
        topics.entombed(FiberId(9)).is_empty(),
        "no tombstone survives the commit"
    );
    // Skipped by a walk (still doomed by the oracle in this fixture) means
    // the row is LIVE again: use an oracle that owes nothing to prove the
    // delivery lands on the successor.
    let fresh = LocalTopics::default();
    let c = fresh.listen("t", 2, 0, Some(FiberId(9)), Arc::new(Counted::default()));
    fresh.entomb(c, consumer(), 7);
    fresh.rebind(
        &[c],
        vec![Rebind {
            topic: "t".to_owned(),
            context: 2,
            token: 0,
            fiber: Some(FiberId(9)),
            budget: None,
            target: Arc::clone(&successor) as Arc<dyn EventTarget>,
        }],
    );
    let report = fresh
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
    assert!(report.refused.is_none());
    assert_eq!(successor.0.load(Ordering::SeqCst), 1);
}

/// A tombstone lives exactly as long as the fiber owes a transition (I4):
/// withdrawal takes every tombstone of the fiber and hands back their
/// topics for the record; the next walk selects nobody, which is then the
/// truth. The tombstone's incarnation answers for the entry meanwhile.
#[tokio::test]
async fn withdrawing_a_fibers_tombstones_leaves_no_row_and_names_their_topics() {
    let topics = LocalTopics::default();
    let a = topics.listen("t", 2, 0, Some(FiberId(9)), Arc::new(Counted::default()));
    let b = topics.listen("u", 2, 1, Some(FiberId(9)), Arc::new(Counted::default()));
    topics.entomb(a, consumer(), 7);
    assert_eq!(topics.entombed_incarnation(&consumer()), Some(7));
    topics.entomb(b, consumer(), 7);
    assert_eq!(
        topics.withdraw_tombstones(FiberId(9)),
        vec!["t".to_owned(), "u".to_owned()]
    );
    assert!(topics.entombed(FiberId(9)).is_empty());
    assert_eq!(topics.entombed_incarnation(&consumer()), None);
    assert!(
        topics.withdraw_tombstones(FiberId(9)).is_empty(),
        "idempotent"
    );
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
    assert!(report.refused.is_none() && report.outputs.is_empty());
}
