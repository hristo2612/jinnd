//! One refusal, four futures: a caller acts on the disposition, not on
//! the fact of being refused, so each one has to survive the trip to the
//! wire and to the ledger under its own name.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use jinnd_api::{DispatchMode, EntryId, FiberId, LedgerEventKind, Owed};

use super::{Counted, EventTarget, RecordingSink, RestartOracle, owing};
use crate::peer::LedgerSink;
use crate::selector::{NoRealms, Selector};
use crate::topics::LocalTopics;

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
