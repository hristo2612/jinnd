//! Pins for the reap-on-record seam (M2-K6 round 2; Law 2): the exit
//! within the bound, and the hostile case — pending on the record, then
//! landed by the host task.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use jinnd_api::{FiberId, LedgerEventKind};

use super::reap::reap_on_record;
use crate::peer::LedgerSink;

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
    fn kinds(&self) -> Vec<(LedgerEventKind, Option<FiberId>)> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }
}

/// A reap landing within the bound is the exit, on the record, now.
#[tokio::test]
async fn a_reap_within_the_bound_ledgers_the_exit() {
    let ledger = Arc::new(Recording::default());
    let sink = Arc::clone(&ledger) as Arc<dyn LedgerSink>;
    let code = reap_on_record(
        sink,
        3,
        Some(FiberId(7)),
        async { -9 },
        Duration::from_secs(1),
    )
    .await;
    assert_eq!(code, Some(-9));
    assert_eq!(
        ledger.kinds(),
        vec![(
            LedgerEventKind::ProcessExited {
                handle: 3,
                code: -9
            },
            Some(FiberId(7))
        )]
    );
}

/// The hostile case: a child not reaped within the bound is PENDING on
/// the record (never silent), and its exit lands from the host task —
/// the same handle, the same attribution — once the reap completes.
#[tokio::test(start_paused = true)]
async fn a_reap_past_the_bound_is_pending_on_the_record_then_lands() {
    let ledger = Arc::new(Recording::default());
    let sink = Arc::clone(&ledger) as Arc<dyn LedgerSink>;
    let (landed, reap) = tokio::sync::oneshot::channel::<i32>();
    let reap = async move { reap.await.unwrap_or(-1) };
    let code = reap_on_record(sink, 4, Some(FiberId(7)), reap, Duration::from_millis(100)).await;
    assert_eq!(code, None);
    assert_eq!(
        ledger.kinds(),
        vec![(
            LedgerEventKind::ProcessReapPending { handle: 4 },
            Some(FiberId(7))
        )]
    );
    landed
        .send(-9)
        .unwrap_or_else(|_| panic!("the reap task is gone"));
    for _ in 0..100 {
        tokio::task::yield_now().await;
        if ledger.kinds().len() == 2 {
            break;
        }
    }
    assert_eq!(
        ledger.kinds().last(),
        Some(&(
            LedgerEventKind::ProcessExited {
                handle: 4,
                code: -9
            },
            Some(FiberId(7))
        ))
    );
}
