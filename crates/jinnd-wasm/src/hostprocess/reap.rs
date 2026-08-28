//! The reap on the record after a kill the kernel itself delivered (M2-K6
//! round 2; Law 2): a kill without its exit is a ledger telling half a
//! story, so the reap is awaited under a bound inside the guest deadline;
//! a child that outlives the bound is recorded as `ProcessReapPending` —
//! never silent — and the host task finishes the reap, appending the exit
//! when it lands.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use jinnd_api::{FiberId, LedgerEventKind};

use crate::peer::LedgerSink;

/// How long a timed-out `run` waits for its SIGKILLed child to be reaped
/// before recording the reap as pending: `RUN_CAP + RUN_REAP_CAP` stays
/// inside the guest deadline (`lane::DEADLINE`; R1).
pub(super) const RUN_REAP_CAP: Duration = Duration::from_millis(500);

/// Awaits `reap` (the exit code of an already-signalled child) for at most
/// `bound`. The exit is ledgered either way — now, or by the detached host
/// task once it lands, with `ProcessReapPending` on the record meanwhile.
/// Answers the code when it landed within the bound.
pub(super) async fn reap_on_record<F>(
    sink: Arc<dyn LedgerSink>,
    handle: u64,
    fiber: Option<FiberId>,
    reap: F,
    bound: Duration,
) -> Option<i32>
where
    F: Future<Output = i32> + Send + 'static,
{
    // Round-1 shape, kept red: the reap is delegated and nothing is recorded.
    let _ = (
        sink,
        handle,
        fiber,
        bound,
        LedgerEventKind::ProcessReapPending { handle },
    );
    drop(tokio::spawn(reap));
    None
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use jinnd_api::{FiberId, LedgerEventKind};

    use super::reap_on_record;
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
        let code =
            reap_on_record(sink, 4, Some(FiberId(7)), reap, Duration::from_millis(100)).await;
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
}
