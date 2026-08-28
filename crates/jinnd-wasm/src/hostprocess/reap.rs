//! The reap on the record after a kill the kernel itself delivered (M2-K6
//! round 2; Law 2): a kill without its exit is half a story, so the reap is
//! awaited under a bound inside the guest deadline; past it, the child is
//! `ProcessReapPending` — never silent — and the host task lands the exit.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use jinnd_api::{FiberId, LedgerEventKind};

use crate::peer::LedgerSink;

/// How long a timed-out `run` waits for its SIGKILLed child to be reaped
/// before recording the reap as pending: `RUN_CAP + RUN_REAP_CAP` stays
/// inside the guest deadline (`lane::DEADLINE`; R1).
pub(super) const RUN_REAP_CAP: Duration = Duration::from_millis(500);

/// Awaits `reap` (an already-signalled child's exit code) for at most
/// `bound`; the exit is ledgered either way — now, or by the detached host
/// task after `ProcessReapPending`. Answers the code when it landed in time.
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
    let mut reap = Box::pin(reap);
    match tokio::time::timeout(bound, &mut reap).await {
        Ok(code) => {
            sink.append(LedgerEventKind::ProcessExited { handle, code }, fiber);
            Some(code)
        }
        Err(_) => {
            sink.append(LedgerEventKind::ProcessReapPending { handle }, fiber);
            drop(tokio::spawn(async move {
                let code = reap.await;
                sink.append(LedgerEventKind::ProcessExited { handle, code }, fiber);
            }));
            None
        }
    }
}
