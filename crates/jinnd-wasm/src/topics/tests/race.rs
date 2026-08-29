//! The hostile interleaving the card names: a swap committing around the
//! refusal check. Both directions are pinned — a walk admitted an instant
//! before the commit, and a delivery already inside a target when it
//! lands — because "never accepted-then-orphaned" is a claim about both.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use jinnd_api::{DispatchMode, FiberId, KernelFuture, LedgerEventKind};

use super::{Answer, Counted, EventTarget, RecordingSink, RestartOracle, Unserved};
use crate::peer::LedgerSink;
use crate::selector::{NoRealms, Selector};
use crate::topics::{LocalTopics, Rebind};

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
