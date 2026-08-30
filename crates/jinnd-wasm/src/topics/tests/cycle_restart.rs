//! The intersection the M2-K10 card named as a probe and the round-1 COO
//! ruling asked for by name: a cycle that forms CONCURRENTLY WITH A
//! RESTART. K9 refuses a reply-expecting walk into an incarnation the
//! kernel is already taking down; K10 refuses a walk that would close a
//! wait cycle. Both conditions can be true of the same walk, and of the
//! same target, at the same moment.
//!
//! Nothing here is a race. The two refusals meet at a fixed point in
//! `emit`, so which one answers is settled BY CONSTRUCTION and can be
//! pinned deterministically: the graph and the oracle are both seeded
//! before the walk starts, and the walk reads them in one order. That is
//! the whole reason this is testable rather than a named limit.
//!
//! Why the order is the one it is: a restart refusal invites the caller
//! back — the incarnation it names is on its way. A cycle refusal does
//! not; nothing about waiting cures it. Answering `Restarting` to a
//! caller whose real condition is a deadlock sends it into exactly the
//! retry-against-an-unchanged-environment loop R9 keeps dead. So when
//! both hold, the caller is told the one that is true for good.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use jinnd_api::{DispatchMode, EntryId, FiberId, KernelFuture, LedgerEventKind, Owed};
use tokio::sync::Notify;

use super::{Counted, EventTarget, RecordingSink, RestartOracle, doomed};
use crate::peer::LedgerSink;
use crate::selector::{NoRealms, Selector};
use crate::topics::LocalTopics;
use crate::waits::WaitGraph;

/// The emitter, and the peer that is both mid-restart and already parked
/// on the emitter — one fiber wearing both conditions at once.
const EMITTER: FiberId = FiberId(4);
const OWING: FiberId = FiberId(9);

/// A registry wired to BOTH seams, plus the graph so a case can seed the
/// wait that makes the next walk a cycle.
fn wired(sink: &Arc<RecordingSink>) -> (LocalTopics, Arc<WaitGraph>) {
    let topics = LocalTopics::traced(Arc::clone(sink) as Arc<dyn LedgerSink>);
    let graph = Arc::new(WaitGraph::default());
    topics.watch_waits(Arc::clone(&graph));
    topics.watch_restarts(doomed(OWING) as Arc<dyn RestartOracle>);
    (topics, graph)
}

/// A delivery that never answers, announcing that it was entered. Stands
/// for the guest that is parked when its fiber is taken from under it.
struct Parking(Arc<Notify>);

impl EventTarget for Parking {
    fn deliver(&self, _: u64, _: &str, _: Vec<u8>) -> KernelFuture<'static, Vec<u8>> {
        let entered = Arc::clone(&self.0);
        Box::pin(async move {
            entered.notify_one();
            std::future::pending::<()>().await;
            Ok(Vec::new())
        })
    }
}

/// Both conditions on the SAME target: the listener owes a reload AND is
/// already awaiting the emitter. The walk refuses as a CYCLE — in every
/// mode, including the fire-and-forget one K9 does not decide at all —
/// because the cycle is the condition waiting cannot cure. Nothing is
/// delivered and the ledger keeps the cycle, not the restart.
#[tokio::test]
async fn a_cycle_forming_around_a_restarting_peer_refuses_as_a_cycle() {
    for mode in [
        DispatchMode::Emit,
        DispatchMode::Serial,
        DispatchMode::Parallel,
        DispatchMode::Bail,
        DispatchMode::Waterfall,
    ] {
        let sink = Arc::new(RecordingSink::default());
        let (topics, graph) = wired(&sink);
        // The peer is already parked on the emitter: an ordinary call it
        // made before the restart was decided, still in flight.
        let _held = graph
            .enter(Some(OWING), Some(EMITTER), "jinn:test/settings.get")
            .unwrap_or_else(|cycle| panic!("the first wait is not itself a cycle: {cycle:?}"));
        let listener = Arc::new(Counted::default());
        topics.listen(
            "t",
            1,
            0,
            Some(OWING),
            Arc::clone(&listener) as Arc<dyn EventTarget>,
        );

        let report = topics
            .emit(
                7,
                "t",
                mode,
                &Selector::All,
                Vec::new(),
                Some(EMITTER),
                &NoRealms,
            )
            .await;

        let cycle = report
            .cycle
            .clone()
            .unwrap_or_else(|| panic!("{mode:?} refuses as a cycle: {report:?}"));
        assert_eq!(cycle.waiter, EMITTER, "{mode:?}: the end that would park");
        assert_eq!(cycle.target, OWING, "{mode:?}: the end already awaiting it");
        assert_eq!(cycle.on, "t", "{mode:?}: the crossing refused");
        assert_eq!(
            cycle.through.len(),
            1,
            "{mode:?}: and the hop back that makes it a cycle: {:?}",
            cycle.through
        );
        // The restart is real and the walk still does not report it: a
        // caller told `Restarting` here would retry into the deadlock.
        assert!(
            report.refused.is_none(),
            "{mode:?}: not answered as a restart: {report:?}"
        );
        assert_eq!(
            listener.0.load(Ordering::SeqCst),
            0,
            "{mode:?}: nothing was delivered"
        );
        let kinds: Vec<LedgerEventKind> =
            sink.recorded().into_iter().map(|(kind, _)| kind).collect();
        assert!(
            kinds
                .iter()
                .any(|kind| matches!(kind, LedgerEventKind::CycleRefused { .. })),
            "{mode:?}: the cycle is the row: {kinds:?}"
        );
        assert!(
            !kinds
                .iter()
                .any(|kind| matches!(kind, LedgerEventKind::DispatchRefused { .. })),
            "{mode:?}: and it is told apart from a restart by KIND, not prose: {kinds:?}"
        );
        assert!(
            !kinds
                .iter()
                .any(|kind| matches!(kind, LedgerEventKind::DispatchTrace { .. })),
            "{mode:?}: a refused walk traces nothing: {kinds:?}"
        );
    }
}

/// The two conditions on DIFFERENT targets of one walk, which is the
/// shape a restart sweep actually makes: one listener mid-restart, a
/// second listener already awaiting the emitter. The walk is refused
/// whole and as a cycle, neither listener is entered, and the edge the
/// walk managed to take before it hit the cycle is given back — an
/// all-or-nothing refusal that leaks no wait (R5).
#[tokio::test]
async fn one_restarting_listener_and_one_cycling_listener_refuse_the_walk_whole() {
    let sink = Arc::new(RecordingSink::default());
    let (topics, graph) = wired(&sink);
    let _held = graph
        .enter(Some(FiberId(11)), Some(EMITTER), "jinn:test/settings.get")
        .unwrap_or_else(|cycle| panic!("{cycle:?}"));
    let restarting = Arc::new(Counted::default());
    let cycling = Arc::new(Counted::default());
    // Selected in this order: the restarting one is reached FIRST, so a
    // walk that decided restarts first would answer `Restarting` here.
    topics.listen(
        "t",
        1,
        0,
        Some(OWING),
        Arc::clone(&restarting) as Arc<dyn EventTarget>,
    );
    topics.listen(
        "t",
        2,
        0,
        Some(FiberId(11)),
        Arc::clone(&cycling) as Arc<dyn EventTarget>,
    );

    let report = topics
        .emit(
            7,
            "t",
            DispatchMode::Serial,
            &Selector::All,
            Vec::new(),
            Some(EMITTER),
            &NoRealms,
        )
        .await;

    let cycle = report
        .cycle
        .clone()
        .unwrap_or_else(|| panic!("the cycle answers, not the restart: {report:?}"));
    assert_eq!(
        cycle.target,
        FiberId(11),
        "the end already awaiting: {cycle:?}"
    );
    assert!(
        report.refused.is_none(),
        "not a restart refusal: {report:?}"
    );
    assert_eq!(restarting.0.load(Ordering::SeqCst), 0, "never half-landed");
    assert_eq!(cycling.0.load(Ordering::SeqCst), 0, "never half-landed");
    // The wait taken for the first listener before the second refused is
    // released: only the pre-existing edge is left standing.
    assert_eq!(
        graph.edges().len(),
        1,
        "a refused walk gives back every edge it took: {:?}",
        graph.edges()
    );
}

/// The other half of the precedence, so the cycle check cannot be said to
/// have swallowed K9: with the SAME registry and the same restarting
/// peer, a walk that closes no cycle still refuses as a restart, typed
/// and ledgered exactly as before.
#[tokio::test]
async fn a_restart_still_refuses_as_a_restart_when_no_cycle_is_present() {
    let sink = Arc::new(RecordingSink::default());
    let (topics, graph) = wired(&sink);
    let listener = Arc::new(Counted::default());
    topics.listen(
        "t",
        1,
        0,
        Some(OWING),
        Arc::clone(&listener) as Arc<dyn EventTarget>,
    );

    let report = topics
        .emit(
            7,
            "t",
            DispatchMode::Serial,
            &Selector::All,
            Vec::new(),
            Some(EMITTER),
            &NoRealms,
        )
        .await;

    assert!(report.cycle.is_none(), "no cycle exists: {report:?}");
    let refused = report
        .refused
        .clone()
        .unwrap_or_else(|| panic!("the restart still refuses: {report:?}"));
    assert_eq!(refused.entry, EntryId("consumer".to_owned()));
    assert_eq!(refused.owed, Owed::Reload);
    assert_eq!(listener.0.load(Ordering::SeqCst), 0);
    assert!(
        graph.edges().is_empty(),
        "and the refused walk kept no wait: {:?}",
        graph.edges()
    );
}

/// The false-positive direction, which is the dangerous one: a walk
/// ABANDONED mid-delivery — the shape of a fiber taken down for its
/// restart while it is parked on a peer — must give its wait back. If it
/// did not, the peer's next honest crossing would be refused for a wait
/// nobody holds, and a cycle refusal is only ever allowed on positive
/// proof.
#[tokio::test]
async fn a_wait_abandoned_by_a_teardown_frees_its_peer() {
    let sink = Arc::new(RecordingSink::default());
    let (topics, graph) = wired(&sink);
    let entered = Arc::new(Notify::new());
    topics.listen(
        "t",
        1,
        0,
        Some(FiberId(11)),
        Arc::new(Parking(Arc::clone(&entered))) as Arc<dyn EventTarget>,
    );

    {
        let walk = topics.emit(
            7,
            "t",
            DispatchMode::Serial,
            &Selector::All,
            Vec::new(),
            Some(EMITTER),
            &NoRealms,
        );
        tokio::pin!(walk);
        tokio::select! {
            _ = &mut walk => panic!("the parked delivery never answers on its own"),
            () = entered.notified() => {}
        }
        assert_eq!(
            graph.edges().len(),
            1,
            "the wait is live while the walk is: {:?}",
            graph.edges()
        );
        // The walk is dropped here — the fiber went away under it.
    }

    assert!(
        graph.edges().is_empty(),
        "an abandoned walk leaves no phantom wait: {:?}",
        graph.edges()
    );
    // And the peer is free: the crossing that WOULD have closed the
    // abandoned cycle is now admitted, because no cycle remains to prove.
    graph
        .enter(Some(FiberId(11)), Some(EMITTER), "jinn:test/settings.get")
        .unwrap_or_else(|cycle| panic!("the freed peer is not refused: {cycle:?}"));
}
