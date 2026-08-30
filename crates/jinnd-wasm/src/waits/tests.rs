//! Wait-graph unit tests (crate lane, M2-K10): what the graph refuses,
//! what it must NOT refuse, and that a refusal costs nothing but a
//! refusal. The hostile probes the packet card names live here — three-fiber
//! cycles, a self-dispatch, refusal storms — because they are properties of
//! the graph rather than of any one surface over it.

use std::sync::Arc;

use jinnd_api::{EntryId, FiberId};

use super::{FiberNames, WaitGraph};

const A: FiberId = FiberId(1);
const B: FiberId = FiberId(2);
const C: FiberId = FiberId(3);

fn graph() -> Arc<WaitGraph> {
    Arc::new(WaitGraph::default())
}

/// Names every fiber `entry-<n>`.
struct Named;

impl FiberNames for Named {
    fn entry(&self, fiber: FiberId) -> Option<EntryId> {
        Some(EntryId(format!("entry-{}", fiber.0)))
    }
}

/// The packet's shape at its smallest: A is parked on B, so B parking on
/// A is refused — named at both ends, with the wait that makes it a cycle.
#[test]
fn a_wait_that_would_close_a_cycle_is_refused() {
    let graph = graph();
    let _held = graph
        .enter(Some(A), Some(B), "jinn:test/settings-changed")
        .unwrap_or_else(|cycle| panic!("the first edge is not a cycle: {cycle:?}"));
    let refused = graph
        .enter(Some(B), Some(A), "jinn:test/settings.get")
        .err()
        .unwrap_or_else(|| panic!("the closing edge must refuse"));
    assert_eq!(refused.waiter, B);
    assert_eq!(refused.target, A);
    assert_eq!(refused.on, "jinn:test/settings.get");
    assert_eq!(
        refused.through,
        vec![super::WaitEdge {
            waiter: A,
            target: B,
            on: "jinn:test/settings-changed".to_owned(),
        }],
        "the record carries the wait that closes it, not a sentence"
    );
    // Refusing added nothing: the graph still holds exactly the one wait.
    assert_eq!(graph.edges().len(), 1);
}

/// The card's hostile probe: a cycle that closes through a third fiber is
/// the same defect and refuses the same way.
#[test]
fn a_three_fiber_cycle_refuses_at_the_closing_edge() {
    let graph = graph();
    let _first = graph.enter(Some(A), Some(B), "a.b");
    let _second = graph.enter(Some(B), Some(C), "b.c");
    let refused = graph
        .enter(Some(C), Some(A), "c.a")
        .err()
        .unwrap_or_else(|| panic!("C parking on A closes A→B→C"));
    assert_eq!(
        refused
            .through
            .iter()
            .map(|edge| (edge.waiter, edge.target))
            .collect::<Vec<_>>(),
        vec![(A, B), (B, C)],
        "the path names every hop back to the waiter: {refused:?}"
    );
}

/// A fiber cannot answer itself while it is parked on the answer: the
/// degenerate cycle is a cycle (card probe: a self-dispatch).
#[test]
fn a_self_dispatch_is_the_degenerate_cycle() {
    let graph = graph();
    let refused = graph
        .enter(Some(A), Some(A), "jinn:test/topic")
        .err()
        .unwrap_or_else(|| panic!("a self-dispatch closes on itself"));
    assert_eq!((refused.waiter, refused.target), (A, A));
    assert!(
        refused.through.is_empty(),
        "there is no hop between a fiber and itself: {refused:?}"
    );
    assert!(graph.edges().is_empty(), "and nothing was recorded");
}

/// The graph is a snapshot of NOW: a settled crossing is not a wait, so
/// the edge it held is gone and the reverse call is ordinary work.
#[test]
fn a_settled_wait_leaves_no_edge() {
    let graph = graph();
    {
        let _held = graph.enter(Some(A), Some(B), "a.b");
        assert!(graph.would_close(Some(B), Some(A)));
    }
    assert!(graph.edges().is_empty());
    assert!(
        !graph.would_close(Some(B), Some(A)),
        "B may park on A once A is no longer parked on B"
    );
}

/// A refusal storm changes nothing: refusals are cheap, leak no edges, and
/// never poison the graph for the crossings that are fine (card probe).
#[test]
fn a_refusal_storm_leaks_nothing() {
    let graph = graph();
    let _held = graph.enter(Some(A), Some(B), "a.b");
    for _ in 0..1_000u32 {
        assert!(graph.enter(Some(B), Some(A), "b.a").is_err());
    }
    assert_eq!(graph.edges().len(), 1, "still exactly the one live wait");
    let _fine = graph
        .enter(Some(B), Some(C), "b.c")
        .unwrap_or_else(|cycle| panic!("an honest wait still admits: {cycle:?}"));
    assert_eq!(graph.edges().len(), 2);
}

/// Waits that share a target are not a cycle. Refusing them would refuse
/// the ordinary composition this packet exists to protect.
#[test]
fn converging_waits_are_not_a_cycle() {
    let graph = graph();
    let _first = graph
        .enter(Some(A), Some(C), "a.c")
        .unwrap_or_else(|cycle| panic!("{cycle:?}"));
    let _second = graph
        .enter(Some(B), Some(C), "b.c")
        .unwrap_or_else(|cycle| panic!("{cycle:?}"));
    assert!(!graph.would_close(Some(A), Some(B)));
    assert_eq!(graph.edges().len(), 2);
}

/// An end the assembly cannot identify — a kernel-supplied host provider,
/// an untracked peer — is never refused and records nothing: there is no
/// far end for a cycle to close through.
#[test]
fn an_end_without_a_fiber_is_never_refused() {
    let graph = graph();
    let _held = graph.enter(Some(A), Some(B), "a.b");
    assert!(graph.enter(None, Some(A), "host.read").is_ok());
    assert!(graph.enter(Some(B), None, "jinn:fs.read").is_ok());
    assert!(!graph.would_close(None, Some(A)));
    assert_eq!(graph.edges().len(), 1, "neither recorded an edge");
}

/// Names come from the seam when the assembly supplies one, and fall back
/// to the fiber when it does not — a refusal is never delayed or faked for
/// want of a name.
#[test]
fn both_ends_are_named_from_the_seam() {
    let graph = graph();
    graph.name_fibers(Arc::new(Named));
    let _held = graph.enter(Some(A), Some(B), "a.b");
    let refused = graph
        .enter(Some(B), Some(A), "b.a")
        .err()
        .unwrap_or_else(|| panic!("cycle"));
    assert_eq!(refused.waiter_entry, Some(EntryId("entry-2".to_owned())));
    assert_eq!(refused.target_entry, Some(EntryId("entry-1".to_owned())));
    assert_eq!(refused.waiter_name(), "entry-2");

    let unnamed = Arc::new(WaitGraph::default());
    let _also = unnamed.enter(Some(A), Some(B), "a.b");
    let bare = unnamed
        .enter(Some(B), Some(A), "b.a")
        .err()
        .unwrap_or_else(|| panic!("cycle"));
    assert_eq!(bare.waiter_name(), "fiber 2");
    assert_eq!(bare.target_name(), "fiber 1");
}

/// What `jinn:introspect.waits` reads: every live wait, with what each end
/// is waiting on.
#[test]
fn edges_report_every_live_wait() {
    let graph = graph();
    let _first = graph.enter(Some(A), Some(B), "a.b");
    let _second = graph.enter(Some(B), Some(C), "b.c");
    let edges = graph.edges();
    assert_eq!(
        edges
            .iter()
            .map(|edge| (edge.waiter, edge.target, edge.on.clone()))
            .collect::<Vec<_>>(),
        vec![(A, B, "a.b".to_owned()), (B, C, "b.c".to_owned()),]
    );
}

/// A cycle named after the fact — the topic registry decides a whole walk
/// before it dispatches, so it never enters the closing edge — reports the
/// same ends and the same path as entering would have.
#[test]
fn a_named_cycle_matches_the_refused_one() {
    let graph = graph();
    let _held = graph.enter(Some(A), Some(B), "a.b");
    let named = graph.cycle(B, A, "b.a");
    let entered = graph
        .enter(Some(B), Some(A), "b.a")
        .err()
        .unwrap_or_else(|| panic!("cycle"));
    assert_eq!(named, entered);
}
