//! The loom model of the wait graph's admission race (M2-K10; CI runs it
//! with `--features loom --release --lib`). The production admission
//! sequence IS [`WaitGraph::enter`] — the model drives that same function,
//! so a check-then-insert that is not atomic fails here.
//!
//! The property: under EVERY interleaving of two fibers each trying to
//! park on the other, at most one edge is live. If both were admitted the
//! kernel would have written down the deadlock instead of refusing it.

use std::sync::Arc;

use jinnd_api::FiberId;
use loom::thread;

use super::WaitGraph;

const A: FiberId = FiberId(1);
const B: FiberId = FiberId(2);

/// Two fibers race to park on each other. Whichever ordering the model
/// picks, they never both hold a wait — and the refused one recorded
/// nothing, so exactly one edge is live while both tickets are held.
#[test]
fn a_cycle_is_never_admitted_by_both_halves() {
    loom::model(|| {
        let graph = Arc::new(WaitGraph::default());
        let first = {
            let graph = Arc::clone(&graph);
            thread::spawn(move || graph.enter(Some(A), Some(B), "a.b"))
        };
        let second = {
            let graph = Arc::clone(&graph);
            thread::spawn(move || graph.enter(Some(B), Some(A), "b.a"))
        };
        let held = [
            first.join().unwrap_or_else(|_| panic!("first join")),
            second.join().unwrap_or_else(|_| panic!("second join")),
        ];
        let admitted = held.iter().filter(|outcome| outcome.is_ok()).count();
        assert!(
            admitted <= 1,
            "at most one half of a cycle is ever admitted, got {admitted}"
        );
        assert_eq!(
            graph.edges().len(),
            admitted,
            "an admitted wait is exactly one edge; a refused one is none"
        );
        drop(held);
        assert!(
            graph.edges().is_empty(),
            "and every ticket retired its edge"
        );
    });
}

/// An honest pair — both fibers parking on a third — is admitted under
/// every interleaving. The gate refuses cycles, never contention.
#[test]
fn converging_waits_are_admitted_under_every_interleaving() {
    loom::model(|| {
        let graph = Arc::new(WaitGraph::default());
        let target = FiberId(3);
        let first = {
            let graph = Arc::clone(&graph);
            thread::spawn(move || graph.enter(Some(A), Some(target), "a.c").is_ok())
        };
        let second = {
            let graph = Arc::clone(&graph);
            thread::spawn(move || graph.enter(Some(B), Some(target), "b.c").is_ok())
        };
        assert!(first.join().unwrap_or_else(|_| panic!("first join")));
        assert!(second.join().unwrap_or_else(|_| panic!("second join")));
        assert!(graph.edges().is_empty(), "both tickets retired their edges");
    });
}
