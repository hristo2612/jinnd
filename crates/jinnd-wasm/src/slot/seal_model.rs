//! The loom model of the seal gate's drain/seal interleavings (M2-K5 #16;
//! CI runs it with `--features loom --release --lib`). The production
//! closing sequence IS [`SealGate::close`] — the model drives that same
//! function, so reordering the seal ahead of the drain fails here.

use std::pin::pin;
use std::task::{Context, Waker};

use loom::sync::Arc;
use loom::sync::atomic::{AtomicUsize, Ordering};
use loom::thread;

use super::gate::SealGate;

/// Drives the closing sequence to completion inside a model thread: the
/// drain it awaits is a synchronous join, so the future never parks.
fn drive(close: impl Future<Output = ()>) {
    let mut close = pin!(close);
    let mut context = Context::from_waker(Waker::noop());
    while close.as_mut().poll(&mut context).is_pending() {}
}

/// Under every interleaving of a handler making N registrations against a
/// closing seat, the drained handler lands all N or — only if it began
/// after the seal — none: never a prefix.
#[test]
fn a_drained_handler_lands_all_or_none() {
    loom::model(|| {
        let gate = Arc::new(SealGate::default());
        let landed = Arc::new(AtomicUsize::new(0));
        let refused = Arc::new(AtomicUsize::new(0));
        let handler = {
            let (gate, landed, refused) =
                (Arc::clone(&gate), Arc::clone(&landed), Arc::clone(&refused));
            thread::spawn(move || {
                for _ in 0..2 {
                    if gate.sealed() {
                        refused.fetch_add(1, Ordering::SeqCst);
                    } else {
                        landed.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
        };
        let closer = {
            let gate = Arc::clone(&gate);
            thread::spawn(move || {
                drive(gate.close(async move {
                    handler.join().unwrap_or_else(|_| panic!("handler join"));
                }));
            })
        };
        closer.join().unwrap_or_else(|_| panic!("closer join"));
        assert_eq!(landed.load(Ordering::SeqCst), 2, "all N landed");
        assert_eq!(refused.load(Ordering::SeqCst), 0, "never a prefix");
        assert!(gate.closing() && gate.sealed());
    });
}

/// An entry arriving at the door after the close began refuses whatever
/// the drain is doing: closing is visible before the seal, never after.
#[test]
fn the_door_shuts_before_the_journal() {
    loom::model(|| {
        let gate = Arc::new(SealGate::default());
        let late = {
            let gate = Arc::clone(&gate);
            thread::spawn(move || (gate.sealed(), gate.closing()))
        };
        drive(gate.close(async {}));
        let (sealed, closing) = late.join().unwrap_or_else(|_| panic!("late join"));
        assert!(!sealed || closing, "a sealed journal implies a shut door");
    });
}
