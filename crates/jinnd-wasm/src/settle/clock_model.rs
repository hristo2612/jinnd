//! R1: model the atomic boundary update / observer snapshot interleavings.
//! The watch value lock serializes these operations in production. Loom's
//! mutex stands for that lock; the actual ActiveClock methods run unchanged.
//! Watch wake/coalescing behavior is exercised by the paused Tokio tests.

use loom::sync::{Arc, Mutex};
use loom::thread;

use super::{ActiveClock, Duration, Instant};

struct Model {
    clock: ActiveClock,
    now: Instant,
    held: [bool; 2],
    active_ticks: u64,
}

impl Model {
    // Each serialized operation is one clock tick. The independent oracle
    // counts unparked ticks, rather than accumulating boundary intervals.
    fn step(&mut self, edge: Option<(usize, bool)>) -> Duration {
        self.now += Duration::from_secs(1);
        if !self.held.iter().any(|held| *held) {
            self.active_ticks += 1;
        }
        if let Some((guard, park)) = edge {
            self.held[guard] = park;
            if park {
                self.clock.park(self.now);
            } else {
                self.clock.resume(self.now);
            }
        }
        let elapsed = self.clock.elapsed_at(self.now);
        assert_eq!(elapsed, Duration::from_secs(self.active_ticks));
        elapsed
    }
}

#[test]
fn nested_edges_and_coalesced_observations_charge_only_active_ticks() {
    loom::model(|| {
        let now = Instant::now();
        let shared = Arc::new(Mutex::new(Model {
            clock: ActiveClock::new(now),
            now,
            held: [false; 2],
            active_ticks: 0,
        }));
        let writers: Vec<_> = (0..2)
            .map(|guard| {
                let shared = shared.clone();
                thread::spawn(move || {
                    shared
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .step(Some((guard, true)));
                    shared
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .step(Some((guard, false)));
                })
            })
            .collect();
        // The observer can miss either complete park, both, or an arbitrary
        // nested prefix. Its per-call baseline must remain valid in each case.
        let baseline = shared.lock().unwrap_or_else(|e| e.into_inner()).step(None);
        let later = shared.lock().unwrap_or_else(|e| e.into_inner()).step(None);
        assert!(later >= baseline);
        for writer in writers {
            writer
                .join()
                .unwrap_or_else(|_| panic!("clock writer panicked"));
        }
        let mut model = shared.lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(model.clock.depth, 0);
        model.step(None);
    });
}
