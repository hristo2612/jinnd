//! The daemon's fiber → entry naming seam for the wait graph (M2-K10,
//! harness FINDINGS #32). The graph itself is kernel-owned and lives in
//! `jinnd-wasm`; what the daemon adds is the one thing only an assembly
//! knows — which profile entry a fiber serves — so a cycle refusal and
//! `jinn:introspect.waits` name their ends the way an operator does.
//!
//! The answer is a snapshot of kernel-owned state under a brief lock. No
//! guest is called and nothing blocks (R1); a fiber this daemon does not
//! track has no name here, which is reported as no name rather than
//! guessed.

use std::sync::Arc;

use jinnd_api::{EntryId, FiberId};
use jinnd_wasm::{FiberNames, LaneCore, WaitGraph};

use crate::support::{SharedFibers, lock};

/// Names the daemon's tracked fibers.
pub(crate) struct Names {
    fibers: SharedFibers,
}

impl Names {
    /// Builds the assembly's ONE wait graph, names its fibers, and installs
    /// it on both surfaces that park a fiber on another — the broker's
    /// contract calls and the topic registry's dispatch walks. One graph,
    /// so a cycle that closes through a call and a dispatch (the harness's
    /// own shape) is seen as the one cycle it is. The handle comes back for
    /// `jinn:introspect.waits`, so ASKING and BEING REFUSED read one source.
    pub(crate) fn install(lane: &Arc<LaneCore>, fibers: &SharedFibers) -> Arc<WaitGraph> {
        let graph = Arc::new(WaitGraph::default());
        graph.name_fibers(Arc::new(Self {
            fibers: Arc::clone(fibers),
        }));
        lane.broker.watch_waits(Arc::clone(&graph));
        lane.topics.watch_waits(Arc::clone(&graph));
        graph
    }
}

impl FiberNames for Names {
    fn entry(&self, fiber: FiberId) -> Option<EntryId> {
        lock(&self.fibers)
            .get(&fiber)
            .map(|tracked| tracked.entry.clone())
    }
}
