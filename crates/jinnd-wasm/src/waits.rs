//! The kernel's WAIT GRAPH and the refusal it makes possible (M2-K10,
//! harness FINDINGS #32): who is parked on whom right now, so a crossing
//! that would close a cycle is REFUSED at the moment it is made instead of
//! parking until the guest deadline kills both ends.
//!
//! K9 closed the addressable-while-restarting window. The window under
//! this module is older and has nothing to do with restarts: a provider
//! answering a call while ALSO notifying its listeners, and an owner that
//! calls back into the provider from the notice handler. Each parks on the
//! other. A Tier A instance serves one guest entry at a time, so a fiber
//! that is parked outbound cannot answer an inbound crossing — which is
//! why a cycle in this graph is a real deadlock and not a slow call.
//!
//! Two facts from the harness govern the shape (packet card, stated before
//! round 1):
//!
//! - **`Emit` is not an escape.** The kernel awaits every listener
//!   delivery end-to-end in EVERY mode; fire-and-forget discards the
//!   answer, never the wait. So every mode records its edges here and
//!   every mode is refused on a cycle — unlike K9's refusal, which is
//!   about a target's pending transition and is scoped to the modes whose
//!   answer carries listener outputs.
//! - **Whether the loser recovers is incidental.** Nothing here relies on
//!   a restart being owed, scheduled, or landed: the graph is only about
//!   live waits.
//!
//! The refusal is TYPED and names BOTH ENDS plus the path between them
//! (R3), and it is its own next move: a cycle is not a restart and not a
//! scope error. Nothing about it is cured by waiting — the caller must
//! break the cycle (answer first and call afterwards, or stop needing the
//! answer), never retry blindly.
//!
//! Not modelled here, deliberately (R10 — the kernel stays small): the
//! kernel-driven crossings that cannot participate in a guest cycle. The
//! base host providers register with no fiber at all, so an edge to one
//! has no far end; teardown withdrawals and vitality checks are the
//! kernel's own traffic, not a plugin parking on a peer.

use std::collections::BTreeMap;
use std::sync::{Arc, MutexGuard, OnceLock};

use jinnd_api::{EntryId, FiberId};

use crate::sync::Mutex;

#[cfg(all(test, feature = "loom"))]
mod cycle_model;
#[cfg(all(test, not(feature = "loom")))]
mod tests;

/// One live wait: `waiter` is parked on `target` until the crossing named
/// by `on` settles. Edges exist only while somebody is actually parked —
/// the graph is a snapshot of now, never a history.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WaitEdge {
    pub waiter: FiberId,
    pub target: FiberId,
    /// What is being waited on: `"contract.operation"` for a contract
    /// call, the topic for a dispatch. Operator-facing prose, never parsed.
    pub on: String,
}

/// A crossing refused because it would close a cycle: both ends named, and
/// the wait path that already runs from `target` back to `waiter` (R3 —
/// the caller reads identity off the record, never out of a sentence).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Cycle {
    /// The fiber that would have parked, and the entry it serves when the
    /// assembly named one.
    pub waiter: FiberId,
    pub waiter_entry: Option<EntryId>,
    /// The fiber it would have parked on — already awaiting the waiter.
    pub target: FiberId,
    pub target_entry: Option<EntryId>,
    /// The crossing that was refused (see [`WaitEdge::on`]).
    pub on: String,
    /// The existing waits from `target` back to `waiter`, in wait order.
    /// Empty only for a self-dispatch, where the target IS the waiter.
    pub through: Vec<WaitEdge>,
}

impl Cycle {
    /// The name a report gives an end: its profile entry where the
    /// assembly supplies names, and the fiber otherwise — honest either
    /// way, never a fabricated entry id.
    #[must_use]
    pub fn waiter_name(&self) -> String {
        name(self.waiter, self.waiter_entry.as_ref())
    }

    /// The far end's name; see [`Cycle::waiter_name`].
    #[must_use]
    pub fn target_name(&self) -> String {
        name(self.target, self.target_entry.as_ref())
    }
}

fn name(fiber: FiberId, entry: Option<&EntryId>) -> String {
    entry.map_or_else(|| format!("fiber {}", fiber.0), |entry| entry.0.clone())
}

/// Maps a live fiber to the profile entry it serves, from a SNAPSHOT of
/// kernel-owned state. No guest is called and nothing blocks (R1). Unset,
/// the graph names fibers — a refusal is never delayed for want of a name.
pub trait FiberNames: Send + Sync + 'static {
    fn entry(&self, fiber: FiberId) -> Option<EntryId>;
}

#[derive(Default)]
struct Inner {
    next: u64,
    /// Keyed by ticket id so a drop removes exactly its own edge, and
    /// ordered so a reported path is stable across reads.
    edges: BTreeMap<u64, WaitEdge>,
}

/// The graph. One per assembly, shared by every surface that parks a fiber
/// on another: the broker's contract calls and the topic registry's
/// dispatch walks.
#[derive(Default)]
pub struct WaitGraph {
    inner: Mutex<Inner>,
    names: OnceLock<Arc<dyn FiberNames>>,
}

impl WaitGraph {
    /// Installs the fiber → entry naming seam. Idempotent; a second
    /// install is ignored.
    pub fn name_fibers(&self, names: Arc<dyn FiberNames>) {
        let _ = self.names.set(names);
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn entry(&self, fiber: FiberId) -> Option<EntryId> {
        self.names.get()?.entry(fiber)
    }

    /// Every live wait, in insertion order — what `jinn:introspect.waits`
    /// answers, and the evidence behind a refusal an operator is looking
    /// at after the fact.
    #[must_use]
    pub fn edges(&self) -> Vec<WaitEdge> {
        self.lock().edges.values().cloned().collect()
    }

    /// Would parking `waiter` on `target` close a cycle? Answered against
    /// the CURRENT graph alone: no speculation about edges a walk has not
    /// taken yet, so a walk is never refused for a wait it does not hold.
    ///
    /// A target that is the waiter is the degenerate cycle — a fiber
    /// cannot answer itself while it is parked on the answer.
    #[must_use]
    pub fn would_close(&self, waiter: Option<FiberId>, target: Option<FiberId>) -> bool {
        let (Some(waiter), Some(target)) = (waiter, target) else {
            return false;
        };
        self.lock().reaches(target, waiter).is_some()
    }

    /// Refuses or admits one wait. `Ok` is the ticket that HOLDS the edge:
    /// the edge lives exactly as long as it, so a settled crossing — or a
    /// failed one, or a dropped future — leaves nothing behind.
    ///
    /// An end with no fiber (a kernel-supplied host provider, an untracked
    /// peer) is never refused and records no edge: there is no far end to
    /// close a cycle through, and inventing one would refuse honest work.
    ///
    /// # Errors
    ///
    /// [`Cycle`] when `target` is, transitively, already awaiting `waiter`.
    pub fn enter(
        self: &Arc<Self>,
        waiter: Option<FiberId>,
        target: Option<FiberId>,
        on: &str,
    ) -> Result<WaitTicket, Cycle> {
        let (Some(waiter), Some(target)) = (waiter, target) else {
            return Ok(WaitTicket::inert());
        };
        let held = {
            let mut inner = self.lock();
            if let Some(through) = inner.reaches(target, waiter) {
                Err(through)
            } else {
                inner.next += 1;
                let id = inner.next;
                inner.edges.insert(
                    id,
                    WaitEdge {
                        waiter,
                        target,
                        on: on.to_owned(),
                    },
                );
                Ok(id)
            }
        };
        match held {
            Ok(id) => Ok(WaitTicket {
                graph: Some(Arc::clone(self)),
                id,
            }),
            // Naming happens OUTSIDE the graph lock: the seam is another
            // component's snapshot, and no lock of ours is ever held
            // across a call out of this module (R1).
            Err(through) => Err(Cycle {
                waiter,
                waiter_entry: self.entry(waiter),
                target,
                target_entry: self.entry(target),
                on: on.to_owned(),
                through,
            }),
        }
    }

    /// Names both ends of an already-detected cycle. Used by a walk that
    /// decided the whole dispatch up front (the topic registry) and
    /// therefore never called [`WaitGraph::enter`] for the closing edge.
    #[must_use]
    pub fn cycle(&self, waiter: FiberId, target: FiberId, on: &str) -> Cycle {
        let through = self.lock().reaches(target, waiter).unwrap_or_default();
        Cycle {
            waiter,
            waiter_entry: self.entry(waiter),
            target,
            target_entry: self.entry(target),
            on: on.to_owned(),
            through,
        }
    }

    fn release(&self, id: u64) {
        self.lock().edges.remove(&id);
    }
}

impl Inner {
    /// The wait path from `from` to `goal`, in wait order, or `None` when
    /// `from` is not waiting on `goal` at all. A breadth-first walk over a
    /// graph whose size is the number of parked crossings, under one brief
    /// lock and never across a call out (R1). `from == goal` is a path of
    /// length zero: the fiber IS the goal.
    fn reaches(&self, from: FiberId, goal: FiberId) -> Option<Vec<WaitEdge>> {
        if from == goal {
            return Some(Vec::new());
        }
        let mut seen = vec![from];
        // Per reached fiber, the path that reached it.
        let mut frontier = vec![(from, Vec::new())];
        while let Some((fiber, path)) = frontier.pop() {
            for edge in self.edges.values().filter(|edge| edge.waiter == fiber) {
                let mut next = path.clone();
                next.push(edge.clone());
                if edge.target == goal {
                    return Some(next);
                }
                if !seen.contains(&edge.target) {
                    seen.push(edge.target);
                    frontier.push((edge.target, next));
                }
            }
        }
        None
    }
}

/// The lifetime of one wait. Holding it IS being parked; dropping it —
/// however the crossing ended, including a cancelled future — retires the
/// edge. An inert ticket holds nothing (an end with no fiber).
pub struct WaitTicket {
    graph: Option<Arc<WaitGraph>>,
    id: u64,
}

impl WaitTicket {
    #[must_use]
    fn inert() -> Self {
        Self { graph: None, id: 0 }
    }
}

impl Drop for WaitTicket {
    fn drop(&mut self) {
        if let Some(graph) = &self.graph {
            graph.release(self.id);
        }
    }
}
