//! The kernel's own side of the byte lane (M2-K13; harness #40/#41): the
//! reserved-topic vocabulary, and the publish that delivers on one. Split
//! from `topics.rs` by responsibility (R10 file hygiene) — a reserved topic
//! shares the registry's listener table and nothing else of its dispatch:
//! no selector, no mode, no reply, no wait edge, no refusal.

use std::num::NonZeroU64;
use std::sync::Arc;

use jinnd_api::{DispatchMode, LedgerEventKind};

use crate::peer::LedgerSink;

use super::{EventTarget, Listener, LocalTopics};

/// The kernel's RESERVED lifecycle topic (M2-K13; harness #40/#41): the
/// kernel publishes every fiber transition it commits here, and nothing
/// else ever writes to it. A guest `emit` on it is refused — a witnessed
/// transition must never be confusable with a forged one — and a
/// subscription is gated by the [`crate::INTROSPECT_CONTRACT`] grant,
/// because the delivered payload is bounded by exactly what that
/// contract's own pull already admits.
pub const TRANSITIONS_TOPIC: &str = "jinn:introspect/transitions";

/// How many published payloads may wait on ONE listener before the kernel
/// drops and counts for that listener alone. The bound is per lane, not
/// per topic, because the whole point of the lane is that a listener's
/// slowness is charged to the listener: a shared bound would let one
/// dawdler spend every other listener's headroom.
const LANE_CAPACITY: usize = 256;

/// Whether `topic` is a kernel-reserved publish topic: only the kernel may
/// [`LocalTopics::publish`] on one, and a guest `emit` is refused (M2-K13).
#[must_use]
pub fn reserved(topic: &str) -> bool {
    topic == TRANSITIONS_TOPIC
}

/// The contract grant a subscription to `topic` requires (M2-K13). Every
/// topic is its own grant name (constitution 01 §Grants) except the
/// kernel's reserved ones, which belong to the contract whose authority
/// bounds their payload.
#[must_use]
pub fn grant_for(topic: &str) -> &str {
    if reserved(topic) {
        crate::grants::INTROSPECT_CONTRACT
    } else {
        topic
    }
}

/// ONE LISTENER'S PRIVATE DELIVERY LANE (M2-K13 round 3): a bounded queue
/// and a task of its own, opened on that listener's first publish and
/// dropped with its registration.
///
/// This is what makes sibling isolation structural rather than careful
/// (R11). Round 1 walked listeners sequentially and a slow one held a quick
/// sibling for its whole handler; round 2 made the deliveries of ONE
/// publish concurrent, and the stall reappeared BETWEEN publishes because
/// the publish still joined before the next could start. A lane removes the
/// join entirely: nowhere on the publish path is one listener's progress a
/// term in another's.
pub(super) struct Lane {
    inbox: tokio::sync::mpsc::Sender<Vec<u8>>,
}

type Sink = Option<Arc<dyn LedgerSink>>;

impl Lane {
    /// Opens a listener's lane on the current runtime. Without one there is
    /// no lane and every offer to it is REFUSED and counted — the kernel
    /// says it could not deliver rather than buffering into a silence.
    fn open(
        target: &Arc<dyn EventTarget>,
        token: u64,
        topic: &str,
        budget: Option<NonZeroU64>,
        sink: Sink,
    ) -> Option<Self> {
        let runtime = tokio::runtime::Handle::try_current().ok()?;
        let (inbox, mut queue) = tokio::sync::mpsc::channel::<Vec<u8>>(LANE_CAPACITY);
        let target = Arc::clone(target);
        let topic = topic.to_owned();
        runtime.spawn(async move {
            while let Some(payload) = queue.recv().await {
                // Each delivery runs on a task of its OWN, so a listener
                // that TRAPS loses that one transition and neither its own
                // lane nor a sibling's (R9, R11). Serial WITHIN the lane:
                // isolation across listeners is never reordering inside
                // one, so a listener sees transitions in commit order.
                //
                // THE CALL IS INSIDE THE TASK, not its argument: `deliver`
                // is the listener's own code and runs on whatever stack
                // invokes it, so building the future out here would raise
                // a synchronous trap on the LANE'S stack — outside the
                // containment meant to hold it — and kill the loop. A dead
                // loop then loses every later transition with no count and
                // no ledger row, which is the absence this packet exists
                // to refuse (R11, Law 2).
                let listener = Arc::clone(&target);
                let subject = topic.clone();
                let settled = tokio::spawn(async move {
                    listener.deliver(token, &subject, payload, budget).await
                })
                .await;
                if let Some(sink) = &sink {
                    sink.append(
                        LedgerEventKind::DispatchTrace {
                            topic: topic.clone(),
                            mode: DispatchMode::Emit,
                            listeners: 1,
                            failures: u32::from(!matches!(settled, Ok(Ok(_)))),
                            emitter: 0,
                        },
                        None,
                    );
                }
            }
        });
        Some(Self { inbox })
    }
}

impl LocalTopics {
    /// Publishes one KERNEL-ORIGIN payload to every listener of a reserved
    /// topic (M2-K13). This is not an `emit` and deliberately shares none
    /// of its decisions: there is no emitter context, no selector, no
    /// reply, and no wait edge — the kernel is not a fiber, so a publish
    /// can neither be waited on nor close a cycle (M2-K10), and no
    /// reply-expecting refusal applies (M2-K9).
    ///
    /// **IT DOES NOT JOIN, AND CANNOT (R11):** it is not `async`, so no
    /// listener can delay the kernel, a sibling, or a sibling's NEXT
    /// transition. It hands each listener's [`Lane`] the payload and
    /// returns; delivery, per-listener ordering, trap containment and the
    /// `DispatchTrace` belong to the lane.
    ///
    /// **A listener that cannot take it LOSES IT LOUDLY.** A lane past
    /// [`LANE_CAPACITY`] — or a listener with no runtime to open one on —
    /// refuses the payload, and the refusals land as one typed
    /// `PublishDropped` row (Law 2, R9). The kernel never grows a queue on
    /// a listener's behalf and never waits for room.
    ///
    /// Answers how many listeners ACCEPTED the payload.
    pub fn publish(&self, topic: &str, payload: &[u8]) -> usize {
        let sink = self.sink.clone();
        let (reached, refused) = {
            let mut inner = self.lock();
            let mut reached = 0usize;
            let mut refused = 0u64;
            // No plugin code runs under this lock and nothing is awaited
            // under it (R1): opening a lane spawns, and offering to one is
            // a non-blocking bounded push.
            let selected = inner
                .listeners
                .iter_mut()
                .filter(|listener| listener.topic == topic);
            for Listener {
                lane,
                target,
                token,
                budget,
                ..
            } in selected
            {
                let opened = match lane {
                    Some(lane) => Some(&*lane),
                    None => Lane::open(target, *token, topic, *budget, sink.clone())
                        .map(|fresh| &*lane.insert(fresh)),
                };
                match opened {
                    Some(lane) if lane.inbox.try_send(payload.to_vec()).is_ok() => reached += 1,
                    _ => refused += 1,
                }
            }
            (reached, refused)
        };
        if refused > 0
            && let Some(sink) = &self.sink
        {
            sink.append(
                LedgerEventKind::PublishDropped {
                    topic: topic.to_owned(),
                    dropped: refused,
                },
                None,
            );
        }
        reached
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests;
