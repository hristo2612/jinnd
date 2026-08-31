//! The kernel's lifecycle publish (M2-K13; harness findings #40 and #41).
//!
//! The kernel already COMMITS every fiber transition to the ledger. This is
//! the missing half: it also PUBLISHES them, on the reserved topic
//! [`TRANSITIONS_TOPIC`], to the listeners a `jinn:introspect` grant admits.
//! A catalog can then emit what it witnessed instead of what it inferred by
//! diffing two snapshots — including the transient readings (`unloading`,
//! `pending`, `loading`) that no poller at this pin can reach (#41).
//!
//! Three properties this module exists to hold, each decided on evidence
//! rather than taste (packet card):
//!
//! 1. **Ordering.** A delivery may never precede its ledger row. The
//!    committing side appends on the ordered unreceipted lane and hands the
//!    transition here; the publisher then reads the ledger's high-water mark
//!    THROUGH the single writer before it delivers anything, which returns
//!    only once every append sent before it has committed. `committed-by`
//!    carries that mark, so a listener can check the guarantee itself rather
//!    than take it on trust (Law 2).
//! 2. **Back-pressure.** The kernel never waits on a listener: the hand-off
//!    is a bounded push that cannot block, and delivery happens on this
//!    task. A listener slow enough to fill [`CAPACITY`] loses transitions —
//!    and the loss is COUNTED, as a `PublishDropped` ledger row and as a gap
//!    in the listener's own `ordinal`. An uncounted drop would be the
//!    absence class returning (R9).
//! 3. **Late join and replay.** There is NO replay. A listener that mounts
//!    mid-life is told so plainly: `ordinal` is the kernel's own count of
//!    transitions published since this process started, so a first delivery
//!    above 1 names exactly how many the listener missed, and a listener
//!    holding `jinn:ledger` recovers them from the stream itself.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use jinnd_api::{EntryId, FiberState, LedgerEventKind, Transition};
use jinnd_ledger::Ledger;
use jinnd_wasm::{LaneCore, TRANSITIONS_TOPIC};

use crate::support::lock;

/// How many committed transitions may wait on a slow listener before the
/// kernel starts dropping and counting. A bound, not a guess at goodness:
/// the alternative to a bound is either an unbounded queue (the kernel's
/// memory hostage to a listener) or a blocking hand-off (a new deadlock
/// surface, on top of the unretired one at #4/#32). Losing loudly is the
/// only remaining honest answer.
const CAPACITY: usize = 256;

/// One transition waiting to be published, with the entry attribution and
/// the incarnation the kernel saw when it committed.
struct Queued {
    entry: EntryId,
    incarnation: Option<u64>,
    transition: Transition,
    ordinal: u64,
}

#[derive(Default)]
struct Pending {
    queue: VecDeque<Queued>,
    /// Every transition ever OFFERED — the ordinal source. It counts what
    /// was dropped too, which is what makes a drop visible to a listener.
    published: u64,
    /// Offered but not enqueued, not yet reported.
    dropped: u64,
}

/// The publisher: one bounded queue and one task, per assembly.
pub(crate) struct Lifecycle {
    pending: Mutex<Pending>,
    wake: tokio::sync::Notify,
    started: AtomicBool,
    ledger: Ledger,
    lane: Arc<LaneCore>,
}

impl Lifecycle {
    pub(crate) fn new(ledger: Ledger, lane: Arc<LaneCore>) -> Arc<Self> {
        Arc::new(Self {
            pending: Mutex::new(Pending::default()),
            wake: tokio::sync::Notify::new(),
            started: AtomicBool::new(false),
            ledger,
            lane,
        })
    }

    /// Hands one COMMITTED transition to the publisher. Never blocks, never
    /// awaits, and never calls into plugin code (R1): the caller is the
    /// kernel's own transition-commit path, and a listener must not be able
    /// to slow it down.
    pub(crate) fn offer(self: &Arc<Self>, entry: &EntryId, transition: &Transition) {
        let incarnation = self.lane.incarnation(entry);
        {
            let mut pending = lock(&self.pending);
            pending.published += 1;
            let ordinal = pending.published;
            if pending.queue.len() >= CAPACITY {
                pending.dropped += 1;
            } else {
                pending.queue.push_back(Queued {
                    entry: entry.clone(),
                    incarnation,
                    transition: transition.clone(),
                    ordinal,
                });
            }
        }
        self.ensure();
        self.wake.notify_one();
    }

    /// Starts the publisher task once, on the runtime the offering path is
    /// already running on. A kernel assembled outside a runtime simply has
    /// no publisher until its first offer from one — nothing is lost that a
    /// listener could have received, because no listener can exist yet.
    fn ensure(self: &Arc<Self>) {
        if self.started.load(Ordering::SeqCst) {
            return;
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current()
            && !self.started.swap(true, Ordering::SeqCst)
        {
            runtime.spawn(Arc::clone(self).run());
        }
    }

    /// Takes everything waiting, plus the drops to report.
    fn drain(&self) -> (Vec<Queued>, u64) {
        let mut pending = lock(&self.pending);
        let dropped = std::mem::take(&mut pending.dropped);
        (pending.queue.drain(..).collect(), dropped)
    }

    async fn run(self: Arc<Self>) {
        loop {
            let (batch, dropped) = self.drain();
            if batch.is_empty() && dropped == 0 {
                self.wake.notified().await;
                continue;
            }
            if dropped > 0 {
                self.ledger.record(
                    LedgerEventKind::PublishDropped {
                        topic: TRANSITIONS_TOPIC.to_owned(),
                        dropped,
                    },
                    None,
                    None,
                );
            }
            // THE ORDERING BARRIER. A read through the ledger's single
            // writer answers only once every append sent before it has
            // committed, so every transition in this batch is durably on
            // the stream at a sequence no higher than `mark` — before one
            // byte of it reaches a listener.
            let Ok(mark) = self.ledger.last_sequence().await else {
                // The writer is gone: the kernel can no longer testify to
                // what it would publish, so it publishes nothing (Law 2 —
                // model-visible means LOGGED, in that order).
                return;
            };
            for item in batch {
                self.lane
                    .topics
                    .publish(TRANSITIONS_TOPIC, &payload(&item, mark))
                    .await;
            }
        }
    }
}

/// The delivered record (`contracts/jinn-introspect`, 0.4.0). Every field
/// is one a `jinn:introspect` pull already admits — entry, fiber,
/// incarnation, and the `state` vocabulary — plus the two the publish
/// itself owns. `cause` is deliberately ABSENT: the authority demonstration
/// failed for it (nothing in `jinn:introspect` answers why a transition
/// happened), so rather than widen the grant the kernel does not deliver
/// it; a listener that needs it holds `jinn:ledger` and reads the row.
fn payload(item: &Queued, mark: u64) -> Vec<u8> {
    serde_json::json!({
        "entry": item.entry.0,
        "fiber": item.transition.fiber.0,
        "incarnation": item.incarnation,
        "from": state(item.transition.from),
        "to": state(item.transition.to),
        "ordinal": item.ordinal,
        "committed-by": mark,
    })
    .to_string()
    .into_bytes()
}

/// The `entry.state` vocabulary, spelled exactly as `jinn:introspect`
/// spells it — one wire spelling, so a puller and a listener never have to
/// reconcile two.
fn state(state: FiberState) -> String {
    format!("{state:?}").to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use jinnd_api::{FiberId, TransitionCause};

    fn transition() -> Transition {
        Transition {
            fiber: FiberId(1),
            from: FiberState::Active,
            to: FiberState::Unloading,
            cause: TransitionCause::ConfigChanged,
        }
    }

    fn publisher() -> Arc<Lifecycle> {
        let ledger = Ledger::open_in_memory().unwrap_or_else(|error| panic!("{error}"));
        let sink: Arc<dyn jinnd_wasm::LedgerSink> = Arc::new(crate::support::Sink {
            ledger: ledger.clone(),
            fibers: Arc::new(Mutex::new(std::collections::HashMap::new())),
        });
        let lane = Arc::new(LaneCore::new(sink).unwrap_or_else(|error| panic!("{error:?}")));
        Lifecycle::new(ledger, lane)
    }

    /// Past the bound the kernel DROPS rather than grows or blocks, and the
    /// loss is counted twice over: in the drop tally it will ledger, and as
    /// the gap it leaves in the ordinals a listener receives.
    #[test]
    fn a_full_queue_drops_and_counts_instead_of_growing() {
        let publisher = publisher();
        let entry = EntryId("watcher".to_owned());
        for _ in 0..(CAPACITY + 10) {
            publisher.offer(&entry, &transition());
        }
        let (batch, dropped) = publisher.drain();
        assert_eq!(
            batch.len(),
            CAPACITY,
            "the queue never grows past its bound"
        );
        assert_eq!(dropped, 10, "every loss past the bound is counted");
        let ordinals: Vec<u64> = batch.iter().map(|item| item.ordinal).collect();
        assert_eq!(ordinals.first(), Some(&1));
        assert_eq!(
            ordinals.last(),
            Some(&(CAPACITY as u64)),
            "ordinals are the OFFER count, so the drops show as the gap \
             between the last delivered ordinal and the next one"
        );
    }

    /// Ordinals never repeat and never go backwards: FIFO in, FIFO out.
    #[test]
    fn ordinals_are_monotonic_in_offer_order() {
        let publisher = publisher();
        let entry = EntryId("watcher".to_owned());
        for _ in 0..8 {
            publisher.offer(&entry, &transition());
        }
        let (batch, _) = publisher.drain();
        let ordinals: Vec<u64> = batch.iter().map(|item| item.ordinal).collect();
        assert_eq!(ordinals, (1..=8).collect::<Vec<u64>>());
    }

    /// The delivered record carries the introspect-admitted fields and NOT
    /// the cause: the authority demonstration failed for that one field.
    #[test]
    fn the_delivered_record_omits_the_cause() {
        let item = Queued {
            entry: EntryId("watcher".to_owned()),
            incarnation: Some(3),
            transition: transition(),
            ordinal: 7,
        };
        let wire: serde_json::Value =
            serde_json::from_slice(&payload(&item, 42)).unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(wire["from"], "active");
        assert_eq!(wire["to"], "unloading");
        assert_eq!(wire["incarnation"], 3);
        assert_eq!(wire["committed-by"], 42);
        assert!(wire.get("cause").is_none(), "no cause is delivered: {wire}");
    }
}
