//! The restart-window row (M2-K26; harness FINDINGS #47): between a
//! replacement's suspension and its commit, a listen registration is a
//! TOMBSTONE — the same row, no delivery target. It is selected exactly as
//! the registration was, so the M2-K9 refusal has a listener to key on for
//! the whole window instead of a walk that selects nobody and answers its
//! own payload; `rebind` replaces an entry's tombstones with its staged
//! listens under the one lock (R8); and a fiber that rests without a
//! successor has them withdrawn on the record (I4). One row kind, one
//! table (R10): the tombstone IS the record of what was entombed. Split
//! from `topics.rs` by responsibility (R10 file hygiene).

use jinnd_api::{DispatchMode, EntryId, FiberId, Owed};

use super::restarting::expects_reply;
use super::{Delivery, Listener, LocalTopics, Selected, Unserved};
use crate::selector::{RealmOracle, Selector, selects};

/// What one walk selected from the table: the LIVE rows it will deliver
/// to, the tombstones it must be decided against, and every selected
/// fiber in table order for the oracle to answer about.
pub(super) struct Selection {
    pub(super) live: Vec<Selected>,
    /// The first selected tombstone, as the refusal it names when the
    /// oracle does not explain it: `stalled` — nothing is coming until
    /// the environment moves (R9), never a delivery to nobody.
    pub(super) tombstone: Option<Unserved>,
    pub(super) fibers: Vec<FiberId>,
}

impl LocalTopics {
    /// Turns a live registration into a tombstone (M2-K26 (a)): the row
    /// stays selectable under its own topic and context, delivers to
    /// nothing, and names the incarnation it was entombed under so a
    /// refusal can identify it even after the roster has moved on.
    /// Idempotent on an already entombed row; `None` for an unknown id.
    /// Returns the topic — the caller's Law-2 label, when it needs one.
    pub fn entomb(&self, id: u64, entry: EntryId, incarnation: u64) -> Option<String> {
        let mut inner = self.lock();
        let listener = inner
            .listeners
            .iter_mut()
            .find(|listener| listener.id == id)?;
        listener.delivery = Delivery::Tombstone { entry, incarnation };
        Some(listener.topic.clone())
    }

    /// The tombstones a fiber left, `(id, topic)` in registration order —
    /// exactly this fiber's, no other entry's (I1). What the replacement's
    /// commit hands `rebind` as the rows to replace.
    #[must_use]
    pub fn entombed(&self, fiber: FiberId) -> Vec<(u64, String)> {
        self.lock()
            .listeners
            .iter()
            .filter(|listener| listener.fiber == Some(fiber) && listener.is_tombstone())
            .map(|listener| (listener.id, listener.topic.clone()))
            .collect()
    }

    /// The incarnation an entry's tombstones were entombed under, while
    /// any remain: the restart oracle's answer for an entry whose roster
    /// row has already left with its seat (M2-K26 (c)).
    #[must_use]
    pub fn entombed_incarnation(&self, entry: &EntryId) -> Option<u64> {
        self.lock()
            .listeners
            .iter()
            .find_map(|listener| match &listener.delivery {
                Delivery::Tombstone {
                    entry: owner,
                    incarnation,
                } if owner == entry => Some(*incarnation),
                _ => None,
            })
    }

    /// Withdraws every tombstone of `fiber` — the fiber rested `Failed`
    /// or `Disposed` with no successor to commit (M2-K26 (c); I4) — and
    /// hands back their topics, in registration order, for the record.
    /// Idempotent: a second call finds nothing.
    pub fn withdraw_tombstones(&self, fiber: FiberId) -> Vec<String> {
        let mut inner = self.lock();
        let mut topics = Vec::new();
        inner.listeners.retain(|listener| {
            if listener.fiber == Some(fiber) && listener.is_tombstone() {
                topics.push(listener.topic.clone());
                false
            } else {
                true
            }
        });
        topics
    }

    /// One walk's selection from a snapshot of the table (no lock is held
    /// across a delivery, R1). A tombstone is selected exactly as its
    /// registration was, by the same selector over the same context —
    /// for a reply-expecting walk. Fire-and-forget is not decided by a
    /// pending transition (M2-K9's scope), so it skips tombstones and is
    /// lost in the window as it is today: the NAMED LIMIT of M2-K26.
    pub(super) fn select(
        &self,
        emitter: u64,
        topic: &str,
        mode: DispatchMode,
        selector: &Selector,
        oracle: &dyn RealmOracle,
    ) -> Selection {
        let inner = self.lock();
        let mut selection = Selection {
            live: Vec::new(),
            tombstone: None,
            fibers: Vec::new(),
        };
        for listener in inner
            .listeners
            .iter()
            .filter(|listener| listener.topic == topic)
            .filter(|listener| selects(selector, oracle, emitter, listener.context))
        {
            match &listener.delivery {
                Delivery::Live(target) => {
                    selection.fibers.extend(listener.fiber);
                    selection.live.push((
                        listener.fiber,
                        listener.token,
                        listener.budget,
                        std::sync::Arc::clone(target),
                    ));
                }
                Delivery::Tombstone { entry, incarnation } if expects_reply(mode) => {
                    selection.fibers.extend(listener.fiber);
                    selection.tombstone.get_or_insert_with(|| Unserved {
                        entry: entry.clone(),
                        incarnation: *incarnation,
                        owed: Owed::Stalled,
                    });
                }
                Delivery::Tombstone { .. } => {}
            }
        }
        selection
    }
}

impl Listener {
    pub(super) fn is_tombstone(&self) -> bool {
        matches!(self.delivery, Delivery::Tombstone { .. })
    }
}
