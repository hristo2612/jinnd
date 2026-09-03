//! One seat's closing sequence (M2-K4), shared by its two inverses —
//! split from `lane.rs` by responsibility (R10 file hygiene).

use std::sync::Arc;

use jinnd_api::EntryId;

use crate::peer::PeerId;
use crate::slot::{SeatState, SharedSlot};

use super::{LaneCore, lock};

/// One seat's closing sequence (M2-K4), shared by its two inverses.
#[derive(Clone)]
pub(super) struct SeatClosing {
    pub(super) slot: Arc<SharedSlot>,
    pub(super) entry: EntryId,
    pub(super) owner: Arc<LaneCore>,
    pub(super) slot_id: u64,
    pub(super) peer: PeerId,
}

impl SeatClosing {
    /// Tombstones the swap slot, then closes the seat in law order (M2-K5
    /// #16): door shut, the instance's in-flight guest entry DRAINED under
    /// its deadline, journal sealed — and takes the seat, or `None` when no
    /// seat was ever installed.
    pub(super) async fn close(&self) -> Option<SeatState> {
        self.owner.swap.dispose(self.slot_id);
        let instance = self.slot.current();
        self.slot
            .close(async move {
                if let Some(instance) = instance {
                    instance.seal().await;
                }
            })
            .await;
        self.slot.take()
    }

    /// The seat is gone: the peer and the roster row go with it.
    pub(super) fn forget(&self) {
        self.owner.broker.remove_peer(self.peer);
        lock(&self.owner.roster).remove(&self.entry);
    }
}
