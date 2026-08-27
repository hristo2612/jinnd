//! The Mode-1 swap view over the adapter's live wasm roster (R8): part of
//! the wasm lane, split by responsibility (R10 file hygiene). Every phase
//! runs through the shared loom-modeled [`jinnd_wasm::SwapCore`] — the
//! driver claims atomically before any install, and the disposer effect
//! tombstones the same machine (round-2 blocker-3: one swap machine).

use std::sync::Arc;

use jinnd_api::{EntryId, ErrorCode, FiberId, KernelFuture};
use jinnd_wasm::{
    ActivationOutcome, InstanceHandle, LoadedComponent, NoRealms, PeerId, Seat, SharedSlot,
    SwapSlots, commit_staged,
};

use super::{DEADLINE, WasmState};
use crate::{error, lock};

/// One health-gated staged instance WITH the outcome its activation
/// registered: the outcome is committed at install, never discarded
/// (round-2 blocker-4).
pub(super) struct Staged {
    instance: InstanceHandle,
    outcome: ActivationOutcome,
}

/// The swap machine's view over the live roster (R8): prepare stages a new
/// instance (staging seat — nothing routes to it), install commits it into
/// the seat after the batch claim, discard disposes it; the old instance
/// stays warm throughout.
pub(super) struct LaneSlots {
    pub(super) state: Arc<WasmState>,
    pub(super) fresh: LoadedComponent,
}

struct Live {
    slot: Arc<SharedSlot>,
    peer: PeerId,
    fiber: FiberId,
    context: u64,
    config: Vec<u8>,
}

impl LaneSlots {
    fn live(&self, entry: &EntryId) -> Option<Live> {
        let roster = lock(&self.state.roster);
        roster.get(entry).map(|live| Live {
            slot: Arc::clone(&live.slot),
            peer: live.peer,
            fiber: live.fiber,
            context: live.context,
            config: live.config.clone(),
        })
    }
}

impl SwapSlots for LaneSlots {
    type Prepared = Staged;

    fn entries_pinned_to(&self, hash: &str) -> Vec<(EntryId, u64)> {
        let roster = lock(&self.state.roster);
        let mut entries: Vec<(EntryId, u64)> = roster
            .iter()
            .filter(|(_, live)| lock(&live.component).hash() == hash)
            .map(|(entry, live)| (entry.clone(), live.slot_id))
            .collect();
        entries.sort();
        entries
    }

    fn prepare(&self, entry: &EntryId) -> KernelFuture<'_, Staged> {
        let entry = entry.clone();
        Box::pin(async move {
            let live = self
                .live(&entry)
                .ok_or_else(|| error(ErrorCode::InvalidProfile, "entry left the roster"))?;
            let old = live
                .slot
                .current()
                .ok_or_else(|| error(ErrorCode::PluginFailed, "no live instance to hand off"))?;
            let handoff = old.snapshot().await?;
            let staged = self.state.host.instantiate(
                &self.fresh,
                Seat {
                    broker: Arc::clone(&self.state.broker),
                    topics: Arc::clone(&self.state.topics),
                    oracle: Arc::new(NoRealms),
                    peer: live.peer,
                    fiber: Some(live.fiber),
                    context: live.context,
                    deadline: DEADLINE,
                    slot: None,
                    staging: true,
                },
            );
            let (outcome, contributed) = staged.activate(live.config).await;
            let healthy = match outcome {
                Ok(()) => staged.restore(handoff).await,
                Err(refused) => Err(refused),
            };
            if let Err(refused) = healthy {
                staged.dispose().await;
                return Err(refused);
            }
            Ok(Staged {
                instance: staged,
                outcome: contributed,
            })
        })
    }

    /// Runs only after the batch claim landed. Commits the staged outcome —
    /// listens rebound to the new instance's own face, provisions diffed,
    /// old instance disposed — then converges with any disposal that raced
    /// the install: a tombstone observed afterwards means the disposer could
    /// not see what we landed, so we retire it ourselves.
    fn install(&self, entry: &EntryId, slot_id: u64, staged: Staged) -> KernelFuture<'_, ()> {
        let entry = entry.clone();
        Box::pin(async move {
            let Some(live) = self.live(&entry) else {
                staged.instance.dispose().await;
                return Err(error(
                    ErrorCode::InvalidProfile,
                    "the entry was disposed before commit",
                ));
            };
            commit_staged(
                &live.slot,
                staged.instance,
                staged.outcome,
                &self.state.broker,
                &self.state.topics,
                live.peer,
                Some(live.fiber),
                live.context,
                self.state.sink.as_ref(),
            )
            .await;
            if self.state.swap.is_tombstone(slot_id) {
                if let Some(seat) = live.slot.take() {
                    let _ = seat
                        .retire(&self.state.broker, &self.state.topics, live.peer)
                        .await;
                }
                return Err(error(
                    ErrorCode::InvalidProfile,
                    "disposal won the entry during commit",
                ));
            }
            Ok(())
        })
    }

    fn discard(&self, staged: Staged) -> KernelFuture<'_, ()> {
        Box::pin(async move {
            staged.instance.dispose().await;
            Ok(())
        })
    }
}
