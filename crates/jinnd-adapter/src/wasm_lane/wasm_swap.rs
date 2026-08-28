//! The Mode-1 swap view over the adapter's live wasm roster (R8), split by
//! responsibility (R10). Every phase runs through the shared loom-modeled
//! [`jinnd_wasm::SwapCore`]: the commit bookkeeping runs INSIDE the batch
//! claim's critical section — the same lock the disposer's tombstone takes —
//! so a disposal lands entirely before the claim (whole-batch rollback) or
//! entirely after commit, where it retires the committed seat itself
//! (round-3 ruling; round-2 blocker-3).

use std::sync::Arc;

use jinnd_api::{EntryId, ErrorCode, FiberId, KernelFuture, LedgerEventKind};
use jinnd_wasm::{
    ActivationOutcome, InstanceHandle, LedgerSink, LoadedComponent, NoRealms, PeerId, Seat,
    SeatState, SharedSlot, SwapSlots, commit_staged,
};

use super::{DEADLINE, WasmState};
use crate::{error, lock};

/// One health-gated staged instance WITH the outcome its activation
/// registered and the roster row it was prepared against — everything the
/// infallible commit needs, captured while failing was still allowed
/// (round-3 ruling: commit performs no lookup that could miss).
pub(super) struct Staged {
    instance: InstanceHandle,
    outcome: ActivationOutcome,
    slot: Arc<SharedSlot>,
    peer: PeerId,
    fiber: FiberId,
    context: u64,
}

/// Withdraws a staged-but-never-committed instance: its staged effects
/// REPLAY in reverse through the instance that registered them, then the
/// instance disposes (round-3 ruling; R5, I1 — a raw dispose that skips
/// `outcome.effects` leaves an off-tree contribution). Staged provisions
/// and listens were recorded, never routed (staging seat), so the guest
/// inverses are the whole contribution to withdraw. A failing inverse is
/// contained and recorded, never silent (R6, R11).
async fn unwind(
    instance: InstanceHandle,
    outcome: ActivationOutcome,
    fiber: FiberId,
    sink: &dyn LedgerSink,
) {
    for (_, token) in outcome.effects().rev() {
        if let Err(error) = instance.undo(token).await {
            sink.append(LedgerEventKind::ErrorRecorded { error }, Some(fiber));
        }
    }
    instance.dispose().await;
}

/// The swap machine's roster view (R8): prepare stages a new instance
/// (staging seat — nothing routes to it) and holds every fallible step;
/// commit is the infallible bookkeeping inside the claim; discard unwinds a
/// staged instance; the old instance stays warm throughout.
pub(super) struct LaneSlots {
    pub(super) state: Arc<WasmState>,
    pub(super) fresh: LoadedComponent,
}

/// One roster row's view: (slot, peer, fiber, context, config).
type Live = (Arc<SharedSlot>, PeerId, FiberId, u64, Vec<u8>);

impl LaneSlots {
    fn live(&self, entry: &EntryId) -> Option<Live> {
        let roster = lock(&self.state.roster);
        roster.get(entry).map(|live| {
            (
                Arc::clone(&live.slot),
                live.peer,
                live.fiber,
                live.context,
                live.config.clone(),
            )
        })
    }
}

impl SwapSlots for LaneSlots {
    type Prepared = Staged;
    type Displaced = SeatState;

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
            let (slot, peer, fiber, context, config) = self
                .live(&entry)
                .ok_or_else(|| error(ErrorCode::InvalidProfile, "entry left the roster"))?;
            let old = slot
                .current()
                .ok_or_else(|| error(ErrorCode::PluginFailed, "no live instance to hand off"))?;
            let handoff = old.snapshot().await?;
            let staged = self.state.host.instantiate(
                &self.fresh,
                Seat {
                    broker: Arc::clone(&self.state.broker),
                    topics: Arc::clone(&self.state.topics),
                    oracle: Arc::new(NoRealms),
                    peer,
                    fiber: Some(fiber),
                    context,
                    deadline: DEADLINE,
                    slot: None,
                    staging: true,
                },
            );
            let (outcome, contributed) = staged.activate(config).await;
            let healthy = match outcome {
                Ok(()) => staged.restore(handoff).await,
                Err(refused) => Err(refused),
            };
            if let Err(refused) = healthy {
                unwind(staged, contributed, fiber, self.state.sink.as_ref()).await;
                return Err(refused);
            }
            Ok(Staged {
                instance: staged,
                outcome: contributed,
                slot,
                peer,
                fiber,
                context,
            })
        })
    }

    /// Pure bookkeeping inside the batch claim's critical section (round-3
    /// ruling): no lookup, no await, no failure path — the roster row was
    /// captured at prepare, and a disposal that could invalidate it would
    /// have tombstoned the claim first.
    fn commit(&self, _entry: &EntryId, staged: Staged) -> Option<SeatState> {
        commit_staged(
            &staged.slot,
            staged.instance,
            staged.outcome,
            &self.state.broker,
            &self.state.topics,
            staged.peer,
            Some(staged.fiber),
            staged.context,
            self.state.sink.as_ref(),
        )
    }

    fn retire_displaced(&self, displaced: SeatState) -> KernelFuture<'_, ()> {
        Box::pin(async move {
            // Dispose-only: the handoff transferred its contribution to the
            // committed successor, whose own activation registered its own
            // inverses (warm until commit, R8).
            displaced.instance.dispose().await;
            Ok(())
        })
    }

    fn discard(&self, staged: Staged) -> KernelFuture<'_, ()> {
        Box::pin(async move {
            unwind(
                staged.instance,
                staged.outcome,
                staged.fiber,
                self.state.sink.as_ref(),
            )
            .await;
            Ok(())
        })
    }
}
