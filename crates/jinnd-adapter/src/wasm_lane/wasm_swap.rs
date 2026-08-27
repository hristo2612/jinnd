//! The Mode-1 swap view over the adapter's live wasm roster (R8): part of
//! the wasm lane, split by responsibility (R10 file hygiene).

use std::sync::Arc;

use jinnd_api::{EntryId, ErrorCode, KernelFuture};
use jinnd_wasm::{InstanceHandle, LoadedComponent, NoRealms, Seat, SwapSlots};

use super::{DEADLINE, WasmState};
use crate::{error, lock};

/// The swap machine's view over the live roster (R8): prepare stages a new
/// instance (staging seat — nothing routes to it), commit installs it into
/// the slot, discard disposes it; the old instance stays warm throughout.
pub(super) struct LaneSlots {
    pub(super) state: Arc<WasmState>,
    pub(super) fresh: LoadedComponent,
}

impl SwapSlots for LaneSlots {
    type Prepared = (EntryId, InstanceHandle);

    fn entries_pinned_to(&self, hash: &str) -> Vec<EntryId> {
        let roster = lock(&self.state.roster);
        let mut entries: Vec<EntryId> = roster
            .iter()
            .filter(|(_, slot)| lock(&slot.component).hash() == hash)
            .map(|(entry, _)| entry.clone())
            .collect();
        entries.sort();
        entries
    }

    fn prepare(&self, entry: &EntryId) -> KernelFuture<'_, Self::Prepared> {
        let entry = entry.clone();
        Box::pin(async move {
            let (slot, peer, fiber, context, config) = {
                let roster = lock(&self.state.roster);
                let live = roster
                    .get(&entry)
                    .ok_or_else(|| error(ErrorCode::InvalidProfile, "entry left the roster"))?;
                (
                    Arc::clone(&live.slot),
                    live.peer,
                    live.fiber,
                    live.context,
                    live.config.clone(),
                )
            };
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
            let (outcome, _) = staged.activate(config).await;
            let healthy = match outcome {
                Ok(()) => staged.restore(handoff).await,
                Err(refused) => Err(refused),
            };
            if let Err(refused) = healthy {
                staged.dispose().await;
                return Err(refused);
            }
            Ok((entry, staged))
        })
    }

    fn commit(&self, _: &EntryId, prepared: Self::Prepared) -> KernelFuture<'_, ()> {
        let (entry, staged) = prepared;
        Box::pin(async move {
            let slot = lock(&self.state.roster)
                .get(&entry)
                .map(|live| Arc::clone(&live.slot))
                .ok_or_else(|| error(ErrorCode::InvalidProfile, "entry left the roster"))?;
            if let Some(old) = slot.install(staged) {
                old.dispose().await;
            }
            Ok(())
        })
    }

    fn discard(&self, prepared: Self::Prepared) -> KernelFuture<'_, ()> {
        Box::pin(async move {
            prepared.1.dispose().await;
            Ok(())
        })
    }
}
