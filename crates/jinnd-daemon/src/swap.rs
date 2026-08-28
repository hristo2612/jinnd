//! Mode-1 hot-swap over the daemon's live roster (R8): the operator replaces
//! an artifact file (with its `.sha256` pin sidecar — Law 5: the operator
//! states the pin, the kernel verifies it), and the batch replaces every
//! live entry pinned to the old hash. All fallible work happens in
//! `prepare` (activate staged + state handoff = the health gate); `commit`
//! is infallible bookkeeping inside the claim's critical section; any
//! failure rolls the whole batch back with the old instances still serving.

use std::sync::Arc;

use jinnd_api::{EntryId, ErrorCode, FiberId, KernelFuture, LedgerEventKind};
use jinnd_wasm::{
    ActivationOutcome, InstanceHandle, LedgerSink, LoadedComponent, NoRealms, PeerId, Seat,
    SeatState, SharedSlot, SwapOutcome, SwapSlots, commit_staged, swap_batch,
};

use crate::lane::LaneState;
use crate::support::{DEADLINE, error, lock};

/// One health-gated staged instance WITH everything the infallible commit
/// needs, captured while failing was still allowed.
pub(crate) struct Staged {
    instance: InstanceHandle,
    outcome: ActivationOutcome,
    slot: Arc<SharedSlot>,
    peer: PeerId,
    fiber: FiberId,
    context: u64,
}

/// Withdraws a staged-but-never-committed instance: its staged effects
/// replay in reverse through the instance that registered them (R5, I1);
/// a failing inverse is contained and recorded (R6, R11).
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

/// The swap machine's view over the daemon roster.
struct LaneSlots {
    state: Arc<LaneState>,
    fresh: LoadedComponent,
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
            let (slot, peer, fiber, context, config) = {
                let roster = lock(&self.state.roster);
                let live = roster.get(&entry).ok_or_else(|| {
                    error(ErrorCode::InvalidProfile, "entry left the roster".into())
                })?;
                (
                    Arc::clone(&live.slot),
                    live.peer,
                    live.fiber,
                    live.context,
                    live.config.clone(),
                )
            };
            let old = slot.current().ok_or_else(|| {
                error(
                    ErrorCode::PluginFailed,
                    "no live instance to hand off".into(),
                )
            })?;
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

    /// Pure bookkeeping inside the batch claim's critical section: no
    /// lookup, no await, no failure path.
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
            // committed successor (warm until commit, R8).
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

/// Swaps every live entry of `package` from its current artifact to the
/// bytes pinned by `pin`. A pin equal to the current hash is a no-op; a
/// committed batch retargets every package cell sharing the old artifact,
/// so future activations use the new one too (batch-by-hash, R8).
pub(crate) async fn swap_package(
    state: &Arc<LaneState>,
    package: &str,
    bytes: Vec<u8>,
    pin: &str,
) -> Result<SwapOutcome, jinnd_api::KernelError> {
    let cell = lock(&state.packages).get(package).cloned().ok_or_else(|| {
        error(
            ErrorCode::InvalidProfile,
            format!("no registered wasm package {package:?}"),
        )
    })?;
    let old_hash = lock(&cell).hash().to_owned();
    if old_hash == pin {
        return Ok(SwapOutcome {
            swapped: Vec::new(),
            rolled_back: false,
        });
    }
    let fresh = state.host.load(bytes, pin, state.sink.as_ref())?;
    let slots = LaneSlots {
        state: Arc::clone(state),
        fresh: fresh.clone(),
    };
    let outcome = swap_batch(
        &slots,
        &state.swap,
        &old_hash,
        fresh.hash(),
        state.sink.as_ref(),
    )
    .await?;
    if !outcome.rolled_back {
        for shared in lock(&state.packages).values() {
            let mut pinned = lock(shared);
            if pinned.hash() == old_hash {
                *pinned = fresh.clone();
            }
        }
    }
    Ok(outcome)
}

impl crate::daemon::Daemon {
    /// Mode-1 hot-swap of one package from its artifact file + `.sha256`
    /// pin sidecar (R8; the operator states the pin, the kernel verifies).
    ///
    /// # Errors
    ///
    /// Unknown package, unreadable artifact or sidecar, refused pin. A
    /// failed health gate is NOT an error: the batch rolls back and the
    /// outcome says so.
    pub async fn swap(&self, package: &str) -> Result<SwapOutcome, jinnd_api::KernelError> {
        let name = crate::packages::basename(package);
        let file = self.paths.artifacts.join(format!("{name}.wasm"));
        let sidecar = self.paths.artifacts.join(format!("{name}.wasm.sha256"));
        let bytes = tokio::fs::read(&file)
            .await
            .map_err(|refused| error(ErrorCode::InvalidProfile, refused.to_string()))?;
        let pin = tokio::fs::read_to_string(&sidecar)
            .await
            .map_err(|refused| {
                error(
                    ErrorCode::InvalidProfile,
                    format!("no pin sidecar {} (Law 5): {refused}", sidecar.display()),
                )
            })?;
        let outcome = swap_package(&self.lane, package, bytes, pin.trim()).await?;
        self.sync_transitions();
        Ok(outcome)
    }
}
