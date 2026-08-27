//! Kernel-boundary wiring for forward effects and revert (M1-P7): the driver
//! that walks a forward effect behind the facade, the slot its installed
//! inverse waits in, and the revert lane's executable inverses.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use jinnd_api::{
    EffectId, ErrorCode, ForwardEffect, KernelError, LedgerEventKind, RevertResolution,
};
use jinnd_effects::{Disposer, EffectScope, ForwardEnd, UndoOutcome};
use jinnd_ledger::Ledger;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::{error, lock};

/// Where a landed forward effect's inverse waits for its withdrawal.
type Slot = Arc<Mutex<Option<Disposer>>>;
/// The settled outcome of one forward walk, observable by every waiter.
type Settled = watch::Receiver<Option<Result<(), KernelError>>>;

/// One begun forward effect, tracked until it is disposed.
pub(crate) struct PendingEffect {
    pub(crate) stale: CancellationToken,
    pub(crate) settled: Settled,
}

pub(crate) type PendingMap = Mutex<HashMap<EffectId, PendingEffect>>;

/// Registers the effect's record synchronously — the id is minted here — and
/// spawns the driver that walks the forward actions (R1: the walk runs on its
/// own task; no lock is held across any action).
pub(crate) fn begin(
    scope: &Arc<Mutex<EffectScope>>,
    pending: &PendingMap,
    ledger: &Ledger,
    label: String,
    forward: ForwardEffect,
) -> Result<EffectId, KernelError> {
    let slot: Slot = Arc::new(Mutex::new(None));
    let stale = CancellationToken::new();
    let (announce, settled) = watch::channel(None);

    let undo_slot = Arc::clone(&slot);
    let undo_stale = stale.clone();
    let mut undo_settled = settled.clone();
    let id = lock(scope).register(
        label.clone(),
        // The record's inverse: divert the walk, wait for the launched action
        // to land — a launched transition always lands — then discharge
        // whatever installed. A walk that failed or diverted installed
        // nothing and already unwound itself.
        Disposer::future(move || async move {
            undo_stale.cancel();
            let _ = undo_settled.wait_for(Option::is_some).await;
            let installed = lock(&undo_slot).take();
            match installed {
                Some(disposer) => discharged(disposer, "installed inverse").await,
                None => Ok(()),
            }
        }),
    )?;
    ledger.record(
        LedgerEventKind::EffectRegistered {
            label: label.clone(),
        },
        None,
        Some(crate::KERNEL_SCOPE),
    );

    let driver_scope = Arc::clone(scope);
    let driver_stale = stale.clone();
    let driver_ledger = ledger.clone();
    tokio::spawn(async move {
        let end = jinnd_effects::advance(forward, &driver_stale).await;
        let outcome = match end {
            ForwardEnd::Installed(disposer) => {
                *lock(&slot) = Some(disposer);
                Ok(())
            }
            // The walk unwound its yielded prefix itself; the bookkeeping
            // record is dropped without running its (empty-slot) inverse.
            ForwardEnd::Diverted { .. } => {
                drop(lock(&driver_scope).detach(id));
                Ok(())
            }
            // All-or-none / prefix rollback: nothing stays installed and the
            // record leaves the tree (paper Def 51/52).
            ForwardEnd::Failed { error, .. } => {
                drop(lock(&driver_scope).detach(id));
                driver_ledger.record(
                    LedgerEventKind::ErrorRecorded {
                        error: error.clone(),
                    },
                    None,
                    Some(crate::KERNEL_SCOPE),
                );
                Err(error)
            }
        };
        let _ = announce.send(Some(outcome));
    });

    lock(pending).insert(id, PendingEffect { stale, settled });
    Ok(id)
}

/// The forward walk's settled outcome: `Ok` for installed or cleanly
/// diverted, the original error for a failed action. `Ok` immediately for an
/// id this lane never drove.
pub(crate) async fn outcome(pending: &PendingMap, effect: EffectId) -> Result<(), KernelError> {
    let settled = lock(pending)
        .get(&effect)
        .map(|entry| entry.settled.clone());
    let Some(mut settled) = settled else {
        return Ok(());
    };
    let state = settled
        .wait_for(Option::is_some)
        .await
        .map_err(|_| error(ErrorCode::EffectFailed, "the effect driver went away"))?;
    state.clone().unwrap_or(Ok(()))
}

/// Diverts an in-flight walk (the launched action lands first), then
/// withdraws the record and its subtree exactly once. Idempotent.
pub(crate) async fn dispose(
    scope: &Arc<Mutex<EffectScope>>,
    pending: &PendingMap,
    ledger: &Ledger,
    effect: EffectId,
) -> Result<(), KernelError> {
    let in_flight = lock(pending).remove(&effect);
    if let Some(entry) = in_flight {
        entry.stale.cancel();
        let mut settled = entry.settled;
        let _ = settled.wait_for(Option::is_some).await;
    }
    let detached = lock(scope).detach(effect);
    if let Some(detached) = detached {
        // Driven with every lock released (R1); containment inside (R11).
        let report = detached.withdraw().await;
        for line in &report.effects {
            ledger.record(
                LedgerEventKind::EffectWithdrawn {
                    label: line.label.clone(),
                    clean: line.outcome.is_done(),
                },
                None,
                Some(crate::KERNEL_SCOPE),
            );
        }
    }
    Ok(())
}

/// The revert lane's executable inverse for one kernel-scope effect: detach
/// at execution time — never before intent is durable — withdraw, and answer
/// clean or the first recorded failure.
pub(crate) fn revert_inverse(
    scope: &Arc<Mutex<EffectScope>>,
    effect: EffectId,
) -> jinnd_ledger::Inverse {
    let scope = Arc::clone(scope);
    Box::new(move || {
        Box::pin(async move {
            let Some(detached) = lock(&scope).detach(effect) else {
                return Err(error(
                    ErrorCode::EffectFailed,
                    "the effect is no longer live in the kernel scope",
                ));
            };
            let report = detached.withdraw().await;
            match report.unclean().next() {
                None => Ok(()),
                Some(line) => Err(error(
                    ErrorCode::EffectFailed,
                    &format!("an inverse did not complete: {:?}", line.outcome),
                )),
            }
        })
    })
}

/// True when `effect` is live in the scope's tree, at any depth.
pub(crate) fn is_live(scope: &Arc<Mutex<EffectScope>>, effect: EffectId) -> bool {
    fn contains(descriptors: &[jinnd_api::EffectDescriptor], effect: EffectId) -> bool {
        descriptors
            .iter()
            .any(|entry| entry.id == effect || contains(&entry.children, effect))
    }
    contains(&lock(scope).tree(), effect)
}

/// Refuses revert operations the protocol cannot honestly serve: an unknown
/// effect with no branch, or a forward walk that has not settled.
pub(crate) fn revert_admissible(
    scope: &Arc<Mutex<EffectScope>>,
    pending: &PendingMap,
    resolution: Option<RevertResolution>,
    effect: EffectId,
) -> Result<(), KernelError> {
    if resolution.is_some() {
        return Ok(());
    }
    let unsettled = lock(pending)
        .get(&effect)
        .is_some_and(|entry| entry.settled.borrow().is_none());
    if unsettled {
        return Err(error(
            ErrorCode::EffectFailed,
            "the effect's forward walk has not settled; revert applies to settled effects",
        ));
    }
    if !is_live(scope, effect) {
        return Err(error(
            ErrorCode::EffectFailed,
            "no such live effect and no recorded revert branch",
        ));
    }
    Ok(())
}

/// Maps a facade compensator to the lane's executable form, contained.
pub(crate) fn compensator_inverse(compensator: Box<dyn jinnd_api::Undo>) -> jinnd_ledger::Inverse {
    Box::new(move || Box::pin(discharged(Disposer::Whole(compensator), "compensator")))
}

/// Discharges one detached inverse and renders its outcome as a result.
async fn discharged(disposer: Disposer, subject: &'static str) -> Result<(), KernelError> {
    match jinnd_effects::discharge(disposer).await {
        UndoOutcome::Done => Ok(()),
        UndoOutcome::Failed(failure) => Err(failure),
        stopped => Err(error(
            ErrorCode::EffectFailed,
            &format!("the {subject} did not complete: {stopped:?}"),
        )),
    }
}
