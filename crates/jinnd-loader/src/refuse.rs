//! The conflict-point refusals (R1; M1-P6b/P6c): checks that keep the loader
//! from ever *beginning* an operation whose waits a plugin could hold up.
//! Split from `gate.rs` by responsibility (R10): the gate owns the exclusion
//! primitives, this module owns the refusal decisions built on kernel state.

use std::sync::Arc;

use jinnd_api::{ErrorCode, KernelError};

use crate::lanes::EntryHandle;
use crate::loader::Loader;
use crate::state::{error, lock};

impl Loader {
    /// Refuses to *begin* a fiber-awaiting operation while any tracked
    /// fiber's withdrawal replay is in flight (R1, M1-P6b round-4 law): the
    /// loader never takes a wait an in-progress teardown could hold up, so no
    /// deadlock cycle can close through a loader wait — whoever asks, from
    /// whatever task, with no caller analysis and no timers. The refusal is
    /// honest and retryable: amend again after quiescence.
    ///
    /// The check is causal, not racy: a call issued from within a withdrawal
    /// replay — directly or via tasks it spawned — happens-after that
    /// replay's begin, so it always observes the conflict. A withdrawal
    /// that begins only *after* an operation was admitted cannot be waiting
    /// on that operation's outcome; its own re-entrant calls are refused by
    /// this same check, so it completes, releases its leases, and the
    /// admitted operation's waits resolve (I3).
    ///
    /// Handles are queried with no lock held (R1). Jurisdiction is honest
    /// too: the loader can only see fibers its document tracks — a
    /// harness-spawned fiber outside the document is outside this horizon.
    pub(crate) fn refuse_amid_withdrawal(&self, operation: &str) -> Result<(), KernelError> {
        let handles: Vec<Arc<dyn EntryHandle>> = lock(&self.state)
            .entries
            .values()
            .filter_map(|runtime| runtime.live.as_ref().map(|live| Arc::clone(&live.handle)))
            .collect();
        if handles.iter().any(|handle| handle.withdrawing()) {
            return Err(error(
                ErrorCode::InvalidProfile,
                &format!(
                    "{operation} refused: a fiber withdrawal is in flight; \
                     retry after quiescence"
                ),
            ));
        }
        Ok(())
    }
}

/// Refuses `operation` unless the target entry's fiber is at REST — committed
/// state equal to the desired one, no transition in flight (the M1-P6c
/// round-2 law). This is the mechanism that closes the self-amend deadlock
/// class: an activation runs strictly inside its fiber's `Loading` window, so
/// an amendment of that entry issued from the activation — directly, or
/// through any chain of spawned-and-awaited tasks — always observes the
/// fiber mid-transition and is refused before the loader takes a wait the
/// activation itself must outlive. Decided entirely from kernel-owned state:
/// no task-locals, no caller identity. Honest and retryable: amend again once
/// the fiber settles. A transition that begins only *after* admission cannot
/// be waiting on this operation — the admitted amendment's waits are fiber
/// settling, which completes regardless (I3).
pub(crate) fn refuse_unrested(
    handle: &dyn EntryHandle,
    operation: &str,
) -> Result<(), KernelError> {
    if !handle.resting() {
        return Err(error(
            ErrorCode::InvalidProfile,
            &format!(
                "{operation} refused: the entry's fiber is mid-transition and the \
                 operation would await it; retry after the fiber settles"
            ),
        ));
    }
    Ok(())
}

/// Refuses `operation` when the calling task IS the fiber the operation would
/// await (M1-P6c): an entry's own activation amending or disposing its entry
/// makes the loader wait for a fiber that cannot settle until this very call
/// returns — a self-deadlock, not a race. No longer load-bearing (a plugin's
/// own spawn escapes any task-local — [`refuse_unrested`] is the mechanism,
/// round 2); kept because it answers the common direct shape earliest and
/// with the sharpest message. A sibling amendment from the same activation
/// stays admissible.
pub(crate) fn refuse_own_fiber(
    handle: &dyn EntryHandle,
    operation: &str,
) -> Result<(), KernelError> {
    if jinnd_fiber::current_fiber() == Some(handle.id()) {
        return Err(error(
            ErrorCode::InvalidProfile,
            &format!(
                "{operation} refused: it would await the calling task's own fiber; \
                 amend from outside the fiber"
            ),
        ));
    }
    Ok(())
}

/// Refuses `operation` when invoked from within a fiber's teardown context
/// (M1-P6b): teardown is the wrong time to reshape the profile — I2 entitles
/// a dying plugin to call the services it leases while unloading, never to
/// amend the document — and admitting any such amendment reopens the
/// re-entrant deadlock class (R1).
pub(crate) fn refuse_teardown_context(operation: &str) -> Result<(), KernelError> {
    if jinnd_fiber::in_teardown() {
        return Err(error(
            ErrorCode::InvalidProfile,
            &format!("{operation} refused: the profile cannot be amended from a teardown context"),
        ));
    }
    Ok(())
}
