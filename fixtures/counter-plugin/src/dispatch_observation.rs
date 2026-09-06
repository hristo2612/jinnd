//! Opt-in request attribution for the daemon dispatch regression. Existing
//! fixture modes retain their payloads, effects and answers. Markers use
//! ordinary guest fs effects, so the ledger brackets the synchronous walk
//! without adding a kernel trace field or relying on writer timestamps.

use super::*;
use jinn::plugin::types::{DispatchMode, KernelError, Selector};

static ATTEMPT: AtomicU64 = AtomicU64::new(0);
type Answer = Result<Vec<Vec<u8>>, KernelError>;

pub(super) fn emit(observed: bool) -> Result<(String, Answer), GuestFault> {
    let attempt = if observed {
        format!("dispatch-{}", ATTEMPT.fetch_add(1, Ordering::SeqCst))
    } else {
        String::new()
    };
    if observed {
        fs::write(&format!("/{attempt}-begin"), b"begin", "").map_err(fs_fault)?;
    }
    let answer = jinn::plugin::events::emit(
        CHANGED_TOPIC,
        DispatchMode::Serial,
        &Selector::All,
        if observed {
            attempt.as_bytes()
        } else {
            b"changed"
        },
    );
    if observed {
        fs::write(&format!("/{attempt}-end"), b"end", "").map_err(fs_fault)?;
    }
    Ok((attempt, answer))
}

/// A real earlier delivery, before patching, makes the old whole-log and
/// whole-history assertions deterministically false. A callback may refuse
/// the wait cycle; receipt of the notice itself is the control's witness.
pub(super) fn control() -> Result<bool, GuestFault> {
    let (attempt, _) = emit(true)?;
    if fs::meta(&format!("/{attempt}-received")).is_err() {
        return Ok(false);
    }
    fs::write("/dispatch-control", attempt.as_bytes(), "").map_err(fs_fault)?;
    Ok(true)
}

pub(super) fn received(payload: &[u8]) -> Result<(), GuestFault> {
    let attempt = String::from_utf8_lossy(payload);
    if attempt
        .strip_prefix("dispatch-")
        .is_some_and(|id| !id.is_empty() && id.bytes().all(|byte| byte.is_ascii_digit()))
    {
        fs::write(&format!("/{attempt}-received"), b"notice", "").map_err(fs_fault)?;
    }
    Ok(())
}
