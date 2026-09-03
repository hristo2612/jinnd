//! Flattening one guest call (split from `instance.rs` by responsibility,
//! R10): deadline, trap, and guest fault each become a contained kernel
//! error (R11); deadline and trap also end the instance.

use std::time::Duration;

use jinnd_api::{ErrorCode, KernelError};
use tokio::sync::watch;
use tokio::time::{Instant, Sleep, sleep};

use crate::bindings::lifecycle;

#[cfg(test)]
mod tests;

pub(crate) fn trapped(trap: &wasmtime::Error) -> KernelError {
    KernelError {
        code: ErrorCode::PluginFailed,
        message: format!("guest trapped: {trap:#}"),
        fiber: None,
    }
}

fn delivery_trap(trap: &wasmtime::Error, budgeted: bool) -> KernelError {
    if budgeted
        && matches!(
            trap.downcast_ref::<wasmtime::Trap>(),
            Some(wasmtime::Trap::OutOfFuel)
        )
    {
        return KernelError {
            code: ErrorCode::PluginFailed,
            message: "guest exhausted its delivery fuel budget".to_owned(),
            fiber: None,
        };
    }
    trapped(trap)
}

pub(crate) fn hung() -> KernelError {
    KernelError {
        code: ErrorCode::PluginFailed,
        message: "guest exceeded its call deadline".to_owned(),
        fiber: None,
    }
}

pub(crate) fn faulted(fault: lifecycle::GuestFault) -> KernelError {
    let lifecycle::GuestFault::Failed(message) = fault;
    KernelError {
        code: ErrorCode::PluginFailed,
        message,
        fiber: None,
    }
}

/// Flattens one guest call: deadline, trap, and guest fault each become a
/// contained kernel error; deadline and trap also end the instance.
pub(crate) enum Settled<T> {
    Ok(T),
    Fault(KernelError),
    Dead(KernelError),
}

/// One instance's guest-call clock. Host imports park it around event walks,
/// so a listener never spends its emitter's containment horizon (M2-K25(a)).
#[derive(Clone, Debug)]
pub(crate) struct DeadlineControl {
    parked: watch::Sender<u64>,
}

impl DeadlineControl {
    pub(crate) fn new() -> Self {
        Self {
            parked: watch::Sender::new(0),
        }
    }

    pub(crate) fn park(&self) -> DeadlinePark {
        self.parked.send_modify(|depth| *depth += 1);
        DeadlinePark {
            control: self.clone(),
        }
    }
}

/// RAII resume: cancellation of an emit future cannot leave its instance's
/// next guest call permanently parked.
pub(crate) struct DeadlinePark {
    control: DeadlineControl,
}

impl Drop for DeadlinePark {
    fn drop(&mut self) {
        self.control
            .parked
            .send_modify(|depth| *depth = depth.saturating_sub(1));
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DeadlineElapsed;

/// Runs `call` under active guest time only. A nested park extends the sleep
/// once for the union of the parked span, never once per nesting level.
pub(crate) async fn within<T>(
    deadline: Duration,
    control: &DeadlineControl,
    call: impl Future<Output = T>,
) -> Result<T, DeadlineElapsed> {
    let mut parked = control.parked.subscribe();
    let mut since = (*parked.borrow() > 0).then(Instant::now);
    let mut timer = Box::pin(sleep(deadline));
    let mut call = Box::pin(call);
    loop {
        tokio::select! {
            biased;
            changed = parked.changed() => {
                debug_assert!(changed.is_ok(), "the call retains its deadline sender");
                let depth = *parked.borrow_and_update();
                match (since, depth) {
                    (None, 1..) => since = Some(Instant::now()),
                    (Some(started), 0) => {
                        let resume_at = timer.deadline() + started.elapsed();
                        Sleep::reset(timer.as_mut(), resume_at);
                        since = None;
                    }
                    _ => {}
                }
            }
            value = &mut call => return Ok(value),
            () = &mut timer, if since.is_none() => return Err(DeadlineElapsed),
        }
    }
}

pub(crate) async fn settle<T>(
    deadline: Duration,
    control: &DeadlineControl,
    call: impl Future<Output = wasmtime::Result<Result<T, lifecycle::GuestFault>>>,
) -> Settled<T> {
    settle_call(deadline, control, call, false).await
}

pub(crate) async fn settle_delivery<T>(
    deadline: Duration,
    control: &DeadlineControl,
    budgeted: bool,
    call: impl Future<Output = wasmtime::Result<Result<T, lifecycle::GuestFault>>>,
) -> Settled<T> {
    settle_call(deadline, control, call, budgeted).await
}

async fn settle_call<T>(
    deadline: Duration,
    control: &DeadlineControl,
    call: impl Future<Output = wasmtime::Result<Result<T, lifecycle::GuestFault>>>,
    budgeted: bool,
) -> Settled<T> {
    match within(deadline, control, call).await {
        Err(_) => Settled::Dead(hung()),
        Ok(Err(trap)) => Settled::Dead(delivery_trap(&trap, budgeted)),
        Ok(Ok(Err(fault))) => Settled::Fault(faulted(fault)),
        Ok(Ok(Ok(value))) => Settled::Ok(value),
    }
}
