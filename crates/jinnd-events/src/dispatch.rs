//! The five mode walks over one snapshot of the listener table.

use std::any::TypeId;
use std::sync::Arc;

use jinnd_api::{
    ContextId, DispatchMode, DispatchReport, ErrorCode, Event, EventListener, KernelError,
};

use crate::contain::{catching, contained, releasing};
use crate::table::{ListenerId, ListenerTable};

/// One selected listener, waiting for its claim and call.
struct Selection<E: Event> {
    id: ListenerId,
    once: bool,
    listener: Arc<dyn EventListener<E>>,
}

/// Dispatches one payload per its type-declared mode.
///
/// The listener set is snapshotted before the walk (R1): registration during
/// dispatch neither deadlocks nor joins this walk, and a removal during
/// dispatch may still see this walk's payload — snapshot semantics. Every
/// listener call, every payload-owned routine (`selects`, `decisive`,
/// `absorb`, `clone`), and every possibly-final handle release runs contained
/// (R11); failures are recorded and never abort a collecting walk (R9).
/// Answers the settled report and how many listeners the payload selected —
/// the trace tap's listener count (M2-K2), observed at the one place the
/// selection exists whatever the mode does with it (bail and waterfall may
/// stop before reaching every selected listener).
pub(crate) async fn walk<E: Event>(
    table: &ListenerTable,
    caller: ContextId,
    mut event: E,
) -> (DispatchReport<E>, usize) {
    let mut outputs = Vec::new();
    let mut failures = Vec::new();
    let selected = select::<E>(table, &event, &mut failures);
    let chosen = selected.len();

    match E::MODE {
        DispatchMode::Emit => {
            for selection in selected {
                let Some(listener) = claim(table, selection, &mut failures) else {
                    continue;
                };
                // Notify all, results ignored (LAW §3) — only failures are kept.
                if let Err(error) = contained(|| listener.call(caller, event.clone())).await {
                    failures.push(error);
                }
                release(listener, &mut failures);
            }
        }
        DispatchMode::Parallel => {
            parallel(table, caller, &event, selected, &mut outputs, &mut failures).await;
        }
        DispatchMode::Serial => {
            for selection in selected {
                let Some(listener) = claim(table, selection, &mut failures) else {
                    continue;
                };
                match contained(|| listener.call(caller, event.clone())).await {
                    Ok(output) => outputs.push(output),
                    Err(error) => failures.push(error),
                }
                release(listener, &mut failures);
            }
        }
        DispatchMode::Bail => {
            let mut rest = selected.into_iter();
            for selection in rest.by_ref() {
                let Some(listener) = claim(table, selection, &mut failures) else {
                    continue;
                };
                // The resolved value is what gets judged: a pending async
                // result was awaited, never counted as bailed, and an error is
                // never a decisive value (R9).
                let outcome = contained(|| listener.call(caller, event.clone())).await;
                release(listener, &mut failures);
                match outcome {
                    Ok(output) => match catching(|| event.decisive(&output)) {
                        Ok(true) => {
                            outputs.push(output);
                            break;
                        }
                        Ok(false) => {}
                        Err(error) => failures.push(error),
                    },
                    Err(error) => failures.push(error),
                }
            }
            discard(rest, &mut failures);
        }
        DispatchMode::Waterfall => {
            let mut rest = selected.into_iter();
            for selection in rest.by_ref() {
                let Some(listener) = claim(table, selection, &mut failures) else {
                    continue;
                };
                let outcome = contained(|| listener.call(caller, event.clone())).await;
                release(listener, &mut failures);
                match outcome {
                    // A failing listener contributes nothing and the walk
                    // continues (R9); a panicking `absorb` stops it — the
                    // accumulator can no longer be trusted mid-mutation.
                    Ok(output) => match catching(|| event.absorb(output)) {
                        Ok(true) => {}
                        Ok(false) => break,
                        Err(error) => {
                            failures.push(error);
                            break;
                        }
                    },
                    Err(error) => failures.push(error),
                }
            }
            discard(rest, &mut failures);
        }
    }

    (
        DispatchReport {
            event,
            outputs,
            failures,
        },
        chosen,
    )
}

/// Interrogates each snapshotted listener's registration context with the
/// payload's filter (inverted routing, LAW §3). A panicking filter is contained:
/// the failure is recorded and that listener is skipped (R11).
fn select<E: Event>(
    table: &ListenerTable,
    event: &E,
    failures: &mut Vec<KernelError>,
) -> Vec<Selection<E>> {
    let mut selected = Vec::new();
    for entry in table.snapshot(TypeId::of::<E>()) {
        let listener = match catching(|| event.selects(entry.context)) {
            // Entries are stored under their event's `TypeId`, so this
            // downcast is total; an impossible mismatch drops the entry
            // rather than the walk.
            Ok(true) => entry
                .callable
                .downcast_ref::<Arc<dyn EventListener<E>>>()
                .cloned(),
            Ok(false) => None,
            Err(error) => {
                failures.push(error);
                None
            }
        };
        let (id, once) = (entry.id, entry.once);
        // The snapshot's handle can be the final one when a concurrent
        // dispatch claims this registration mid-walk; its drop stays contained
        // like every plugin destructor (R11).
        if let Err(error) = releasing(entry) {
            failures.push(error);
        }
        if let Some(listener) = listener {
            selected.push(Selection { id, once, listener });
        }
    }
    selected
}

/// Claims a selection immediately before its call: a once-registration is
/// withdrawn under the table's lock, so of any concurrent dispatches exactly
/// one delivers it — and a walk that stops earlier never consumes it. `None`
/// when another dispatch already consumed it; the handle is released in place.
fn claim<E: Event>(
    table: &ListenerTable,
    selection: Selection<E>,
    failures: &mut Vec<KernelError>,
) -> Option<Arc<dyn EventListener<E>>> {
    let Selection { id, once, listener } = selection;
    if once && !table.remove(TypeId::of::<E>(), id) {
        release(listener, failures);
        return None;
    }
    Some(listener)
}

/// Releases one listener handle behind containment: for a claimed
/// once-registration this is the final handle, so the destructor that runs is
/// plugin code (R11) — its panic is recorded, never unwound (R9).
fn release<E: Event>(listener: Arc<dyn EventListener<E>>, failures: &mut Vec<KernelError>) {
    if let Err(error) = releasing(listener) {
        failures.push(error);
    }
}

/// Releases the selections a stopped walk never reached, without claiming
/// them: an unconsumed once-registration stays live for the next dispatch.
fn discard<E: Event>(rest: impl Iterator<Item = Selection<E>>, failures: &mut Vec<KernelError>) {
    for selection in rest {
        release(selection.listener, failures);
    }
}

/// The parallel walk: every claimed listener runs concurrently, every one
/// settles, outputs land in registration order and failures aggregate (R9).
async fn parallel<E: Event>(
    table: &ListenerTable,
    caller: ContextId,
    event: &E,
    selected: Vec<Selection<E>>,
    outputs: &mut Vec<E::Output>,
    failures: &mut Vec<KernelError>,
) {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => {
            let mut tasks = Vec::new();
            for selection in selected {
                // The payload's `Clone` is plugin-authored: it runs contained,
                // and a failing clone is recorded as that listener's failure
                // before the claim, so a once-registration stays live (R11).
                let payload = match catching(|| event.clone()) {
                    Ok(payload) => payload,
                    Err(error) => {
                        failures.push(error);
                        release(selection.listener, failures);
                        continue;
                    }
                };
                let Some(listener) = claim(table, selection, failures) else {
                    if let Err(error) = releasing(payload) {
                        failures.push(error);
                    }
                    continue;
                };
                tasks.push(handle.spawn(async move {
                    let result = contained(|| listener.call(caller, payload)).await;
                    // The handle is released inside the task so its possibly
                    // plugin-authored destructor is contained and reported,
                    // never left to the task boundary (R11).
                    (result, releasing(listener).err())
                }));
            }
            for task in tasks {
                match task.await {
                    Ok((result, released)) => {
                        match result {
                            Ok(output) => outputs.push(output),
                            Err(error) => failures.push(error),
                        }
                        if let Some(error) = released {
                            failures.push(error);
                        }
                    }
                    // In-task containment already converts panics; this arm is
                    // a cancelled or torn-down task, and it is still one
                    // listener's local failure (R11).
                    Err(join) => failures.push(KernelError {
                        code: ErrorCode::ListenerFailed,
                        message: if join.is_panic() {
                            "the listener task ended in an uncontained panic".to_owned()
                        } else {
                            "the listener task was cancelled before it settled".to_owned()
                        },
                        fiber: None,
                    }),
                }
            }
        }
        // No runtime to schedule on: every listener still runs and settles,
        // sequentially — the gathered contract is identical, only the overlap
        // is lost. The kernel proper always dispatches inside its runtime (R1).
        Err(_) => {
            for selection in selected {
                let Some(listener) = claim(table, selection, failures) else {
                    continue;
                };
                match contained(|| listener.call(caller, event.clone())).await {
                    Ok(output) => outputs.push(output),
                    Err(error) => failures.push(error),
                }
                release(listener, failures);
            }
        }
    }
}
