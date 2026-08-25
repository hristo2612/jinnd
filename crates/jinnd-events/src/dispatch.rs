//! The five mode walks over one snapshot of the listener table.

use std::any::TypeId;
use std::sync::Arc;

use jinnd_api::{
    ContextId, DispatchMode, DispatchReport, ErrorCode, Event, EventListener, KernelError,
};

use crate::contain::{catching, contained};
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
/// listener call and every payload-owned routine (`selects`, `decisive`,
/// `absorb`) runs contained (R11); failures are recorded and never abort a
/// collecting walk (R9).
pub(crate) async fn walk<E: Event>(
    table: &ListenerTable,
    caller: ContextId,
    mut event: E,
) -> DispatchReport<E> {
    let mut outputs = Vec::new();
    let mut failures = Vec::new();
    let selected = select::<E>(table, &event, &mut failures);

    match E::MODE {
        DispatchMode::Emit => {
            for selection in selected {
                if !claim::<E>(table, &selection) {
                    continue;
                }
                let listener = selection.listener;
                // Notify all, results ignored (LAW §3) — only failures are kept.
                if let Err(error) = contained(|| listener.call(caller, event.clone())).await {
                    failures.push(error);
                }
            }
        }
        DispatchMode::Parallel => {
            parallel(table, caller, &event, selected, &mut outputs, &mut failures).await;
        }
        DispatchMode::Serial => {
            for selection in selected {
                if !claim::<E>(table, &selection) {
                    continue;
                }
                let listener = selection.listener;
                match contained(|| listener.call(caller, event.clone())).await {
                    Ok(output) => outputs.push(output),
                    Err(error) => failures.push(error),
                }
            }
        }
        DispatchMode::Bail => {
            for selection in selected {
                if !claim::<E>(table, &selection) {
                    continue;
                }
                let listener = selection.listener;
                // The resolved value is what gets judged: a pending async
                // result was awaited, never counted as bailed, and an error is
                // never a decisive value (R9).
                match contained(|| listener.call(caller, event.clone())).await {
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
        }
        DispatchMode::Waterfall => {
            for selection in selected {
                if !claim::<E>(table, &selection) {
                    continue;
                }
                let listener = selection.listener;
                match contained(|| listener.call(caller, event.clone())).await {
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
        }
    }

    DispatchReport {
        event,
        outputs,
        failures,
    }
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
        match catching(|| event.selects(entry.context)) {
            Ok(true) => {}
            Ok(false) => continue,
            Err(error) => {
                failures.push(error);
                continue;
            }
        }
        // Entries are stored under their event's `TypeId`, so this downcast is
        // total; an impossible mismatch drops the entry rather than the walk.
        let Some(listener) = entry
            .callable
            .downcast_ref::<Arc<dyn EventListener<E>>>()
            .cloned()
        else {
            continue;
        };
        selected.push(Selection {
            id: entry.id,
            once: entry.once,
            listener,
        });
    }
    selected
}

/// Claims a selection immediately before its call: a once-registration is
/// withdrawn under the table's lock, so of any concurrent dispatches exactly
/// one delivers it — and a walk that stops earlier never consumes it.
fn claim<E: Event>(table: &ListenerTable, selection: &Selection<E>) -> bool {
    !selection.once || table.remove(TypeId::of::<E>(), selection.id)
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
                if !claim::<E>(table, &selection) {
                    continue;
                }
                let listener = selection.listener;
                let event = event.clone();
                tasks.push(
                    handle.spawn(async move { contained(|| listener.call(caller, event)).await }),
                );
            }
            for task in tasks {
                match task.await {
                    Ok(Ok(output)) => outputs.push(output),
                    Ok(Err(error)) => failures.push(error),
                    // In-task containment already converts panics; only a
                    // cancelled task reaches this arm, and it is still one
                    // listener's local failure (R11).
                    Err(_) => failures.push(KernelError {
                        code: ErrorCode::ListenerFailed,
                        message: "the listener task was cancelled before it settled".to_owned(),
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
                if !claim::<E>(table, &selection) {
                    continue;
                }
                let listener = selection.listener;
                match contained(|| listener.call(caller, event.clone())).await {
                    Ok(output) => outputs.push(output),
                    Err(error) => failures.push(error),
                }
            }
        }
    }
}
