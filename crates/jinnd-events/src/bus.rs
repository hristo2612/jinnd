//! The bus surface: typed registration and mode-declared dispatch.

use std::any::TypeId;
use std::sync::Arc;

use jinnd_api::{ContextId, DispatchMode, DispatchReport, Event, EventListener};

use crate::dispatch;
use crate::table::{ListenerId, ListenerTable};

/// One emit's dispatch trace (M2-K2; Law 2): what the ledger's
/// `DispatchTrace` event records — topic, declared mode, how many listeners
/// the payload selected, how many contained failures the walk observed (R9),
/// and the emitting context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DispatchTraceRecord {
    /// The typed event's type name — the typed lane's topic.
    pub topic: &'static str,
    pub mode: DispatchMode,
    pub listeners: usize,
    pub failures: usize,
    pub emitter: ContextId,
}

/// Where dispatch traces land (M2-K2). The bus stays a pure bus (R10): the
/// sink is the seam the assembly glues to its ledger. An implementation must
/// be fire-and-forget — it is called after the walk settled, never holds up
/// or alters dispatch outcomes, and swallows its own storage refusals via
/// the ledger's honesty path (R11).
pub trait TraceSink: Send + Sync + 'static {
    fn trace(&self, record: DispatchTraceRecord);
}

/// One event bus.
///
/// Cloning shares the bus: handles are cheap and every clone sees the same
/// listener table.
#[derive(Clone)]
pub struct EventBus {
    table: Arc<ListenerTable>,
    trace: Option<Arc<dyn TraceSink>>,
}

impl EventBus {
    /// An empty bus.
    #[must_use]
    pub fn new() -> Self {
        Self {
            table: Arc::new(ListenerTable::new()),
            trace: None,
        }
    }

    /// An empty bus whose every dispatch lands one trace on `sink` (M2-K2;
    /// Law 2: bus emits are kernel-boundary events).
    #[must_use]
    pub fn traced(sink: Arc<dyn TraceSink>) -> Self {
        Self {
            table: Arc::new(ListenerTable::new()),
            trace: Some(sink),
        }
    }

    /// Registers a typed listener and returns its removal handle.
    ///
    /// Registration is just an effect (R5): the caller wraps the returned
    /// [`Registration`] as the undo it registers at the same boundary. With
    /// `once`, the kernel withdraws the registration itself, at most once,
    /// immediately before the listener's first selected delivery.
    pub fn listen<E: Event, L: EventListener<E>>(
        &self,
        context: ContextId,
        listener: L,
        once: bool,
    ) -> Registration {
        let callable: Arc<dyn EventListener<E>> = Arc::new(listener);
        let id = self
            .table
            .insert(TypeId::of::<E>(), context, once, Arc::new(callable));
        Registration {
            table: Arc::clone(&self.table),
            event: TypeId::of::<E>(),
            id,
        }
    }

    /// Dispatches one payload per its type-declared mode and reports every
    /// listener outcome (R9: failures are observed, never aborted on). With
    /// a trace sink, exactly one trace lands after the walk settled — the
    /// append is fire-and-forget relative to the walk and never changes the
    /// report (M2-K2; R11).
    pub async fn dispatch<E: Event>(&self, caller: ContextId, event: E) -> DispatchReport<E> {
        let (report, listeners) = dispatch::walk(&self.table, caller, event).await;
        if let Some(sink) = &self.trace {
            sink.trace(DispatchTraceRecord {
                topic: std::any::type_name::<E>(),
                mode: E::MODE,
                listeners,
                failures: report.failures.len(),
                emitter: caller,
            });
        }
        report
    }
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for EventBus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventBus").finish_non_exhaustive()
    }
}

/// The idempotent removal handle for one registration.
///
/// Removal by value or by replayed undo lands on the same table operation:
/// whichever runs second observes `false` and changes nothing.
#[derive(Clone, Debug)]
pub struct Registration {
    table: Arc<ListenerTable>,
    event: TypeId,
    id: ListenerId,
}

impl Registration {
    /// Removes the registration; `false` when it was already gone.
    pub fn remove(&self) -> bool {
        self.table.remove(self.event, self.id)
    }
}
