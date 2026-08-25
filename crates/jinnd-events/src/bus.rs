//! The bus surface: typed registration and mode-declared dispatch.

use std::any::TypeId;
use std::sync::Arc;

use jinnd_api::{ContextId, DispatchReport, Event, EventListener};

use crate::dispatch;
use crate::table::{ListenerId, ListenerTable};

/// One event bus.
///
/// Cloning shares the bus: handles are cheap and every clone sees the same
/// listener table.
#[derive(Clone)]
pub struct EventBus {
    table: Arc<ListenerTable>,
}

impl EventBus {
    /// An empty bus.
    #[must_use]
    pub fn new() -> Self {
        Self {
            table: Arc::new(ListenerTable::new()),
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
    /// listener outcome (R9: failures are observed, never aborted on).
    pub async fn dispatch<E: Event>(&self, caller: ContextId, event: E) -> DispatchReport<E> {
        dispatch::walk(&self.table, caller, event).await
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
