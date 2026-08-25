//! The listener table: the one shared cell dispatch snapshots and claims.
//!
//! The cell is pure decision logic behind the [`crate::sync`] shim, so the loom
//! models in [`crate::models`] drive exactly the code dispatch runs. Every
//! operation takes the lock briefly and returns owned data: no guard survives
//! into listener code (R1), and removal doubles as the once-claim — under one
//! lock, at most one caller ever observes `true` for one registration.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use jinnd_api::ContextId;

use crate::sync::Mutex;

/// Identity of one listener registration within its bus. Never reused.
pub(crate) type ListenerId = u64;

/// One registered listener, type-erased under its event's `TypeId`.
///
/// `callable` holds an `Arc<dyn EventListener<E>>` for the `E` the entry is
/// keyed by; dispatch downcasts it back behind that key (R3: resolution is by
/// event type, the erasure is storage only).
#[derive(Clone)]
pub(crate) struct Entry {
    pub(crate) id: ListenerId,
    pub(crate) context: ContextId,
    pub(crate) once: bool,
    pub(crate) callable: Arc<dyn Any + Send + Sync>,
}

/// The bus's shared listener state.
pub(crate) struct ListenerTable {
    inner: Mutex<TableState>,
}

struct TableState {
    next: ListenerId,
    listeners: HashMap<TypeId, Vec<Entry>>,
}

impl ListenerTable {
    /// An empty table.
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(TableState {
                next: 0,
                listeners: HashMap::new(),
            }),
        }
    }

    /// Inserts one registration and returns its identity. The per-event vec
    /// order is registration order, which every walk preserves.
    pub(crate) fn insert(
        &self,
        event: TypeId,
        context: ContextId,
        once: bool,
        callable: Arc<dyn Any + Send + Sync>,
    ) -> ListenerId {
        self.with(|state| {
            let id = state.next;
            state.next += 1;
            state.listeners.entry(event).or_default().push(Entry {
                id,
                context,
                once,
                callable,
            });
            id
        })
    }

    /// Removes one registration; `false` when it was already gone.
    ///
    /// This is both the effect's undo and the once-claim: idempotent by
    /// construction, and exactly-once by the lock's mutual exclusion.
    pub(crate) fn remove(&self, event: TypeId, id: ListenerId) -> bool {
        self.with(|state| {
            let Some(entries) = state.listeners.get_mut(&event) else {
                return false;
            };
            let before = entries.len();
            entries.retain(|entry| entry.id != id);
            entries.len() < before
        })
    }

    /// Clones the current registration list for one walk.
    ///
    /// Dispatch never iterates the live map (R1): a listener registered after
    /// this snapshot is not part of the walk, and one removed after it may
    /// still receive this walk's payload — snapshot semantics, stated at the
    /// bus boundary.
    pub(crate) fn snapshot(&self, event: TypeId) -> Vec<Entry> {
        self.with(|state| state.listeners.get(&event).cloned().unwrap_or_default())
    }

    fn with<T>(&self, change: impl FnOnce(&mut TableState) -> T) -> T {
        let mut state = self
            .inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        change(&mut state)
    }
}

impl std::fmt::Debug for ListenerTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ListenerTable").finish_non_exhaustive()
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use std::any::TypeId;
    use std::sync::Arc;

    use jinnd_api::ContextId;

    use super::ListenerTable;

    struct Marker;

    fn key() -> TypeId {
        TypeId::of::<Marker>()
    }

    #[test]
    fn insert_snapshot_preserves_registration_order() {
        let table = ListenerTable::new();
        let first = table.insert(key(), ContextId(1), false, Arc::new(()));
        let second = table.insert(key(), ContextId(2), false, Arc::new(()));

        let snapshot = table.snapshot(key());
        assert_eq!(
            snapshot.iter().map(|entry| entry.id).collect::<Vec<_>>(),
            vec![first, second]
        );
        assert_eq!(snapshot[0].context, ContextId(1));
    }

    #[test]
    fn remove_is_idempotent_and_scoped_to_one_registration() {
        let table = ListenerTable::new();
        let first = table.insert(key(), ContextId(1), false, Arc::new(()));
        let second = table.insert(key(), ContextId(1), false, Arc::new(()));

        assert!(table.remove(key(), first));
        assert!(!table.remove(key(), first), "a second removal is a no-op");
        assert_eq!(table.snapshot(key()).len(), 1);
        assert!(table.remove(key(), second));
        assert!(table.snapshot(key()).is_empty());
    }

    #[test]
    fn snapshot_of_an_unknown_event_is_empty() {
        let table = ListenerTable::new();
        assert!(table.snapshot(key()).is_empty());
        assert!(!table.remove(key(), 0));
    }

    #[test]
    fn identities_are_never_reused() {
        let table = ListenerTable::new();
        let first = table.insert(key(), ContextId(1), false, Arc::new(()));
        assert!(table.remove(key(), first));
        let second = table.insert(key(), ContextId(1), false, Arc::new(()));
        assert_ne!(first, second);
    }
}
