//! The provider's inverse index (M2-K3; split from `hostfs.rs` by
//! responsibility, R10): what is retained per effect — a header, never a
//! prior content — and the seams that consume it: the keyed-revert action,
//! reclaim, teardown withdrawal, keyed exactly-once lookup, and the
//! durable-first registration itself.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use jinnd_api::{EffectId, ErrorCode, FiberId, KernelError, KernelFuture, Witness};

use super::retention::{Header, Prior, Record};
use super::{HostFs, Retained, UndoAction, ops};
use crate::broker_state::refusal;
use crate::lane::lock;

impl HostFs {
    /// The effect a non-empty idempotency `key` already recorded for
    /// `owner` (03 §Act): a replay answers this, mutating nothing.
    #[must_use]
    pub fn recorded(&self, owner: Option<FiberId>, key: &str) -> Option<EffectId> {
        if key.is_empty() {
            return None;
        }
        let owner = owner.map_or(0, |fiber| fiber.0);
        lock(&self.index)
            .iter()
            .find(|(_, retained)| retained.header.key == key && retained.header.owner == owner)
            .map(|(id, _)| EffectId(*id))
    }

    /// Every live (unconsumed) revertible effect, in id order: (id, scoped
    /// path).
    #[must_use]
    pub fn effects(&self) -> Vec<(EffectId, String)> {
        lock(&self.index)
            .iter()
            .filter(|(_, retained)| !retained.consumed)
            .map(|(id, retained)| (EffectId(*id), retained.header.label.clone()))
            .collect()
    }

    /// The in-memory index's footprint in bytes — headers and bookkeeping
    /// only, whatever the prior contents weighed (finding 8 bound).
    #[must_use]
    pub fn index_bytes(&self) -> usize {
        lock(&self.index)
            .values()
            .map(|retained| {
                retained.header.label.len()
                    + retained.header.key.len()
                    + std::mem::size_of::<Retained>()
                    + 8
            })
            .sum()
    }

    /// How many inverses are spilled in the retention store right now.
    #[must_use]
    pub fn spilled(&self) -> usize {
        self.store.spilled()
    }

    /// The keyed-revert action for one effect this provider owns (Law 3):
    /// the witness reads the file back against the spilled prior; the
    /// inverse restores prior content, absence, or length from the spill.
    /// A consumed effect still answers — with an inverse that refuses to
    /// run again (the ledger answers its replay from the record).
    #[must_use]
    pub fn undo_action(&self, effect: EffectId) -> Option<UndoAction> {
        let id = effect.0;
        let consumed = lock(&self.index).get(&id)?.consumed;
        if consumed {
            let witness: Witness = Arc::new(|| false);
            let inverse: Box<dyn FnOnce() -> KernelFuture<'static, ()> + Send> =
                Box::new(move || {
                    Box::pin(async move {
                        Err(refusal(
                            ErrorCode::EffectFailed,
                            format!("effect {id}'s inverse was already consumed"),
                        ))
                    })
                });
            return Some((witness, inverse));
        }
        let (witness_root, witness_store) = (self.root.clone(), self.store.clone());
        let witness: Witness = Arc::new(move || {
            witness_store
                .load_sync(id)
                .is_some_and(|record| ops::witness(&witness_root, &record))
        });
        let (root, store) = (self.root.clone(), self.store.clone());
        let inverse: Box<dyn FnOnce() -> KernelFuture<'static, ()> + Send> = Box::new(move || {
            Box::pin(async move {
                let record = store.load(id).await?;
                ops::apply_inverse(root, record).await
            })
        });
        Some((witness, inverse))
    }

    /// Consumes one reverted effect's inverse: its spilled storage is
    /// reclaimed and it leaves the live effect list. The assembly calls
    /// this after the ledger records the branch `Reverted`.
    ///
    /// # Errors
    ///
    /// An effect this provider does not own, or a storage refusal.
    pub async fn reclaim(&self, effect: EffectId) -> Result<(), KernelError> {
        let id = effect.0;
        if !lock(&self.index).contains_key(&id) {
            return Err(refusal(
                ErrorCode::EffectFailed,
                format!("no revertible effect {id}"),
            ));
        }
        self.store.reclaim(id).await?;
        if let Some(retained) = lock(&self.index).get_mut(&id) {
            retained.consumed = true;
        }
        Ok(())
    }

    /// The teardown withdrawal of one live effect (R5, LIFO through the
    /// owning seat's journal): runs the inverse from the spill, then
    /// reclaims. An already-consumed effect (a keyed revert got there
    /// first) withdraws clean; an unknown one is refused.
    ///
    /// # Errors
    ///
    /// An unknown effect, a failing inverse, or a storage refusal.
    pub async fn withdraw(&self, effect: EffectId) -> Result<(), KernelError> {
        let id = effect.0;
        match lock(&self.index).get(&id).map(|retained| retained.consumed) {
            None => {
                return Err(refusal(
                    ErrorCode::EffectFailed,
                    format!("no revertible effect {id}"),
                ));
            }
            Some(true) => return Ok(()),
            Some(false) => {}
        }
        let record = self.store.load(id).await?;
        ops::apply_inverse(self.root.clone(), record).await?;
        self.reclaim(effect).await
    }

    /// Registers one revertible effect: the inverse is made durable FIRST;
    /// only then may the caller mutate. Returns the effect id to label.
    pub(super) async fn retain(&self, header: Header, prior: Prior) -> Result<u64, KernelError> {
        let id = self.next.fetch_add(1, Ordering::SeqCst);
        let record = Record {
            header: header.clone(),
            prior,
        };
        self.store.persist(id, &record).await?;
        lock(&self.index).insert(
            id,
            Retained {
                header,
                consumed: false,
            },
        );
        Ok(id)
    }

    /// Drops a retained inverse whose mutation never happened (the io
    /// refused after the spill): nothing to revert, nothing to keep.
    pub(super) async fn release(&self, id: u64) {
        lock(&self.index).remove(&id);
        let _ = self.store.reclaim(id).await;
    }
}
