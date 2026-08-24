//! One frozen layer of the context tree.

use std::sync::Arc;

use jinnd_api::ContextId;

use crate::key::{KeyId, RealmId};

/// A layer holds the bindings one derivation added, and a pointer to its parent.
///
/// Own keys are fixed when the layer is built (the whole tree is frozen layers plus
/// parent pointers), so a lookup is a walk of `Arc` pointers with no synchronisation.
/// Own bindings are sorted by key, which keeps a lookup at a binary search over the
/// handful of keys one derivation binds.
#[derive(Debug)]
pub(crate) struct Layer<I> {
    pub(crate) id: ContextId,
    pub(crate) depth: u32,
    pub(crate) parent: Option<Arc<Layer<I>>>,
    isolation: Box<[(KeyId, RealmId)]>,
    intercept: Box<[(KeyId, I)]>,
}

impl<I> Layer<I> {
    /// Freezes one derivation's bindings into a layer.
    ///
    /// Both binding lists must already be deduplicated by key; [`crate::Derive`] is
    /// the only constructor and enforces last-binding-wins as it collects them.
    pub(crate) fn new(
        id: ContextId,
        parent: Option<Arc<Layer<I>>>,
        mut isolation: Vec<(KeyId, RealmId)>,
        mut intercept: Vec<(KeyId, I)>,
    ) -> Self {
        let depth = parent.as_ref().map_or(0, |parent| parent.depth + 1);
        isolation.sort_unstable_by_key(|(key, _)| *key);
        intercept.sort_unstable_by_key(|(key, _)| *key);
        Self {
            id,
            depth,
            parent,
            isolation: isolation.into_boxed_slice(),
            intercept: intercept.into_boxed_slice(),
        }
    }

    /// The realm this layer itself binds `key` to, if it binds it at all.
    pub(crate) fn own_realm(&self, key: KeyId) -> Option<RealmId> {
        self.isolation
            .binary_search_by_key(&key, |(bound, _)| *bound)
            .ok()
            .and_then(|index| self.isolation.get(index))
            .map(|(_, realm)| *realm)
    }

    /// The config overlay this layer itself carries for `key`, if any.
    pub(crate) fn own_intercept(&self, key: KeyId) -> Option<&I> {
        self.intercept
            .binary_search_by_key(&key, |(bound, _)| *bound)
            .ok()
            .and_then(|index| self.intercept.get(index))
            .map(|(_, value)| value)
    }
}

/// Frees the chain iteratively.
///
/// A layer owns its parent, so the derived `Drop` would recurse once per layer and
/// overflow the stack on a deep tree — an abort, which no kernel boundary can contain
/// (R11). Unlinking each parent before it is freed keeps the cost flat.
impl<I> Drop for Layer<I> {
    fn drop(&mut self) {
        let mut next = self.parent.take();
        while let Some(layer) = next {
            // Another handle still holds this layer: it owns the rest of the chain and
            // frees it the same way when its own last reference goes.
            let Ok(mut layer) = Arc::try_unwrap(layer) else {
                break;
            };
            next = layer.parent.take();
        }
    }
}

/// Iterator over the config overlays for one key, nearest first.
#[derive(Debug)]
pub struct InterceptChain<'a, I> {
    layer: Option<&'a Layer<I>>,
    key: KeyId,
}

impl<'a, I> InterceptChain<'a, I> {
    pub(crate) fn new(layer: &'a Layer<I>, key: KeyId) -> Self {
        Self {
            layer: Some(layer),
            key,
        }
    }
}

impl<'a, I> Iterator for InterceptChain<'a, I> {
    type Item = &'a I;

    fn next(&mut self) -> Option<&'a I> {
        while let Some(layer) = self.layer {
            self.layer = layer.parent.as_deref();
            if let Some(value) = layer.own_intercept(self.key) {
                return Some(value);
            }
        }
        None
    }
}
