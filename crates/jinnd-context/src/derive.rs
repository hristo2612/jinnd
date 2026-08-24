//! Deriving a child context: one identity, one frozen layer.

use std::sync::Arc;

use jinnd_api::IsolationBinding;

use crate::context::Context;
use crate::key::{KeyId, RealmId};
use crate::layer::Layer;

/// Builder for one derived context: collects the bindings of a single frozen layer.
///
/// Nothing is allocated until [`Derive::build`], so binding several keys still costs
/// one layer and one identity — which is what keeps derivation O(1).
#[derive(Debug)]
pub struct Derive<'a, I> {
    parent: &'a Context<I>,
    isolation: Vec<(KeyId, RealmId)>,
    intercept: Vec<(KeyId, I)>,
}

impl<'a, I> Derive<'a, I> {
    pub(crate) fn new(parent: &'a Context<I>) -> Self {
        Self {
            parent,
            isolation: Vec::new(),
            intercept: Vec::new(),
        }
    }

    /// Maps one service to a realm, interning the binding's key and realm.
    #[must_use]
    pub fn bind(self, binding: &IsolationBinding) -> Self {
        let tree = self.parent.tree();
        let key = tree.key(&binding.service);
        let realm = tree.realm(&binding.realm);
        self.isolate(key, realm)
    }

    /// Maps several services to realms in one derivation.
    #[must_use]
    pub fn bind_all(self, bindings: &[IsolationBinding]) -> Self {
        bindings.iter().fold(self, Derive::bind)
    }

    /// Maps one already-interned service key to a realm.
    #[must_use]
    pub fn isolate(mut self, key: KeyId, realm: RealmId) -> Self {
        replace_or_push(&mut self.isolation, key, realm);
        self
    }

    /// Adds a config overlay for `key` over the derived subtree.
    #[must_use]
    pub fn intercept(mut self, key: KeyId, value: I) -> Self {
        replace_or_push(&mut self.intercept, key, value);
        self
    }

    /// Allocates the child context: one identity and one layer, whatever the depth of
    /// the tree.
    #[must_use]
    pub fn build(self) -> Context<I> {
        let layer = Layer::new(
            self.parent.allocate_id(),
            Some(Arc::clone(self.parent.layer_arc())),
            self.isolation,
            self.intercept,
        );
        self.parent.with_layer(Arc::new(layer))
    }
}

/// Binding a key twice in one derivation keeps the last value, matching the TS
/// original's dict assignment.
fn replace_or_push<V>(entries: &mut Vec<(KeyId, V)>, key: KeyId, value: V) {
    match entries.iter_mut().find(|(bound, _)| *bound == key) {
        Some(entry) => entry.1 = value,
        None => entries.push((key, value)),
    }
}
