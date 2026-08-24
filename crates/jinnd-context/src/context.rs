//! The tree arena and the context handle.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use jinnd_api::{ContextId, Realm, ServiceContract};

use crate::derive::Derive;
use crate::key::{KeyId, KeyTable, RealmId, RealmTable};
use crate::layer::{InterceptChain, Layer};

#[derive(Debug)]
struct TreeInner<I> {
    next_id: AtomicU64,
    keys: RwLock<KeyTable>,
    realms: RwLock<RealmTable>,
    root: Arc<Layer<I>>,
}

/// Shared arena for one context tree: identity allocation and the two interners.
///
/// `I` is the type of a config overlay carried by the intercept chain. The kernel is
/// generic over it so no contract config is erased to `Any` at this boundary (R3).
#[derive(Debug)]
pub struct ContextTree<I = ()> {
    inner: Arc<TreeInner<I>>,
}

impl<I> ContextTree<I> {
    /// Creates a tree holding only its root context.
    #[must_use]
    pub fn new() -> Self {
        let root = Arc::new(Layer::new(ContextId(0), None, Vec::new(), Vec::new()));
        Self {
            inner: Arc::new(TreeInner {
                next_id: AtomicU64::new(1),
                keys: RwLock::new(KeyTable::default()),
                realms: RwLock::new(RealmTable::new()),
                root,
            }),
        }
    }

    /// The tree's root context, which binds nothing and has no parent.
    #[must_use]
    pub fn root(&self) -> Context<I> {
        Context {
            tree: Arc::clone(&self.inner),
            layer: Arc::clone(&self.inner.root),
        }
    }

    /// Interns a service key from the dynamic (profile) lane.
    #[must_use]
    pub fn key(&self, name: &str) -> KeyId {
        write(&self.inner.keys).intern(name)
    }

    /// Interns the key of a typed contract; the same slot as [`ContextTree::key`] of
    /// its `NAME`.
    #[must_use]
    pub fn key_of<S: ServiceContract>(&self) -> KeyId {
        self.key(S::NAME)
    }

    /// The name `key` was interned from, for diagnostics.
    #[must_use]
    pub fn key_name(&self, key: KeyId) -> Option<String> {
        read(&self.inner.keys).name(key).map(ToOwned::to_owned)
    }

    /// Interns a realm. Equal realms intern to one [`RealmId`].
    #[must_use]
    pub fn realm(&self, realm: &Realm) -> RealmId {
        write(&self.inner.realms).intern(realm)
    }

    /// The realm `id` was interned from, for diagnostics and config write-back.
    #[must_use]
    pub fn realm_value(&self, id: RealmId) -> Option<Realm> {
        read(&self.inner.realms).value(id).cloned()
    }
}

impl<I> Default for ContextTree<I> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I> Clone for ContextTree<I> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// A layered view over the tree: the isolation map and intercept chain visible at one
/// point in it.
///
/// Cloning is two atomic increments; [`Context::derive`] allocates one layer.
#[derive(Debug)]
pub struct Context<I = ()> {
    tree: Arc<TreeInner<I>>,
    layer: Arc<Layer<I>>,
}

impl<I> Context<I> {
    /// This context's stable identity within its tree.
    #[must_use]
    pub fn id(&self) -> ContextId {
        self.layer.id
    }

    /// Number of derivations between this context and the root.
    #[must_use]
    pub fn depth(&self) -> u32 {
        self.layer.depth
    }

    /// Whether this is the tree's root context.
    #[must_use]
    pub fn is_root(&self) -> bool {
        self.layer.parent.is_none()
    }

    /// The context this one was derived from, or `None` at the root.
    #[must_use]
    pub fn parent(&self) -> Option<Context<I>> {
        self.layer.parent.as_ref().map(|parent| Context {
            tree: Arc::clone(&self.tree),
            layer: Arc::clone(parent),
        })
    }

    /// This context's ancestors, nearest first, ending at the root.
    pub fn ancestors(&self) -> impl Iterator<Item = Context<I>> {
        std::iter::successors(self.parent(), Context::parent)
    }

    /// Whether `other` is a strict descendant of this context in the same tree.
    #[must_use]
    pub fn is_ancestor_of(&self, other: &Context<I>) -> bool {
        Arc::ptr_eq(&self.tree, &other.tree)
            && other.depth() > self.depth()
            && other.ancestors().any(|ancestor| ancestor.id() == self.id())
    }

    /// The tree this context belongs to.
    #[must_use]
    pub fn tree(&self) -> ContextTree<I> {
        ContextTree {
            inner: Arc::clone(&self.tree),
        }
    }

    /// Begins deriving a child context. The child is allocated on
    /// [`Derive::build`], with exactly the bindings collected until then.
    #[must_use]
    pub fn derive(&self) -> Derive<'_, I> {
        Derive::new(self)
    }

    /// The realm this context resolves `key` in: the nearest binding on the layer
    /// chain, or [`RealmId::ROOT`] when no layer binds it.
    #[must_use]
    pub fn realm_of(&self, key: KeyId) -> RealmId {
        let mut layer = self.layer.as_ref();
        loop {
            if let Some(realm) = layer.own_realm(key) {
                return realm;
            }
            match layer.parent.as_deref() {
                Some(parent) => layer = parent,
                None => return RealmId::ROOT,
            }
        }
    }

    /// The realm binding this context's own layer added, if it added one.
    #[must_use]
    pub fn own_realm(&self, key: KeyId) -> Option<RealmId> {
        self.layer.own_realm(key)
    }

    /// The config overlay in effect for `key`: the nearest one on the layer chain.
    #[must_use]
    pub fn intercept_of(&self, key: KeyId) -> Option<&I> {
        self.intercept_chain(key).next()
    }

    /// Every config overlay for `key` on the layer chain, nearest first.
    ///
    /// Interception is right-biased: a fold over this chain must let the first item
    /// win, which is what makes a redundant ancestor overlay inert.
    #[must_use]
    pub fn intercept_chain(&self, key: KeyId) -> InterceptChain<'_, I> {
        InterceptChain::new(self.layer.as_ref(), key)
    }

    pub(crate) fn allocate_id(&self) -> ContextId {
        ContextId(self.tree.next_id.fetch_add(1, Ordering::Relaxed))
    }

    pub(crate) fn layer_arc(&self) -> &Arc<Layer<I>> {
        &self.layer
    }

    pub(crate) fn with_layer(&self, layer: Arc<Layer<I>>) -> Context<I> {
        Context {
            tree: Arc::clone(&self.tree),
            layer,
        }
    }

    pub(crate) fn key_name(&self, key: KeyId) -> Option<String> {
        read(&self.tree.keys).name(key).map(ToOwned::to_owned)
    }
}

impl<I> Clone for Context<I> {
    fn clone(&self) -> Self {
        Self {
            tree: Arc::clone(&self.tree),
            layer: Arc::clone(&self.layer),
        }
    }
}

impl<I> PartialEq for Context<I> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.tree, &other.tree) && self.id() == other.id()
    }
}

impl<I> Eq for Context<I> {}

/// Lock helpers that recover from poisoning instead of panicking (R11): a poisoned
/// interner still holds valid, append-only data.
fn read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poison| poison.into_inner())
}

fn write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(|poison| poison.into_inner())
}

#[cfg(test)]
mod tests {
    use super::{Context, ContextTree};

    fn assert_send_sync<T: Send + Sync>() {}

    /// Handles cross task boundaries, so the kernel's supervisor tasks can hold them
    /// (R1).
    #[test]
    fn context_handles_are_send_and_sync() {
        assert_send_sync::<Context<()>>();
        assert_send_sync::<ContextTree<()>>();
    }
}
