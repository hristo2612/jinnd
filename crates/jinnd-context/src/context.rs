use std::sync::Arc;

use jinnd_api::{ContextId, IsolationBinding, Realm, ServiceContract};

use crate::key::{KeyId, RealmId};
use crate::layer::Layer;

#[derive(Debug)]
pub struct ContextTree<I = ()> {
    _marker: std::marker::PhantomData<fn() -> I>,
}

impl<I> ContextTree<I> {
    #[must_use]
    pub fn new() -> Self {
        todo!()
    }

    #[must_use]
    pub fn root(&self) -> Context<I> {
        todo!()
    }

    #[must_use]
    pub fn key(&self, _name: &str) -> KeyId {
        todo!()
    }

    #[must_use]
    pub fn key_of<S: ServiceContract>(&self) -> KeyId {
        todo!()
    }

    #[must_use]
    pub fn key_name(&self, _key: KeyId) -> Option<String> {
        todo!()
    }

    #[must_use]
    pub fn realm(&self, _realm: &Realm) -> RealmId {
        todo!()
    }

    #[must_use]
    pub fn realm_value(&self, _realm: RealmId) -> Option<Realm> {
        todo!()
    }
}

#[derive(Debug)]
pub struct Context<I = ()> {
    _marker: std::marker::PhantomData<fn() -> I>,
}

impl<I> Context<I> {
    #[must_use]
    pub fn id(&self) -> ContextId {
        todo!()
    }

    #[must_use]
    pub fn depth(&self) -> u32 {
        todo!()
    }

    #[must_use]
    pub fn is_root(&self) -> bool {
        todo!()
    }

    #[must_use]
    pub fn parent(&self) -> Option<Context<I>> {
        todo!()
    }

    pub fn ancestors(&self) -> impl Iterator<Item = Context<I>> {
        std::iter::successors(self.parent(), Context::parent)
    }

    #[must_use]
    pub fn is_ancestor_of(&self, _other: &Context<I>) -> bool {
        todo!()
    }

    #[must_use]
    pub fn derive(&self) -> Derive<'_, I> {
        todo!()
    }

    #[must_use]
    pub fn realm_of(&self, _key: KeyId) -> RealmId {
        todo!()
    }

    #[must_use]
    pub fn own_realm(&self, _key: KeyId) -> Option<RealmId> {
        todo!()
    }

    #[must_use]
    pub fn intercept_of(&self, _key: KeyId) -> Option<&I> {
        todo!()
    }

    #[must_use]
    pub fn intercept_chain(&self, _key: KeyId) -> InterceptChain<'_, I> {
        todo!()
    }
}

impl<I> Clone for Context<I> {
    fn clone(&self) -> Self {
        todo!()
    }
}

impl<I> PartialEq for Context<I> {
    fn eq(&self, _other: &Self) -> bool {
        todo!()
    }
}

impl<I> Eq for Context<I> {}

#[derive(Debug)]
pub struct Derive<'a, I> {
    _marker: std::marker::PhantomData<&'a I>,
}

impl<I> Derive<'_, I> {
    #[must_use]
    pub fn bind(self, _binding: &IsolationBinding) -> Self {
        todo!()
    }

    #[must_use]
    pub fn bind_all(self, _bindings: &[IsolationBinding]) -> Self {
        todo!()
    }

    #[must_use]
    pub fn isolate(self, _key: KeyId, _realm: RealmId) -> Self {
        todo!()
    }

    #[must_use]
    pub fn intercept(self, _key: KeyId, _value: I) -> Self {
        todo!()
    }

    #[must_use]
    pub fn build(self) -> Context<I> {
        todo!()
    }
}

#[derive(Debug)]
pub struct InterceptChain<'a, I> {
    _marker: std::marker::PhantomData<&'a I>,
}

impl<'a, I> Iterator for InterceptChain<'a, I> {
    type Item = &'a I;

    fn next(&mut self) -> Option<&'a I> {
        todo!()
    }
}

#[allow(dead_code)]
fn _unused(_: Option<Arc<Layer<()>>>) {}
