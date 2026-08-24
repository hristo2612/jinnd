use jinnd_api::{ContextId, KernelError};

use crate::context::Context;
use crate::key::{KeyId, RealmId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Probe<T> {
    Provided(T),
    Declared,
    Absent,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Resolved<T> {
    pub value: T,
    pub caller: ContextId,
    pub provider: ContextId,
    pub realm: RealmId,
}

#[derive(Debug)]
pub struct ResolutionFrames<I> {
    _marker: std::marker::PhantomData<fn() -> I>,
}

impl<I> Iterator for ResolutionFrames<I> {
    type Item = Context<I>;

    fn next(&mut self) -> Option<Context<I>> {
        todo!()
    }
}

impl<I> Context<I> {
    #[must_use]
    pub fn resolution_frames(&self, _key: KeyId) -> ResolutionFrames<I> {
        todo!()
    }

    pub fn resolve<T>(
        &self,
        _key: KeyId,
        _probe: impl FnMut(&Context<I>) -> Probe<T>,
    ) -> Result<Resolved<T>, KernelError> {
        todo!()
    }
}
