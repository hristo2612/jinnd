use std::sync::Arc;

use jinnd_api::ContextId;

use crate::key::{KeyId, RealmId};

#[derive(Debug)]
pub(crate) struct Layer<I> {
    pub(crate) id: ContextId,
    pub(crate) depth: u32,
    pub(crate) parent: Option<Arc<Layer<I>>>,
    _isolation: Box<[(KeyId, RealmId)]>,
    _intercept: Box<[(KeyId, I)]>,
}

impl<I> Layer<I> {
    pub(crate) fn new(
        _id: ContextId,
        _parent: Option<Arc<Layer<I>>>,
        _isolation: Vec<(KeyId, RealmId)>,
        _intercept: Vec<(KeyId, I)>,
    ) -> Self {
        todo!()
    }

    pub(crate) fn own_realm(&self, _key: KeyId) -> Option<RealmId> {
        todo!()
    }

    pub(crate) fn own_intercept(&self, _key: KeyId) -> Option<&I> {
        todo!()
    }
}
