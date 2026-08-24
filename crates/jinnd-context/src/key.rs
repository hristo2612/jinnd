use jinnd_api::Realm;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KeyId(u64);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RealmId(u64);

impl RealmId {
    pub const ROOT: Self = Self(0);

    #[must_use]
    pub fn is_root(self) -> bool {
        todo!()
    }
}

#[derive(Debug, Default)]
pub(crate) struct KeyTable;

impl KeyTable {
    pub(crate) fn intern(&mut self, _name: &str) -> KeyId {
        todo!()
    }

    pub(crate) fn name(&self, _key: KeyId) -> Option<&str> {
        todo!()
    }
}

#[derive(Debug)]
pub(crate) struct RealmTable;

impl RealmTable {
    pub(crate) fn new() -> Self {
        todo!()
    }

    pub(crate) fn intern(&mut self, _realm: &Realm) -> RealmId {
        todo!()
    }

    pub(crate) fn value(&self, _realm: RealmId) -> Option<&Realm> {
        todo!()
    }
}
