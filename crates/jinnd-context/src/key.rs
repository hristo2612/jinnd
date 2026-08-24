//! Interned identities. The resolution walk compares `u64`s only (R3).

use std::collections::HashMap;

use jinnd_api::Realm;

/// Interned identity of one service key.
///
/// The key namespace is the contract name ([`jinnd_api::ServiceContract::NAME`]),
/// shared by the typed lane and the profile's dynamic string lane: a profile's
/// isolation binding for `"bar"` and a typed resolve of a contract named `"bar"`
/// address one slot, exactly as the TS original keys isolation by property name.
///
/// Interning happens once, at the boundary; callers cache the [`KeyId`] for the
/// lifetime of an activation and the walk never touches a string.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KeyId(u64);

/// Interned identity of one realm.
///
/// Two [`Realm`] values that are equal intern to one `RealmId`, so a profile that
/// rewrites the same isolation mapping produces no observable change.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RealmId(u64);

impl RealmId {
    /// The realm a key resolves in when no layer binds it: [`Realm::Root`].
    pub const ROOT: Self = Self(0);

    /// Whether this is the unbound root realm.
    #[must_use]
    pub fn is_root(self) -> bool {
        self == Self::ROOT
    }
}

/// Interner for service keys. Owned by the tree; never reachable from a walk.
#[derive(Debug, Default)]
pub(crate) struct KeyTable {
    forward: HashMap<Box<str>, KeyId>,
    reverse: Vec<Box<str>>,
}

impl KeyTable {
    /// Returns the id for `name`, assigning one on first sight.
    pub(crate) fn intern(&mut self, name: &str) -> KeyId {
        if let Some(key) = self.forward.get(name) {
            return *key;
        }
        let key = KeyId(self.reverse.len() as u64);
        let owned: Box<str> = Box::from(name);
        self.reverse.push(owned.clone());
        self.forward.insert(owned, key);
        key
    }

    /// The name `key` was interned from, for diagnostics only.
    pub(crate) fn name(&self, key: KeyId) -> Option<&str> {
        self.reverse.get(key.0 as usize).map(AsRef::as_ref)
    }
}

/// Interner for realms, pre-seeded so that [`Realm::Root`] is [`RealmId::ROOT`].
#[derive(Debug)]
pub(crate) struct RealmTable {
    forward: HashMap<Realm, RealmId>,
    reverse: Vec<Realm>,
}

impl RealmTable {
    pub(crate) fn new() -> Self {
        let mut table = Self {
            forward: HashMap::new(),
            reverse: Vec::new(),
        };
        let root = table.intern(&Realm::Root);
        debug_assert_eq!(root, RealmId::ROOT);
        table
    }

    /// Returns the id for `realm`, assigning one on first sight.
    pub(crate) fn intern(&mut self, realm: &Realm) -> RealmId {
        if let Some(id) = self.forward.get(realm) {
            return *id;
        }
        let id = RealmId(self.reverse.len() as u64);
        self.reverse.push(realm.clone());
        self.forward.insert(realm.clone(), id);
        id
    }

    /// The realm `id` was interned from, for diagnostics and write-back.
    pub(crate) fn value(&self, id: RealmId) -> Option<&Realm> {
        self.reverse.get(id.0 as usize)
    }
}
