//! Interned identities. The resolution walk compares `u64`s only (R3).

use std::any::TypeId;
use std::collections::HashMap;

use jinnd_api::Realm;

/// Interned identity of one service *name*.
///
/// Names are the profile's vocabulary. The isolation map and the intercept chain are
/// keyed by name, so a profile binding for `"bar"` isolates a typed contract named
/// `"bar"` exactly as it isolates a dynamic one — which is how the TS original keys
/// isolation by property name.
///
/// A name is not a slot: see [`ServiceKey`]. Interning happens once, at the boundary;
/// callers cache the id for the lifetime of an activation and the walk never touches a
/// string.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NameId(u64);

/// Identity of one service slot (R3).
///
/// The typed lane resolves **by type**: a key carries the `TypeId` of its contract, so
/// two contract types that publish one name never share a slot. The dynamic lane —
/// reserved for plugins loaded by name at runtime — carries no type and is identified
/// by its name alone.
///
/// Both lanes expose the same [`ServiceKey::name`], which is what keeps a profile's
/// name-keyed isolation binding effective against a typed contract.
///
/// `Copy` and compared as integers: no string reaches the resolution hot path.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ServiceKey {
    name: NameId,
    type_id: Option<TypeId>,
}

impl ServiceKey {
    /// The slot of a statically typed contract.
    #[must_use]
    pub fn typed(name: NameId, type_id: TypeId) -> Self {
        Self {
            name,
            type_id: Some(type_id),
        }
    }

    /// The slot of a plugin loaded by name, which has no Rust type at this boundary.
    #[must_use]
    pub fn dynamic(name: NameId) -> Self {
        Self {
            name,
            type_id: None,
        }
    }

    /// The name this slot is isolated and intercepted under.
    #[must_use]
    pub fn name(self) -> NameId {
        self.name
    }

    /// The contract type, or `None` on the dynamic lane.
    #[must_use]
    pub fn type_id(self) -> Option<TypeId> {
        self.type_id
    }

    /// Whether this slot belongs to the typed lane.
    #[must_use]
    pub fn is_typed(self) -> bool {
        self.type_id.is_some()
    }
}

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

/// Interner for service names. Owned by the tree; never reachable from a walk.
#[derive(Debug, Default)]
pub(crate) struct NameTable {
    forward: HashMap<Box<str>, NameId>,
    reverse: Vec<Box<str>>,
}

impl NameTable {
    /// Returns the id for `name`, assigning one on first sight.
    pub(crate) fn intern(&mut self, name: &str) -> NameId {
        if let Some(id) = self.forward.get(name) {
            return *id;
        }
        let id = NameId(self.reverse.len() as u64);
        let owned: Box<str> = Box::from(name);
        self.reverse.push(owned.clone());
        self.forward.insert(owned, id);
        id
    }

    /// The text `id` was interned from, for diagnostics only.
    pub(crate) fn text(&self, id: NameId) -> Option<&str> {
        self.reverse.get(id.0 as usize).map(AsRef::as_ref)
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
