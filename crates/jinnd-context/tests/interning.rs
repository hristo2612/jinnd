//! Key and realm interning: the resolution hot path compares `u64`s only (R3).

use std::any::TypeId;

use jinnd_api::{EntryId, Realm, ServiceContract, ServiceType};
use jinnd_context::{ContextTree, RealmId};

struct Bar(u8);

impl ServiceContract for Bar {
    type Observation = u8;

    const NAME: &'static str = "bar";

    fn observe(&self) -> u8 {
        self.0
    }
}

#[test]
fn equal_names_intern_to_one_id_and_distinct_names_do_not() {
    let tree = ContextTree::<()>::new();

    assert_eq!(tree.name("bar"), tree.name("bar"));
    assert_ne!(tree.name("bar"), tree.name("qux"));
}

/// R3: the two lanes are distinct slots — the typed one carries its `TypeId`, the
/// dynamic one carries none — but they share the isolation namespace, so a profile's
/// `isolate: ["bar"]` still governs the typed contract named `"bar"`.
#[test]
fn the_two_lanes_are_distinct_slots_that_share_one_isolation_name() {
    let tree = ContextTree::<()>::new();

    let typed = tree.key_of::<Bar>();
    let dynamic = tree.dynamic_key("bar");

    assert_ne!(typed, dynamic);
    assert_eq!(typed.name(), dynamic.name());
    assert_eq!(typed.type_id(), Some(TypeId::of::<Bar>()));
    assert_eq!(dynamic.type_id(), None);
    assert!(typed.is_typed());
    assert!(!dynamic.is_typed());
}

/// Interning a typed key twice yields one identity; the walk caches it per activation.
#[test]
fn a_typed_key_interns_to_one_identity() {
    let tree = ContextTree::<()>::new();

    assert_eq!(tree.key_of::<Bar>(), tree.key_of::<Bar>());
}

#[test]
fn root_realm_is_the_reserved_zero_realm() {
    let tree = ContextTree::<()>::new();

    assert_eq!(tree.realm(&Realm::Root), RealmId::ROOT);
    assert!(RealmId::ROOT.is_root());
}

/// TS origin: `packages/loader/src/config/isolate.ts` realm store — one realm value
/// is one realm. Structural half of loader case
/// `semantically_identical_realm_update_is_inert`.
#[test]
fn semantically_identical_realms_intern_equal() {
    let tree = ContextTree::<()>::new();
    let alpha = Realm::Local(EntryId("alpha".into()));

    assert_eq!(
        tree.realm(&alpha),
        tree.realm(&Realm::Local(EntryId("alpha".into())))
    );
}

#[test]
fn local_and_shared_realms_with_the_same_label_are_distinct_slots() {
    let tree = ContextTree::<()>::new();

    let local = tree.realm(&Realm::Local(EntryId("alpha".into())));
    let shared = tree.realm(&Realm::Shared("alpha".into()));

    assert_ne!(local, shared);
    assert_ne!(local, RealmId::ROOT);
}

#[test]
fn interned_identities_report_their_source_value_for_diagnostics() {
    let tree = ContextTree::<()>::new();
    let name = tree.name("bar");
    let realm = tree.realm(&Realm::Shared("beta".into()));

    assert_eq!(tree.name_value(name).as_deref(), Some("bar"));
    assert_eq!(tree.realm_value(realm), Some(Realm::Shared("beta".into())));
    assert_eq!(tree.realm_value(RealmId::ROOT), Some(Realm::Root));
}

/// A second contract that happens to publish the same name as [`Bar`].
struct ShadowBar(u8);

impl ServiceContract for ShadowBar {
    type Observation = u8;

    const NAME: &'static str = "bar";

    fn observe(&self) -> u8 {
        self.0
    }
}

/// R3: the typed lane resolves by type, so two contract types that publish one name
/// must never share a slot. The string lane stays name-keyed for dynamic plugins.
#[test]
fn distinct_typed_contracts_never_alias_on_a_shared_name() {
    let tree = ContextTree::<()>::new();

    assert_ne!(tree.key_of::<Bar>(), tree.key_of::<ShadowBar>());
}

/// Two contract types that publish one name are two slots, but one isolation name:
/// a single profile binding moves both, which is what keeps the dynamic lane able to
/// govern typed contracts at all.
#[test]
fn contracts_that_share_a_name_share_their_isolation_but_not_their_slot() {
    let tree = ContextTree::<()>::new();

    let bar = tree.key_of::<Bar>();
    let shadow = tree.key_of::<ShadowBar>();
    let realm = tree.realm(&Realm::Shared("beta".into()));

    let isolated = tree.root().derive().isolate(bar.name(), realm).build();

    assert_ne!(bar, shadow);
    assert_eq!(bar.name(), shadow.name());
    assert_eq!(isolated.realm_of(shadow.name()), realm);
}

/// Distinct names stay distinct in both lanes, whatever the type.
#[test]
fn distinct_names_never_share_an_isolation_name() {
    let tree = ContextTree::<()>::new();

    assert_ne!(tree.key_of::<Bar>().name(), tree.dynamic_key("qux").name());
}

/// The facade already carries a typed identity ([`ServiceType`]), so the registry can
/// name a slot without this crate reaching into the contract type. That is why this
/// packet needs no `jinnd-api` delta.
#[test]
fn a_facade_service_type_names_the_same_slot_as_its_contract() {
    let tree = ContextTree::<()>::new();

    let from_type = tree.key_for(&ServiceType {
        type_id: TypeId::of::<Bar>(),
        name: Bar::NAME,
    });

    assert_eq!(from_type, tree.key_of::<Bar>());
    assert_ne!(from_type, tree.key_of::<ShadowBar>());
}

/// A slot's name is always its contract's `NAME`: that name is what a profile's
/// isolation binding addresses, so the two can never be allowed to disagree.
#[test]
fn a_slot_is_always_named_after_its_contract() {
    let tree = ContextTree::<()>::new();

    assert_eq!(tree.key_of::<Bar>().name(), tree.name(Bar::NAME));
    assert_eq!(tree.dynamic_key("qux").name(), tree.name("qux"));
}
