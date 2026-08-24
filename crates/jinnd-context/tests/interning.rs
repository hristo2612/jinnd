//! Key and realm interning: the resolution hot path compares `u64`s only (R3).

use jinnd_api::{EntryId, Realm, ServiceContract};
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
fn equal_names_intern_to_one_key_and_distinct_names_do_not() {
    let tree = ContextTree::<()>::new();

    assert_eq!(tree.key("bar"), tree.key("bar"));
    assert_ne!(tree.key("bar"), tree.key("qux"));
}

/// The typed lane and the profile's dynamic string lane address one slot.
#[test]
fn typed_contract_shares_the_key_namespace_with_the_dynamic_lane() {
    let tree = ContextTree::<()>::new();

    assert_eq!(tree.key_of::<Bar>(), tree.key("bar"));
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
    let key = tree.key("bar");
    let realm = tree.realm(&Realm::Shared("beta".into()));

    assert_eq!(tree.key_name(key).as_deref(), Some("bar"));
    assert_eq!(tree.realm_value(realm), Some(Realm::Shared("beta".into())));
    assert_eq!(tree.realm_value(RealmId::ROOT), Some(Realm::Root));
}
