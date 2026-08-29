//! Round-2 ruling 2 (Law 1), red-first: grants of one contract to one peer
//! compose ORDER-INDEPENDENTLY — scopes union, the effective operation
//! class is the union of each grant's declared class (absent = every
//! operation), and attenuation only ever narrows within a single grant.

use std::sync::Arc;

use jinnd_api::{FiberId, LedgerEventKind};

use crate::broker::Broker;
use crate::grants::GrantScope;
use crate::hostfs::FS_CONTRACT;
use crate::hostkeystore::KEYSTORE_CONTRACT;
use crate::peer::LedgerSink;

struct Silent;

impl LedgerSink for Silent {
    fn append(&self, _kind: LedgerEventKind, _fiber: Option<FiberId>) {}
}

fn broker() -> Broker {
    Broker::new(Arc::new(Silent) as Arc<dyn LedgerSink>)
}

fn ops(names: &[&str]) -> Vec<String> {
    names.iter().map(|name| (*name).to_owned()).collect()
}

#[test]
fn key_and_path_scopes_union_in_either_order() {
    let broker = broker();
    let forward = broker.register_peer(Some(FiberId(1)));
    let reverse = broker.register_peer(Some(FiberId(2)));
    let engines = || GrantScope::Keys(vec!["engines/".to_owned()]);
    let smtp = || GrantScope::Keys(vec!["smtp/".to_owned()]);
    broker.grant_with(forward, KEYSTORE_CONTRACT, engines());
    broker.grant_with(forward, KEYSTORE_CONTRACT, smtp());
    broker.grant_with(reverse, KEYSTORE_CONTRACT, smtp());
    broker.grant_with(reverse, KEYSTORE_CONTRACT, engines());
    for peer in [forward, reverse] {
        let policy = broker
            .policy(peer, KEYSTORE_CONTRACT)
            .unwrap_or_else(|| panic!("granted"));
        assert!(policy.admits_key("engines/openai"), "{policy:?}");
        assert!(policy.admits_key("smtp/password"), "{policy:?}");
        assert!(!policy.admits_key("other/x"), "{policy:?}");
    }
    let log = || GrantScope::Paths(vec!["/log".to_owned()]);
    let tmp = || GrantScope::Paths(vec!["/tmp".to_owned()]);
    broker.grant_with(forward, FS_CONTRACT, log());
    broker.grant_with(forward, FS_CONTRACT, tmp());
    broker.grant_with(reverse, FS_CONTRACT, tmp());
    broker.grant_with(reverse, FS_CONTRACT, log());
    let mut a = broker.scopes(forward, FS_CONTRACT).unwrap_or_default();
    let mut b = broker.scopes(reverse, FS_CONTRACT).unwrap_or_default();
    a.sort();
    b.sort();
    assert_eq!(a, b);
    assert_eq!(a, vec!["/log".to_owned(), "/tmp".to_owned()]);
}

#[test]
fn an_unattenuated_grant_is_every_operation_in_either_order() {
    let broker = broker();
    let forward = broker.register_peer(Some(FiberId(1)));
    let reverse = broker.register_peer(Some(FiberId(2)));
    for peer in [forward, reverse] {
        broker.grant_with(
            peer,
            KEYSTORE_CONTRACT,
            GrantScope::Keys(vec!["k/".to_owned()]),
        );
    }
    broker.grant_ops(forward, KEYSTORE_CONTRACT, ops(&["get", "list"]));
    broker.lift_ops(forward, KEYSTORE_CONTRACT);
    broker.lift_ops(reverse, KEYSTORE_CONTRACT);
    broker.grant_ops(reverse, KEYSTORE_CONTRACT, ops(&["get", "list"]));
    for peer in [forward, reverse] {
        assert!(
            broker.check_op(peer, KEYSTORE_CONTRACT, "put").is_ok(),
            "a grant without ops is every operation, whatever the order"
        );
    }
}

#[test]
fn two_attenuated_grants_union_their_classes_and_stay_attenuated() {
    let broker = broker();
    let forward = broker.register_peer(Some(FiberId(1)));
    let reverse = broker.register_peer(Some(FiberId(2)));
    broker.grant_ops(forward, FS_CONTRACT, ops(&["read"]));
    broker.grant_ops(forward, FS_CONTRACT, ops(&["list"]));
    broker.grant_ops(reverse, FS_CONTRACT, ops(&["list"]));
    broker.grant_ops(reverse, FS_CONTRACT, ops(&["read"]));
    for peer in [forward, reverse] {
        assert!(broker.check_op(peer, FS_CONTRACT, "read").is_ok());
        assert!(broker.check_op(peer, FS_CONTRACT, "list").is_ok());
        assert!(
            broker.check_op(peer, FS_CONTRACT, "write").is_err(),
            "the union of two attenuations is still attenuated"
        );
    }
}
