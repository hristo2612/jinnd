//! Round-2 ruling 2 (Law 1), red-first: grants of one contract to one peer
//! compose ORDER-INDEPENDENTLY — scopes union, the effective operation
//! class is the union of each grant's declared class (absent = every
//! operation), and attenuation only ever narrows within a single grant.

use std::sync::Arc;

use jinnd_api::{FiberId, LedgerEventKind};

use crate::broker::Broker;
use crate::grants::{GrantScope, NetScope};
use crate::hostcaps::NET_CONTRACT;
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

fn net(ranges: &[(u16, u16)]) -> GrantScope {
    GrantScope::Net(NetScope {
        bind: ranges.to_vec(),
        outbound: Vec::new(),
    })
}

fn bind_of(broker: &Broker, peer: u64) -> NetScope {
    match broker.policy(peer, NET_CONTRACT) {
        Some(GrantScope::Net(policy)) => policy,
        other => panic!("net policy: {other:?}"),
    }
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

/// Round-3 ruling (Law 1), red-first: two disjoint bind grants compose to
/// their EXACT set of ranges, never the numeric hull — a hull over
/// `[1000,1000] ∪ [2000,2000]` would confer port 1500 that no grant named.
/// Normalization (sort, coalesce overlapping and adjacent) keeps equal sets
/// equal, so composition stays order-independent.
#[test]
fn disjoint_bind_ranges_compose_to_their_exact_set_never_the_hull() {
    let broker = broker();
    let forward = broker.register_peer(Some(FiberId(3)));
    let reverse = broker.register_peer(Some(FiberId(4)));
    let low = || net(&[(1000, 1000)]);
    let high = || net(&[(2000, 2000)]);
    broker.grant_with(forward, NET_CONTRACT, low());
    broker.grant_with(forward, NET_CONTRACT, high());
    broker.grant_with(reverse, NET_CONTRACT, high());
    broker.grant_with(reverse, NET_CONTRACT, low());
    for peer in [forward, reverse] {
        let policy = bind_of(&broker, peer);
        assert_eq!(policy.bind, vec![(1000, 1000), (2000, 2000)], "{policy:?}");
        assert!(policy.admits_port(1000) && policy.admits_port(2000));
        assert!(
            !policy.admits_port(1500),
            "the hull would confer a port no grant named: {policy:?}"
        );
    }
}

/// Normalization is what makes the set commutative: overlapping and
/// ADJACENT ranges coalesce, so `[10,20] ∪ [21,30]` and its reverse are the
/// same one range, while a gap of one port stays two.
#[test]
fn bind_sets_normalize_so_either_order_compares_equal() {
    let broker = broker();
    let forward = broker.register_peer(Some(FiberId(5)));
    let reverse = broker.register_peer(Some(FiberId(6)));
    broker.grant_with(forward, NET_CONTRACT, net(&[(10, 20), (40, 41)]));
    broker.grant_with(forward, NET_CONTRACT, net(&[(21, 30), (15, 18)]));
    broker.grant_with(reverse, NET_CONTRACT, net(&[(15, 18), (21, 30)]));
    broker.grant_with(reverse, NET_CONTRACT, net(&[(40, 41), (10, 20)]));
    let a = bind_of(&broker, forward);
    assert_eq!(a.bind, bind_of(&broker, reverse).bind, "order-independent");
    assert_eq!(a.bind, vec![(10, 30), (40, 41)], "{a:?}");
    assert!(a.admits_port(30) && a.admits_port(40));
    assert!(!a.admits_port(31) && !a.admits_port(39), "{a:?}");
}
