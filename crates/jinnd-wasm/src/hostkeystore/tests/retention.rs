//! `jinn:keystore` retention pins (M2-K8): LIFO withdrawal from sealed
//! inverses, the keyed-revert witness, reopen from disk with the journal,
//! and malformed key names.

use std::sync::{Arc, Mutex};

use jinnd_api::{EffectId, ErrorCode, FiberId};

use super::{
    Broker, GrantScope, HostKeystore, KEYSTORE_CONTRACT, LedgerSink, OTHER, Recording, SECRET,
    effect_of, home, open, passphrase, put_wire, rig,
};

#[tokio::test]
async fn withdrawal_restores_each_prior_lifo_and_the_witness_attests() {
    let rig = rig("withdraw");
    let first = effect_of(&rig.ok("put", put_wire("engines/k", SECRET)).await);
    let second = effect_of(&rig.ok("put", put_wire("engines/k", OTHER)).await);
    let third = effect_of(&rig.ok("delete", b"engines/k".to_vec()).await);
    assert_eq!(rig.keystore.effects().len(), 3);
    let (witness, inverse) = rig
        .keystore
        .undo_action(third)
        .unwrap_or_else(|| panic!("revertible"));
    assert!(!witness(), "before the inverse the key is not at its prior");
    inverse()
        .await
        .unwrap_or_else(|error| panic!("inverse: {error:?}"));
    assert!(witness(), "the deleted value is back");
    rig.keystore
        .reclaim(third)
        .await
        .unwrap_or_else(|error| panic!("reclaim: {error:?}"));
    assert_eq!(rig.ok("get", b"engines/k".to_vec()).await, OTHER.to_vec());
    rig.keystore
        .withdraw(second)
        .await
        .unwrap_or_else(|error| panic!("withdraw: {error:?}"));
    assert_eq!(rig.ok("get", b"engines/k".to_vec()).await, SECRET.to_vec());
    rig.keystore
        .withdraw(first)
        .await
        .unwrap_or_else(|error| panic!("withdraw: {error:?}"));
    assert_eq!(
        rig.call(rig.guest, "get", b"engines/k".to_vec()).await,
        Err(ErrorCode::NotFound),
        "the trail withdrawn LIFO leaves prior absence"
    );
    assert!(rig.keystore.effects().is_empty());
    assert!(
        rig.keystore.withdraw(third).await.is_ok(),
        "an already-consumed effect withdraws clean"
    );
    assert!(rig.keystore.withdraw(EffectId(999)).await.is_err());
}

#[tokio::test]
async fn the_store_reopens_from_disk_with_its_journal() {
    let home = home("reopen");
    let ledger = Arc::new(Recording(Mutex::new(Vec::new())));
    {
        let broker = Arc::new(Broker::new(Arc::clone(&ledger) as Arc<dyn LedgerSink>));
        let keystore = open(&home, &ledger);
        keystore
            .register(&broker)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let guest = broker.register_peer(Some(FiberId(7)));
        broker.attribute_entry(guest, "holder");
        broker.grant_with(
            guest,
            KEYSTORE_CONTRACT,
            GrantScope::Keys(vec!["e/".to_owned()]),
        );
        broker
            .dispatch(guest, KEYSTORE_CONTRACT, "put", put_wire("e/k", SECRET))
            .await
            .unwrap_or_else(|error| panic!("{error:?}"));
    }
    let reopened = open(&home, &ledger);
    let broker = Arc::new(Broker::new(Arc::clone(&ledger) as Arc<dyn LedgerSink>));
    reopened
        .register(&broker)
        .unwrap_or_else(|error| panic!("{error:?}"));
    let guest = broker.register_peer(Some(FiberId(11)));
    broker.grant_with(
        guest,
        KEYSTORE_CONTRACT,
        GrantScope::Keys(vec!["e/".to_owned()]),
    );
    assert_eq!(
        broker
            .dispatch(guest, KEYSTORE_CONTRACT, "get", b"e/k".to_vec())
            .await
            .map_err(|error| error.code),
        Ok(SECRET.to_vec()),
        "the sealed document reopens under the same master key"
    );
    let journals = reopened.journals();
    assert_eq!(journals.len(), 1);
    assert_eq!(journals[0].0, "holder");
    assert!(journals[0].1[0].label.starts_with("keystore put e/k"));
    // A tampered derivation salt (a different key) refuses the whole
    // store (fail-closed), never serves it empty.
    std::fs::write(home.0.join("keystore/salt"), [7u8; 16])
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(
        HostKeystore::open(
            home.0.join("keystore"),
            passphrase(),
            Arc::clone(&ledger) as Arc<dyn LedgerSink>
        )
        .is_err()
    );
}

#[tokio::test]
async fn malformed_key_names_are_the_typed_invalid() {
    let rig = rig("invalid");
    for key in ["", "engines/\0nul", &"x".repeat(513)] {
        assert_eq!(
            rig.call(rig.guest, "put", put_wire(key, SECRET)).await,
            Err(ErrorCode::InvalidProfile),
            "{key:?} refuses typed"
        );
    }
    assert_eq!(rig.ledger.scope_refusals(FiberId(7)), 0);
    assert!(rig.keystore.effects().is_empty());
}
