//! `jinn:keystore` authority pins (M2-K8): a bare grant admits no key; a
//! read-only attenuation refuses the mutations on the record.

use jinnd_api::{ErrorCode, FiberId};

use super::{OTHER, SECRET, names, put_wire, rig};

#[tokio::test]
async fn a_bare_grant_admits_no_key() {
    let rig = rig("bare");
    assert_eq!(
        rig.call(rig.bare, "put", put_wire("anything", SECRET))
            .await,
        Err(ErrorCode::EffectFailed)
    );
    assert_eq!(
        rig.call(rig.bare, "get", b"anything".to_vec()).await,
        Err(ErrorCode::EffectFailed)
    );
    assert!(
        names(
            &rig.call(rig.bare, "list", Vec::new())
                .await
                .unwrap_or_default()
        )
        .is_empty()
    );
    assert_eq!(rig.ledger.scope_refusals(FiberId(8)), 2);
}

#[tokio::test]
async fn a_read_only_attenuation_refuses_put_and_delete() {
    let rig = rig("read-only");
    rig.ok("put", put_wire("engines/a", SECRET)).await;
    assert_eq!(
        rig.call(rig.reader, "get", b"engines/a".to_vec()).await,
        Ok(SECRET.to_vec())
    );
    assert_eq!(
        names(
            &rig.call(rig.reader, "list", Vec::new())
                .await
                .unwrap_or_default()
        ),
        vec!["engines/a".to_owned()]
    );
    for (op, payload) in [
        ("put", put_wire("engines/a", OTHER)),
        ("delete", b"engines/a".to_vec()),
    ] {
        assert_eq!(
            rig.call(rig.reader, op, payload).await,
            Err(ErrorCode::EffectFailed),
            "{op} under a read-only grant refuses"
        );
    }
    assert_eq!(rig.ledger.scope_refusals(FiberId(9)), 2);
    assert_eq!(rig.ok("get", b"engines/a".to_vec()).await, SECRET.to_vec());
}
