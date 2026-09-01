//! The M2-K14 authority matrix through the real daemon: four readings,
//! four different answers, each asserting its own precondition — and the
//! demonstration that an entry cannot widen its own outbound allowlist.

use std::sync::atomic::Ordering;

use jinnd_api::LedgerEventKind;

use super::{SECRET, booted, events, home, paths, requests, target};

/// The whole outbound matrix through the real daemon: the fixture's
/// activation ASSERTS each reading in-guest (a wrong answer faults the
/// entry, so a clean boot is the proof), and the kernel's own record
/// agrees — THREE calls made across BOTH provided shapes, and the
/// off-allowlist target never dialled once, redirect included.
#[tokio::test]
async fn the_outbound_matrix_holds_through_the_real_daemon() {
    let denied = target(None);
    let allowed = target(Some(denied.port));
    let home = home("matrix");
    let grants = serde_json::json!([
        "jinn:fs",
        { "contract": "jinn:net", "scope": { "outbound": [format!("127.0.0.1:{}", allowed.port)] } }
    ]);
    let daemon = booted(paths(
        &home,
        grants,
        &format!("net-out:{},{}", allowed.port, denied.port),
    ))
    .await;
    let records = events(&daemon).await;

    assert_eq!(
        denied.hits.load(Ordering::SeqCst),
        0,
        "the off-allowlist target was never dialled, redirect included"
    );
    assert_eq!(
        allowed.hits.load(Ordering::SeqCst),
        3,
        "three authorized calls, three dials"
    );
    let rows = requests(&records);
    assert_eq!(rows.len(), 3, "one record per AUTHORIZED call: {rows:?}");
    let LedgerEventKind::NetRequested {
        method,
        host,
        path,
        status,
        response_bytes,
        ..
    } = rows[0]
    else {
        panic!("not a request row")
    };
    assert_eq!(
        (method.as_str(), path.as_str(), *status),
        ("GET", "/probe", 200)
    );
    assert_eq!(host, &format!("127.0.0.1:{}", allowed.port));
    assert_eq!(*response_bytes, 4);
    // The refusals are on the record too, and they are not the same class.
    let refusals: Vec<String> = records
        .iter()
        .filter_map(|record| match &record.kind {
            LedgerEventKind::GrantRefused {
                contract, detail, ..
            } if contract == "jinn:net" => detail.clone(),
            _ => None,
        })
        .collect();
    assert_eq!(
        refusals.len(),
        3,
        "the off-allowlist call, the off-allowlist redirect, and the same \
         refusal through the 0.1.0 shape: {refusals:?}"
    );
    assert!(
        refusals.iter().all(|detail| detail.contains("allowlist")),
        "each names the allowlist: {refusals:?}"
    );

    // Law 2 vs 02 §Redaction: the credential reached the target (the guest
    // asserted the 200) and reaches NO byte of the durable record.
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
    let ledger = std::fs::read(home.0.join("ledger.sqlite"))
        .unwrap_or_else(|error| panic!("ledger file: {error}"));
    assert!(
        !ledger
            .windows(SECRET.len())
            .any(|window| window == SECRET.as_bytes()),
        "the ledger file carries no byte of the credential"
    );
}

/// The widening demonstration, stated: an entry cannot widen its OWN
/// outbound allowlist — `jinn:net` has no operation that writes a grant,
/// and `jinn:profile` refuses an entry that patches itself, even holding
/// the `*` scope. The fixture asserts both in-guest; a clean boot with the
/// target never dialled is the proof.
#[tokio::test]
async fn an_entry_cannot_widen_its_own_outbound_allowlist() {
    let server = target(None);
    let home = home("widen");
    let grants = serde_json::json!([
        { "contract": "jinn:net", "scope": { "outbound": ["127.0.0.1:1"] } },
        { "contract": "jinn:profile", "scope": ["*"] }
    ]);
    let daemon = booted(paths(
        &home,
        grants,
        &format!("net-widen:worker@{}", server.port),
    ))
    .await;
    let records = events(&daemon).await;
    assert_eq!(server.hits.load(Ordering::SeqCst), 0, "never reached");
    assert!(
        records.iter().any(|record| matches!(&record.kind,
            LedgerEventKind::AmendmentRefused { detail } if detail.contains("itself"))),
        "the refused self-patch is on the record"
    );
    assert!(
        !records
            .iter()
            .any(|record| matches!(&record.kind, LedgerEventKind::ProfilePatched { .. })),
        "and no patch was ever committed"
    );
}
