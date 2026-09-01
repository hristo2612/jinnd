//! M2-K15 through the REAL daemon assembly: a guest makes https calls, and
//! the three answers stay three — off the allowlist `denied`, an
//! unanchored certificate `untrusted`, and neither of them the network's
//! `failed`. The authorized-but-unbelieved call still LANDS its
//! irreversible row, and Law 3 refuses to revert it, after a reopen too.
//!
//! WHY THE HAPPY PATH IS NOT HERE, stated plainly rather than left as a
//! gap. Making a locally issued certificate VERIFY needs the extra trust
//! anchor, and that seam is `#[cfg(test)]` inside `jinnd-wasm` — it does
//! not exist in the build this integration test links, which is exactly
//! the property the card demands ("cannot exist in a release build"). So
//! the daemon proves everything a REFUSED handshake can prove, and the
//! answered handshake is proven in the host suite where the seam lives
//! (`hostnet::tls_tests`). Where inference would begin, it stops instead.

use std::sync::atomic::Ordering;

use jinnd_api::{ErrorCode, LedgerEventKind};
use jinnd_daemon::{Daemon, UnitMember};

use super::{SECRET, booted, events, home, paths, requests, target, tls_target};

/// The whole M2-K15 daemon acceptance in one boot: the guest's own
/// classification (it FAILS the boot unless each call answered its own
/// case), the record, and the revert.
#[tokio::test]
async fn an_https_call_is_classified_recorded_and_never_revertible() {
    let unanchored = tls_target();
    let elsewhere = target(None);
    let home = home("tls");
    let grants = serde_json::json!([
        "jinn:fs",
        { "contract": "jinn:net", "scope": { "outbound": [format!("127.0.0.1:{}", unanchored.port)] } }
    ]);
    let daemon = booted(paths(
        &home,
        grants,
        &format!("net-tls:{},{}", unanchored.port, elsewhere.port),
    ))
    .await;

    // The guest saw `untrusted` through BOTH doors, or the boot failed.
    // Its verdict is not the only evidence: the peer WAS reached twice and
    // the off-allowlist target never was.
    assert_eq!(
        unanchored.hits.load(Ordering::SeqCst),
        2,
        "both doors reached the peer and neither believed it"
    );
    assert_eq!(
        elsewhere.hits.load(Ordering::SeqCst),
        0,
        "an off-allowlist authority is refused before the dial"
    );

    // The record: every authorized call landed a row, and not one of them
    // carries the credential the guest sent in a header and a query.
    let records = events(&daemon).await;
    let rows = requests(&records);
    assert_eq!(rows.len(), 2, "one row per authorized call: {rows:?}");
    for row in &rows {
        let LedgerEventKind::NetRequested {
            host, path, status, ..
        } = row
        else {
            panic!("not a request row")
        };
        assert_eq!(host, &format!("127.0.0.1:{}", unanchored.port));
        assert_eq!(path, "/probe", "the query string is never recorded");
        assert_eq!(*status, 0, "no answer was believed");
    }
    let ledger = std::fs::read(home.0.join("ledger.sqlite"))
        .unwrap_or_else(|error| panic!("read ledger: {error}"));
    assert!(
        !String::from_utf8_lossy(&ledger).contains(SECRET),
        "the credential is nowhere in the durable record"
    );

    // Law 3 over TLS: the call cannot be un-sent, so a unit containing it
    // is rejected whole and the revertible member beside it still stands.
    let written = home.0.join("data/kept");
    assert!(written.exists(), "the fs effect landed");
    let (fs_effect, _) = daemon
        .fs_effects()
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("one revertible fs effect"));
    let calls = daemon
        .net_effects()
        .await
        .unwrap_or_else(|error| panic!("net effects: {error:?}"));
    assert_eq!(calls.len(), 2, "two irreversible calls: {calls:?}");
    let (net_effect, label) = calls[0].clone();
    let refused = daemon
        .revert_unit(
            &[UnitMember::Fs(fs_effect), UnitMember::Net(net_effect)],
            "tls-1",
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("a unit containing an irreversible tls call is rejected"));
    assert_eq!(refused.code, ErrorCode::Irreversible, "typed, not prose");
    assert!(
        refused.message.contains(&label),
        "the refusal names WHAT: {}",
        refused.message
    );
    assert!(written.exists(), "nothing in the rejected unit was applied");

    // DURABLE, not merely live: the refusal survives the process that made
    // the call. Re-proven over TLS rather than inherited from M2-K14.
    let reopened = Daemon::open(paths(&home, serde_json::json!([]), "idle"))
        .unwrap_or_else(|error| panic!("reopen: {error:?}"));
    let again = reopened
        .revert_unit(&[UnitMember::Net(net_effect)], "tls-2")
        .await
        .err()
        .unwrap_or_else(|| panic!("still irreversible after a reopen"));
    assert_eq!(again.code, ErrorCode::Irreversible, "durably typed");
}
