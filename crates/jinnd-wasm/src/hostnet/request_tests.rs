//! Provider-seam pins for the M2-K14 outbound AUTHORITY: the allowlist as
//! ACCEPTANCE (an allowed authority succeeds, an off-allowlist one refuses
//! without ever connecting, a host alone never means every port), and the
//! three readings kept as three answers — `denied`, `invalid`, `failed`.
//!
//! Every refusal case asserts its OWN precondition: the same request under
//! a grant that admits it must succeed, or the refusal proves nothing
//! (M2-K8 round-3 lesson). The record's shape lives in
//! `request_record_tests`, the bounds and redirects in
//! `request_bounds_tests`.

use std::sync::atomic::Ordering;

use jinnd_api::{ErrorCode, LedgerEventKind, RefusalReason};

use super::outbound_rig_tests::{ok_body, rig, rig_pair, target};

/// The allowlist is ACCEPTANCE, not preamble: an off-allowlist authority
/// refuses on the record and the kernel never dials it — and the SAME
/// request under a grant that admits it succeeds, so the refusal is not
/// vacuous.
#[tokio::test]
async fn an_off_allowlist_authority_refuses_without_ever_connecting() {
    let server = target(ok_body("pong"));
    let url = format!("http://127.0.0.1:{}/probe", server.port);
    let refused = rig(&["127.0.0.1:1"]);
    assert_eq!(refused.get(&url).await, Err(ErrorCode::EffectFailed));
    assert_eq!(server.hits.load(Ordering::SeqCst), 0, "never dialled");
    assert_eq!(refused.scope_refusals(), 1, "the refusal is on the record");
    assert!(refused.requested().is_empty(), "no record of a sent call");

    let allowed = rig_pair(server.port);
    assert!(allowed.get(&url).await.is_ok(), "the precondition holds");
    assert_eq!(server.hits.load(Ordering::SeqCst), 1);
}

/// A bare grant reaches nothing: the empty allowlist is default deny.
#[tokio::test]
async fn a_bare_grant_reaches_nothing() {
    let server = target(ok_body("pong"));
    let rig = rig(&[]);
    assert_eq!(
        rig.get(&format!("http://127.0.0.1:{}/probe", server.port))
            .await,
        Err(ErrorCode::EffectFailed)
    );
    assert_eq!(server.hits.load(Ordering::SeqCst), 0);
}

/// An entry admits its OWN authority and nothing beside it: a host alone
/// never means "every port" (Law 1, the M2-K8 hull ruling read for hosts).
#[tokio::test]
async fn an_entry_admits_its_own_authority_and_nothing_beside_it() {
    let server = target(ok_body("pong"));
    let rig = rig(&["127.0.0.1"]);
    assert_eq!(
        rig.get(&format!("http://127.0.0.1:{}/probe", server.port))
            .await,
        Err(ErrorCode::EffectFailed),
        "granting the host alone confers port 80, not this one"
    );
    assert_eq!(server.hits.load(Ordering::SeqCst), 0);
    // And the same spelling difference cuts the other way: `localhost` is
    // a different authority from `127.0.0.1`, never a silent alias.
    let aliased = rig_pair(server.port);
    assert_eq!(
        aliased
            .get(&format!("http://localhost:{}/probe", server.port))
            .await,
        Err(ErrorCode::EffectFailed)
    );
    assert_eq!(server.hits.load(Ordering::SeqCst), 0);
}

/// Three readings, three answers: off the allowlist is `denied`
/// (EffectFailed), a URL the provider cannot make sense of is `invalid`
/// (InvalidProfile) and never a grant event, and an authorized call the
/// network failed is `failed` (PluginFailed).
#[tokio::test]
async fn the_three_refusals_are_three_distinct_answers() {
    let rig = rig(&["127.0.0.1:1"]);
    for bad in [
        "not a url",
        "https://127.0.0.1:1/x",
        "http://",
        "http://user:pw@127.0.0.1:1/x",
        "http://127.0.0.1:notaport/x",
    ] {
        assert_eq!(
            rig.get(bad).await,
            Err(ErrorCode::InvalidProfile),
            "{bad} is invalid, not denied"
        );
    }
    assert_eq!(rig.scope_refusals(), 0, "a bad URL is not a grant refusal");
    // Authorized, and nothing is listening on port 1.
    assert_eq!(
        rig.get("http://127.0.0.1:1/x").await,
        Err(ErrorCode::PluginFailed),
        "a network failure is its own reading"
    );
}

/// A non-loopback target is refused as such even when the allowlist names
/// it: v0.2 reaches loopback only, and no resolver is ever consulted.
#[tokio::test]
async fn a_non_loopback_target_on_the_allowlist_is_still_refused() {
    let rig = rig(&["example.com:80", "203.0.113.7:80"]);
    for url in ["http://example.com/x", "http://203.0.113.7/x"] {
        assert_eq!(rig.get(url).await, Err(ErrorCode::EffectFailed), "{url}");
    }
    let reasons: Vec<RefusalReason> = rig
        .ledger
        .kinds()
        .into_iter()
        .filter_map(|(kind, _)| match kind {
            LedgerEventKind::GrantRefused { reason, .. } => Some(reason),
            _ => None,
        })
        .collect();
    assert_eq!(
        reasons,
        vec![RefusalReason::NotLoopback, RefusalReason::NotLoopback],
        "the record says WHY, and it is not a scope mismatch"
    );
}

/// A header a caller could smuggle a second request line through is
/// `invalid`, and so is one that would fight the kernel for the framing.
#[tokio::test]
async fn injected_and_framing_headers_are_invalid() {
    let server = target(ok_body("pong"));
    let rig = rig_pair(server.port);
    let url = format!("http://127.0.0.1:{}/probe", server.port);
    for (name, value) in [
        ("x-smuggle\r\nx-evil", "1"),
        ("x-smuggle", "1\r\nx-evil: 1"),
        ("content-length", "0"),
        ("connection", "keep-alive"),
        ("host", "elsewhere"),
    ] {
        assert_eq!(
            rig.request("GET", &url, &[(name.to_owned(), value.to_owned())], &[])
                .await,
            Err(ErrorCode::InvalidProfile),
            "{name}"
        );
    }
    assert_eq!(server.hits.load(Ordering::SeqCst), 0);
}
