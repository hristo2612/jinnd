//! What an outbound call REFUSES TO DO (M2-K14): it never follows a
//! redirect, it never hands back one it cannot prove the allowlist admits,
//! and it is bounded in body and in time so an authorized call can never
//! stall the caller (R9, R1).

use std::sync::atomic::Ordering;

use jinnd_api::{ErrorCode, LedgerEventKind};

use super::outbound_rig_tests::{Answer, ok_body, redirector, rig, rig_pair, target};

/// The redirect hole, closed WHERE IT LIVES: a 30x naming an authority the
/// allowlist does not admit is DENIED inside the host call. Handing the
/// caller the hop and trusting it not to follow would move the boundary
/// out of the kernel and into the guarded party — an authority the guarded
/// party enforces is not an authority. The call still happened, so the row
/// still lands: a refusal is never a licence to forget a sent request.
#[tokio::test]
async fn a_redirect_off_the_allowlist_is_denied_inside_the_call() {
    let elsewhere = target(ok_body("secret"));
    let hop = redirector(302, elsewhere.port);
    let rig = rig_pair(hop.port);
    assert_eq!(
        rig.get(&format!("http://127.0.0.1:{}/go", hop.port)).await,
        Err(ErrorCode::EffectFailed),
        "the caller is refused, not handed the hop"
    );
    assert_eq!(
        elsewhere.hits.load(Ordering::SeqCst),
        0,
        "and the kernel never dialled the redirect target"
    );
    let rows = rig.requested();
    assert_eq!(rows.len(), 1, "the sent call is still on the record");
    let LedgerEventKind::NetRequested { status, .. } = &rows[0] else {
        panic!("not a request row")
    };
    assert_eq!(*status, 302, "recorded as what it was");
    assert_eq!(rig.scope_refusals(), 1, "and the refusal names the scope");
}

/// The refusal is the ALLOWLIST's, not a blanket fear of 3xx: a redirect
/// naming an authority the caller may already reach is answered — and
/// still not followed, so the caller re-issues it under the same check.
#[tokio::test]
async fn a_redirect_to_an_admitted_authority_is_answered_and_still_not_followed() {
    let admitted = target(ok_body("secret"));
    let hop = redirector(302, admitted.port);
    let rig = rig(&[
        &format!("127.0.0.1:{}", hop.port),
        &format!("127.0.0.1:{}", admitted.port),
    ]);
    let (status, headers, _) = rig
        .get(&format!("http://127.0.0.1:{}/go", hop.port))
        .await
        .unwrap_or_else(|code| panic!("request: {code:?}"));
    assert_eq!(status, 302, "the caller sees the redirect");
    assert!(
        headers.iter().any(|(name, _)| name == "location"),
        "and can read where it points: {headers:?}"
    );
    assert_eq!(
        admitted.hits.load(Ordering::SeqCst),
        0,
        "the kernel followed nothing"
    );
    assert_eq!(rig.requested().len(), 1, "one call, one record");
}

/// A 30x that names no destination names no authority, so there is nothing
/// to refuse; a relative one stays on the authority already admitted.
#[tokio::test]
async fn a_redirect_naming_no_new_authority_is_answered() {
    for location in ["", "location: /elsewhere\r\n"] {
        let hop = target(Answer::Raw(format!(
            "HTTP/1.1 302 Found\r\n{location}content-length: 0\r\n\r\n"
        )));
        let rig = rig_pair(hop.port);
        let (status, _, _) = rig
            .get(&format!("http://127.0.0.1:{}/go", hop.port))
            .await
            .unwrap_or_else(|code| panic!("request {location:?}: {code:?}"));
        assert_eq!(status, 302);
    }
}

/// A redirect the provider cannot even parse cannot be proven admitted, so
/// it is refused rather than passed along. Fail closed.
#[tokio::test]
async fn a_redirect_the_provider_cannot_parse_is_denied() {
    let hop = target(Answer::Raw(
        "HTTP/1.1 302 Found\r\nlocation: gopher://nowhere\r\ncontent-length: 0\r\n\r\n".to_owned(),
    ));
    let rig = rig_pair(hop.port);
    assert_eq!(
        rig.get(&format!("http://127.0.0.1:{}/go", hop.port)).await,
        Err(ErrorCode::EffectFailed)
    );
}

/// Bounded (R9): a response past the cap is a TYPED failure, never a
/// truncated body handed back as whole, and chunked framing is a named
/// typed failure rather than a silently mis-parsed body.
#[tokio::test]
async fn a_body_past_the_cap_and_chunked_framing_are_typed_failures() {
    let big = target(Answer::Body(super::request::BODY_CAP + 1));
    let capped = rig_pair(big.port);
    assert_eq!(
        capped
            .get(&format!("http://127.0.0.1:{}/big", big.port))
            .await,
        Err(ErrorCode::PluginFailed)
    );
    let chunked = target(Answer::Raw(
        "HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\n\r\n4\r\npong\r\n0\r\n\r\n".to_owned(),
    ));
    let rig = rig_pair(chunked.port);
    assert_eq!(
        rig.get(&format!("http://127.0.0.1:{}/x", chunked.port))
            .await,
        Err(ErrorCode::PluginFailed)
    );
    // Under the cap the same shape succeeds: the cap, not the plumbing.
    let fits = target(Answer::Body(1024));
    let ok = rig_pair(fits.port);
    let (status, _, body) = ok
        .get(&format!("http://127.0.0.1:{}/small", fits.port))
        .await
        .unwrap_or_else(|code| panic!("request: {code:?}"));
    assert_eq!((status, body.len()), (200, 1024));
}

/// A target that accepts and never answers cannot pin the caller: the
/// whole call is bounded, and the bound sits UNDER the guest deadline so
/// the guest is answered rather than killed (the M2-K12 lesson).
#[tokio::test(start_paused = true)]
async fn a_target_that_never_answers_is_bounded() {
    assert!(
        super::request::BOUND < crate::lane::DEADLINE,
        "the outbound bound must sit under the guest-call deadline"
    );
    let server = target(Answer::Silent);
    let rig = rig_pair(server.port);
    assert_eq!(
        rig.get(&format!("http://127.0.0.1:{}/hang", server.port))
            .await,
        Err(ErrorCode::PluginFailed),
        "the bound answers the caller"
    );
    let rows = rig.requested();
    assert_eq!(rows.len(), 1, "an authorized attempt is on the record");
    let LedgerEventKind::NetRequested { status, .. } = &rows[0] else {
        panic!("not a request row")
    };
    assert_eq!(*status, 0, "no status was read, and the row says so");
}
