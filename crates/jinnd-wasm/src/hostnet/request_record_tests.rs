//! What an outbound call WRITES (M2-K14): the Law-2 record that carries
//! the call's shape and never its credentials, the effect id that makes
//! the irreversibility durable, and the 0.1.0 `request` declaration which
//! is now provided beside the 0.2.0 whole-response edition (R12).

use std::sync::atomic::Ordering;

use jinnd_api::{ErrorCode, FiberId, LedgerEventKind};

use super::outbound_rig_tests::{ok_body, rig_pair, target};

/// The provided path: an allowed authority answers status, headers and
/// body, and lands exactly ONE record carrying the call's shape with the
/// caller's fiber attribution (Law 2).
#[tokio::test]
async fn an_allowed_authority_answers_and_lands_one_shaped_record() {
    let server = target(ok_body("pong"));
    let rig = rig_pair(server.port);
    let (status, headers, body) = rig
        .get(&format!("http://127.0.0.1:{}/probe", server.port))
        .await
        .unwrap_or_else(|code| panic!("request: {code:?}"));
    assert_eq!(status, 200);
    assert_eq!(body, b"pong");
    assert!(
        headers
            .iter()
            .any(|(name, value)| name == "content-type" && value == "text/plain"),
        "the response headers reach the caller: {headers:?}"
    );
    let rows = rig.requested();
    assert_eq!(rows.len(), 1, "exactly one record per call: {rows:?}");
    let LedgerEventKind::NetRequested {
        method,
        host,
        path,
        status: recorded,
        response_bytes,
        ..
    } = &rows[0]
    else {
        panic!("not a request row")
    };
    assert_eq!((method.as_str(), path.as_str()), ("GET", "/probe"));
    assert_eq!(host, &format!("127.0.0.1:{}", server.port));
    assert_eq!((*recorded, *response_bytes), (200, 4));
    let attributed = rig
        .ledger
        .kinds()
        .into_iter()
        .find(|(kind, _)| matches!(kind, LedgerEventKind::NetRequested { .. }))
        .and_then(|(_, fiber)| fiber);
    assert_eq!(attributed, Some(FiberId(7)), "attributed to the caller");
}

/// R12: the 0.1.0 declaration is PROVIDED, at its own signature and its
/// own broker wire — body in, body out, no header sent and no status
/// returned. It crosses the SAME authority and lands the SAME record as
/// the 0.2.0 edition: two handles, one door.
#[tokio::test]
async fn the_declared_request_shape_is_provided_at_the_same_door() {
    let server = target(ok_body("pong"));
    let rig = rig_pair(server.port);
    let body = rig
        .legacy("GET", &format!("http://127.0.0.1:{}/probe", server.port))
        .await
        .unwrap_or_else(|code| panic!("legacy request: {code:?}"));
    assert_eq!(body, b"pong", "the declared shape answers the body alone");
    let rows = rig.requested();
    assert_eq!(rows.len(), 1, "one call, one record: {rows:?}");
    let LedgerEventKind::NetRequested { method, path, .. } = &rows[0] else {
        panic!("not a request row")
    };
    assert_eq!((method.as_str(), path.as_str()), ("GET", "/probe"));
    // Same authority: an off-allowlist call through the OLD shape is
    // refused exactly like one through the new one.
    let elsewhere = target(ok_body("secret"));
    assert_eq!(
        rig.legacy("GET", &format!("http://127.0.0.1:{}/x", elsewhere.port))
            .await,
        Err(ErrorCode::EffectFailed),
        "the old shape is not a way around the allowlist"
    );
    assert_eq!(elsewhere.hits.load(Ordering::SeqCst), 0);
}

/// Law 2 vs 02 §Redaction: the record carries the call's SHAPE. A
/// credential-bearing header and a credential-bearing query string reach
/// the target and NEVER the ledger.
#[tokio::test]
async fn no_credential_header_or_query_string_reaches_the_ledger() {
    const SECRET: &str = "sk-live-0xDEADBEEF-fixture-secret";
    let server = target(ok_body("pong"));
    let rig = rig_pair(server.port);
    let (status, _, _) = rig
        .request(
            "POST",
            &format!(
                "http://127.0.0.1:{}/v1/keys?access_token={SECRET}",
                server.port
            ),
            &[("authorization".to_owned(), format!("Bearer {SECRET}"))],
            SECRET.as_bytes(),
        )
        .await
        .unwrap_or_else(|code| panic!("request: {code:?}"));
    assert_eq!(status, 200);
    let sent = String::from_utf8_lossy(
        &server
            .seen
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone(),
    )
    .into_owned();
    assert!(
        sent.contains(&format!("Bearer {SECRET}")),
        "the credential DID reach the target: {sent}"
    );
    let recorded = format!("{:?}", rig.ledger.kinds());
    assert!(
        !recorded.contains(SECRET),
        "no ledger row carries the credential: {recorded}"
    );
    let rows = rig.requested();
    let LedgerEventKind::NetRequested { path, method, .. } = &rows[0] else {
        panic!("not a request row")
    };
    assert_eq!(
        (method.as_str(), path.as_str()),
        ("POST", "/v1/keys"),
        "the path stops at the query"
    );
}

/// Every AUTHORIZED attempt lands an irreversible effect ID in the DURABLE
/// row — there is no second, in-memory register (R5). A refused call lands
/// none, and no two calls ever share an id.
#[tokio::test]
async fn every_authorized_attempt_records_a_distinct_irreversible_effect() {
    let server = target(ok_body("pong"));
    let rig = rig_pair(server.port);
    assert!(rig.requested().is_empty());
    assert_eq!(
        rig.get("http://127.0.0.1:1/nope").await,
        Err(ErrorCode::EffectFailed),
        "off the allowlist"
    );
    assert!(
        rig.requested().is_empty(),
        "a refused call is not an effect"
    );
    let url = format!("http://127.0.0.1:{}/probe", server.port);
    for _ in 0..2 {
        rig.get(&url)
            .await
            .unwrap_or_else(|code| panic!("request: {code:?}"));
    }
    let effects: Vec<u64> = rig
        .requested()
        .iter()
        .map(|kind| match kind {
            LedgerEventKind::NetRequested { effect, .. } => effect.0,
            _ => panic!("not a request row"),
        })
        .collect();
    assert_eq!(effects.len(), 2, "one effect per sent call");
    assert_ne!(effects[0], effects[1], "and never the same id twice");
    assert!(effects.iter().all(|effect| *effect > 0), "{effects:?}");
}
