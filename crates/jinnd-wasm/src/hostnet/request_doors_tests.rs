//! EVERY property, through BOTH doors (COO round-2 steer).
//!
//! `request` and `send-request` share one `outbound()` core, so "same
//! authority, same record, same irreversibility" is true by construction —
//! but that is a claim about our own code, and it is the exact shape that
//! produced this packet's round-1 R5 defect: a guarantee that held for the
//! path under consideration and quietly did not hold for the other one.
//!
//! So nothing here is verified on the new door and assumed on the old.
//! Each case runs the table `DOORS`, and a failure names which door broke
//! it. A legacy door that skipped the allowlist would be worse than no
//! legacy door at all, and "it always failed before" is precisely the
//! reasoning that lost round 1 its R12 blocker.

use std::sync::atomic::Ordering;

use jinnd_api::{ErrorCode, LedgerEventKind};

use super::outbound_rig_tests::{DOORS, ok_body, redirector, rig, rig_pair, target};

/// Both doors reach an admitted authority, and each lands ONE row carrying
/// a nonzero effect id — the id that makes the refusal durable.
#[tokio::test]
async fn both_doors_answer_and_record_an_effect_id() {
    for door in DOORS {
        let server = target(ok_body("pong"));
        let rig = rig_pair(server.port);
        let body = rig
            .door_get(door, &format!("http://127.0.0.1:{}/probe", server.port))
            .await
            .unwrap_or_else(|code| panic!("{door:?}: {code:?}"));
        assert_eq!(body, b"pong", "{door:?}");
        let effects = rig.effects();
        assert_eq!(effects.len(), 1, "{door:?}: one call, one row");
        assert!(effects[0] > 0, "{door:?}: the row names an effect");
        assert_eq!(server.hits.load(Ordering::SeqCst), 1, "{door:?}");
    }
}

/// Both doors are refused off the allowlist, on the record, without ever
/// dialling — and the SAME call through the SAME door under a grant that
/// admits it succeeds, so neither refusal is vacuous.
#[tokio::test]
async fn both_doors_refuse_an_off_allowlist_authority() {
    for door in DOORS {
        let server = target(ok_body("pong"));
        let url = format!("http://127.0.0.1:{}/probe", server.port);
        let refused = rig(&["127.0.0.1:1"]);
        assert_eq!(
            refused.door_get(door, &url).await,
            Err(ErrorCode::EffectFailed),
            "{door:?} is not a way around the allowlist"
        );
        assert_eq!(server.hits.load(Ordering::SeqCst), 0, "{door:?}: no dial");
        assert_eq!(refused.scope_refusals(), 1, "{door:?}: on the record");
        assert!(refused.requested().is_empty(), "{door:?}: no sent call");

        let allowed = rig_pair(server.port);
        assert!(
            allowed.door_get(door, &url).await.is_ok(),
            "{door:?}: the precondition holds"
        );
        assert_eq!(server.hits.load(Ordering::SeqCst), 1, "{door:?}");
    }
}

/// Both doors refuse an off-allowlist REDIRECT inside the call — the
/// property round 1 got wrong on the only door it had. Neither hands the
/// caller a hop it is trusted not to take; both still record the call that
/// produced it.
#[tokio::test]
async fn both_doors_refuse_an_off_allowlist_redirect_inside_the_call() {
    for door in DOORS {
        let elsewhere = target(ok_body("secret"));
        let hop = redirector(302, elsewhere.port);
        let rig = rig_pair(hop.port);
        assert_eq!(
            rig.door_get(door, &format!("http://127.0.0.1:{}/go", hop.port))
                .await,
            Err(ErrorCode::EffectFailed),
            "{door:?} handed back an off-allowlist hop"
        );
        assert_eq!(
            elsewhere.hits.load(Ordering::SeqCst),
            0,
            "{door:?}: the redirect target was never dialled"
        );
        let rows = rig.requested();
        assert_eq!(rows.len(), 1, "{door:?}: the sent call is on the record");
        let LedgerEventKind::NetRequested { status, effect, .. } = &rows[0] else {
            panic!("not a request row")
        };
        assert_eq!(*status, 302, "{door:?}: recorded as what it was");
        assert!(effect.0 > 0, "{door:?}: and still names an effect");
        assert_eq!(rig.scope_refusals(), 1, "{door:?}: the refusal is typed");
    }
}

/// Both doors keep `invalid` a DISTINCT reading from `denied`: a URL the
/// provider cannot make sense of is never a grant refusal, through either.
#[tokio::test]
async fn both_doors_keep_invalid_distinct_from_denied() {
    for door in DOORS {
        let rig = rig(&["127.0.0.1:1"]);
        for bad in [
            "not a url",
            // M2-K15: `https` is PROVIDED from 0.3.0, so the unsupported
            // scheme that stands for "cannot make sense of it" is one the
            // contract genuinely does not name. The reading is unchanged;
            // only the example moved with the contract.
            "ftp://127.0.0.1:1/x",
            "http://",
            "https://",
            "http://user:pw@127.0.0.1:1/x",
            "https://user:pw@127.0.0.1:1/x",
            "http://127.0.0.1:notaport/x",
            "https://127.0.0.1:notaport/x",
        ] {
            assert_eq!(
                rig.door_get(door, bad).await,
                Err(ErrorCode::InvalidProfile),
                "{door:?} {bad}"
            );
        }
        assert_eq!(rig.scope_refusals(), 0, "{door:?}: not a grant refusal");
        assert!(rig.requested().is_empty(), "{door:?}: nothing was sent");
        // And the third reading stays third: authorized, nothing listening.
        assert_eq!(
            rig.door_get(door, "http://127.0.0.1:1/x").await,
            Err(ErrorCode::PluginFailed),
            "{door:?}: a network failure is its own answer"
        );
    }
}

/// Both doors redact. The 0.1.0 shape sends no header at all, so the
/// header case is structurally impossible there — but a query string
/// carries a credential just as readily, and THAT reaches both doors.
#[tokio::test]
async fn neither_door_writes_a_credential_to_the_ledger() {
    const SECRET: &str = "sk-live-0xDEADBEEF-fixture-secret";
    for door in DOORS {
        let server = target(ok_body("pong"));
        let rig = rig_pair(server.port);
        rig.through(
            door,
            "POST",
            &format!(
                "http://127.0.0.1:{}/v1/keys?access_token={SECRET}",
                server.port
            ),
            SECRET.as_bytes(),
        )
        .await
        .unwrap_or_else(|code| panic!("{door:?}: {code:?}"));
        let sent = String::from_utf8_lossy(
            &server
                .seen
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .clone(),
        )
        .into_owned();
        assert!(
            sent.contains("access_token="),
            "{door:?}: the credential DID reach the target: {sent}"
        );
        let recorded = format!("{:?}", rig.ledger.kinds());
        assert!(
            !recorded.contains(SECRET),
            "{door:?}: a row carries the credential: {recorded}"
        );
        let LedgerEventKind::NetRequested { path, method, .. } = &rig.requested()[0] else {
            panic!("not a request row")
        };
        assert_eq!(
            (method.as_str(), path.as_str()),
            ("POST", "/v1/keys"),
            "{door:?}: the path stops at the query"
        );
    }
}

/// The two doors share ONE effect counter: calls through both, interleaved,
/// never collide. An id names one call whichever handle opened it.
#[tokio::test]
async fn the_doors_share_one_effect_counter() {
    let server = target(ok_body("pong"));
    let rig = rig_pair(server.port);
    let url = format!("http://127.0.0.1:{}/probe", server.port);
    for door in [DOORS[0], DOORS[1], DOORS[0], DOORS[1]] {
        rig.door_get(door, &url)
            .await
            .unwrap_or_else(|code| panic!("{door:?}: {code:?}"));
    }
    let effects = rig.effects();
    assert_eq!(effects.len(), 4);
    let mut sorted = effects.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), 4, "no id names two calls: {effects:?}");
}
