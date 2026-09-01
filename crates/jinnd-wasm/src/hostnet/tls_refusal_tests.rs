//! M2-K15: the outbound TLS REFUSALS — three answers, never one blurred
//! one (R3), each proven through BOTH doors.
//!
//! The allowlist, the certificate, and the network are three different
//! NEXT MOVES for a caller: widen a profile, fix nothing and stop, retry.
//! A kernel that answered them alike would push that judgment onto the
//! guarded party, which is how M2-K14 lost its round-1 redirect property.

use std::sync::atomic::Ordering;

use jinnd_api::RefusalReason;

use super::outbound_rig_tests::{Answer, DOORS, ok_body, rig, rig_pair};
use super::tls_rig_tests::{Identity, tls_target};
use super::tls_tests::{BAD_CERTIFICATE, OFF_ALLOWLIST, TRANSPORT, url};

/// A certificate from an authority the kernel does not anchor is refused
/// through both doors — and the SAME target under an anchored certificate
/// answers, so the refusal is not vacuous.
#[tokio::test]
async fn both_doors_refuse_a_certificate_from_an_unanchored_authority() {
    for door in DOORS {
        let hostile = tls_target(Identity::Foreign, ok_body("secret"));
        let rig = rig_pair(hostile.port);
        assert_eq!(
            rig.door_get(door, &url(hostile.port, "/probe")).await,
            Err(BAD_CERTIFICATE),
            "{door:?}: an unanchored issuer is not an identity"
        );

        let honest = tls_target(Identity::Good, ok_body("secret"));
        let allowed = rig_pair(honest.port);
        assert_eq!(
            allowed
                .door_get(door, &url(honest.port, "/probe"))
                .await
                .unwrap_or_else(|code| panic!("{door:?}: precondition {code:?}")),
            b"secret",
            "{door:?}: the precondition holds — only the issuer differed"
        );
    }
}

/// An anchored certificate NAMED FOR ANOTHER HOST is refused: the anchor
/// says the issuer is trusted, never that this peer is the authority the
/// allowlist named.
#[tokio::test]
async fn both_doors_refuse_a_certificate_named_for_another_host() {
    for door in DOORS {
        let server = tls_target(Identity::WrongHost, ok_body("secret"));
        let rig = rig_pair(server.port);
        assert_eq!(
            rig.door_get(door, &url(server.port, "/probe")).await,
            Err(BAD_CERTIFICATE),
            "{door:?}: a trusted issuer is not a matched name"
        );
    }
}

/// An anchored, correctly named certificate that is OUT OF DATE is refused.
#[tokio::test]
async fn both_doors_refuse_an_expired_certificate() {
    for door in DOORS {
        let server = tls_target(Identity::Expired, ok_body("secret"));
        let rig = rig_pair(server.port);
        assert_eq!(
            rig.door_get(door, &url(server.port, "/probe")).await,
            Err(BAD_CERTIFICATE),
            "{door:?}: an expired certificate is not an identity"
        );
    }
}

/// THREE ANSWERS, not one blurred one, through both doors — and each
/// asserts its OWN precondition, so no case is passing for a neighbour's
/// reason: off the allowlist, bad certificate, and an authorized call the
/// network failed are three different next moves for the caller.
#[tokio::test]
async fn the_three_refusal_classes_stay_three_answers() {
    for door in DOORS {
        // 1. Off the allowlist: a target that WOULD have verified.
        let good = tls_target(Identity::Good, ok_body("pong"));
        let denied = rig(&["127.0.0.1:1"]);
        assert_eq!(
            denied.door_get(door, &url(good.port, "/probe")).await,
            Err(OFF_ALLOWLIST),
            "{door:?}: the allowlist"
        );
        assert_eq!(good.hits.load(Ordering::SeqCst), 0, "{door:?}: no dial");
        assert_eq!(denied.scope_refusals(), 1, "{door:?}: on the record");
        // Its own precondition: admitted, the same call answers.
        assert!(
            rig_pair(good.port)
                .door_get(door, &url(good.port, "/probe"))
                .await
                .is_ok(),
            "{door:?}: the allowlist case is not vacuous"
        );

        // 2. Admitted, reached, and refused on its certificate.
        let hostile = tls_target(Identity::Foreign, ok_body("secret"));
        let certificate = rig_pair(hostile.port);
        assert_eq!(
            certificate
                .door_get(door, &url(hostile.port, "/probe"))
                .await,
            Err(BAD_CERTIFICATE),
            "{door:?}: the certificate"
        );
        // Its own precondition: the connection WAS made — this is not the
        // allowlist refusing before the dial, and not a dead port.
        assert_eq!(
            hostile.hits.load(Ordering::SeqCst),
            1,
            "{door:?}: the peer was reached and still not believed"
        );

        // 3. Admitted, and the network failed: nothing listens.
        let dead = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|error| panic!("bind: {error}"));
        let port = dead
            .local_addr()
            .unwrap_or_else(|error| panic!("addr: {error}"))
            .port();
        drop(dead);
        assert_eq!(
            rig_pair(port).door_get(door, &url(port, "/probe")).await,
            Err(TRANSPORT),
            "{door:?}: the transport"
        );

        assert_ne!(OFF_ALLOWLIST, BAD_CERTIFICATE);
        assert_ne!(BAD_CERTIFICATE, TRANSPORT);
        assert_ne!(OFF_ALLOWLIST, TRANSPORT);
    }
}

/// The allowlist still binds ON REDIRECT when the hop is https: the
/// property M2-K14 proved over plain HTTP, re-proven over TLS rather than
/// assumed to carry.
#[tokio::test]
async fn both_doors_refuse_an_off_allowlist_https_redirect() {
    for door in DOORS {
        let elsewhere = tls_target(Identity::Good, ok_body("secret"));
        let hop = tls_target(
            Identity::Good,
            Answer::Raw(format!(
                "HTTP/1.1 302 Found\r\nlocation: https://127.0.0.1:{}/taken\r\ncontent-length: 0\r\n\r\n",
                elsewhere.port
            )),
        );
        let rig = rig_pair(hop.port);
        assert_eq!(
            rig.door_get(door, &url(hop.port, "/go")).await,
            Err(OFF_ALLOWLIST),
            "{door:?}: an https hop is still a hop"
        );
        assert_eq!(
            elsewhere.hits.load(Ordering::SeqCst),
            0,
            "{door:?}: never followed"
        );
        // The hop that produced it was really sent, and is on the record.
        assert_eq!(rig.effects().len(), 1, "{door:?}: the call is recorded");
    }
}

/// What TLS could newly leak is the peer's certificate — its subject, its
/// issuer, its bytes. The refusal names the AUTHORITY the caller already
/// knows and nothing the peer presented.
#[tokio::test]
async fn a_certificate_refusal_leaks_no_certificate_material() {
    for door in DOORS {
        let server = tls_target(Identity::Foreign, ok_body("secret"));
        let rig = rig_pair(server.port);
        let told = rig
            .door_get_told(door, &url(server.port, "/probe"))
            .await
            .err()
            .unwrap_or_else(|| panic!("{door:?}: expected a refusal"));
        assert_eq!(told.code, BAD_CERTIFICATE, "{door:?}");
        assert!(
            told.message.contains(&format!("127.0.0.1:{}", server.port)),
            "{door:?}: the refusal names the authority: {:?}",
            told.message
        );
        for leak in ["BEGIN CERTIFICATE", "jinnd untrusted issuer", "CN="] {
            assert!(
                !told.message.contains(leak),
                "{door:?}: {leak:?} in {:?}",
                told.message
            );
        }
    }
}

/// TLS lifts the loopback limit for `https` ONLY. Plaintext outbound stays
/// loopback-only: a kernel that would carry a credential in the clear to
/// an arbitrary host is not one TLS made safer.
#[tokio::test]
async fn plain_http_still_reaches_loopback_only() {
    for door in DOORS {
        let rig = rig(&["example.invalid:80"]);
        assert_eq!(
            rig.door_get(door, "http://example.invalid/probe").await,
            Err(OFF_ALLOWLIST),
            "{door:?}: plaintext off loopback is refused although GRANTED"
        );
        assert_eq!(
            rig.refusals(RefusalReason::NotLoopback),
            1,
            "{door:?}: refused as off-loopback, not as off-allowlist"
        );
        assert!(rig.requested().is_empty(), "{door:?}: nothing was sent");
    }
}

/// An https URL naming no port is `:443`, and a grant of `:80` does not
/// confer it — the allowlist matches one normal form, exactly (Law 1).
#[tokio::test]
async fn an_https_url_without_a_port_is_matched_at_443() {
    for door in DOORS {
        let at_80 = rig(&["example.invalid:80"]);
        assert_eq!(
            at_80.door_get(door, "https://example.invalid/probe").await,
            Err(OFF_ALLOWLIST),
            "{door:?}: :80 does not confer :443"
        );
        // The precondition: the same URL under a `:443` grant passes the
        // allowlist and is refused LATER, by the network, not the scope.
        let at_443 = rig(&["example.invalid:443"]);
        assert_eq!(
            at_443.door_get(door, "https://example.invalid/probe").await,
            Err(TRANSPORT),
            "{door:?}: :443 admits it, and only the network stops it"
        );
        assert_eq!(at_443.scope_refusals(), 0, "{door:?}: the scope admitted");
    }
}
