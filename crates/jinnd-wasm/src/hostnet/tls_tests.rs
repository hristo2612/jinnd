//! M2-K15: outbound TLS. What must hold beyond "https works".
//!
//! Every property runs the `DOORS` table — `request` and `send-request`
//! enter one `outbound()` core, and that is a claim about our own code, so
//! it is proven through BOTH doors and never on one with the other
//! assumed. That precedent is M2-K14's, where the legacy door's guarantees
//! were asserted by construction and a mutation showed the old suite
//! caught the allowlist hole with exactly one assertion and the redirect
//! hole with none.

use std::sync::atomic::Ordering;

use jinnd_api::{ErrorCode, LedgerEventKind, RefusalReason};

use super::outbound_rig_tests::{Answer, DOORS, ok_body, rig, rig_pair, target};
use super::tls_rig_tests::{Identity, tls_target};

/// The three refusal classes an outbound call answers with, as the caller
/// classifies them: the allowlist, the certificate, the network. Each is a
/// DIFFERENT next move, so each is a different answer (R3).
const OFF_ALLOWLIST: ErrorCode = ErrorCode::EffectFailed;
const BAD_CERTIFICATE: ErrorCode = ErrorCode::Untrusted;
const TRANSPORT: ErrorCode = ErrorCode::PluginFailed;

/// Every Rust source file in this crate, as `(path, text)`.
///
/// Read off disk rather than named one by one: a scan that must be
/// extended by hand to cover a new file is a scan that stops covering the
/// crate the day someone forgets. Tests run from the source checkout, so
/// `CARGO_MANIFEST_DIR` is that crate.
fn sources() -> Vec<(String, String)> {
    fn walk(directory: &std::path::Path, into: &mut Vec<(String, String)>) {
        let entries = std::fs::read_dir(directory)
            .unwrap_or_else(|error| panic!("read {}: {error}", directory.display()));
        for entry in entries {
            let path = entry
                .unwrap_or_else(|error| panic!("entry: {error}"))
                .path();
            if path.is_dir() {
                walk(&path, into);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                let text = std::fs::read_to_string(&path)
                    .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
                into.push((path.display().to_string(), text));
            }
        }
    }
    let mut sources = Vec::new();
    walk(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut sources,
    );
    assert!(sources.len() > 20, "the walk found the crate");
    sources
}

fn url(port: u16, path: &str) -> String {
    format!("https://127.0.0.1:{port}{path}")
}

/// Both doors reach a real HTTPS target, over a verified certificate, and
/// each lands ONE row carrying a nonzero effect id — the id that makes the
/// irreversibility durable.
#[tokio::test]
async fn both_doors_reach_an_https_target() {
    for door in DOORS {
        let server = tls_target(Identity::Good, ok_body("pong"));
        let rig = rig_pair(server.port);
        let body = rig
            .door_get(door, &url(server.port, "/probe"))
            .await
            .unwrap_or_else(|code| panic!("{door:?}: {code:?}"));
        assert_eq!(body, b"pong", "{door:?}");
        let effects = rig.effects();
        assert_eq!(effects.len(), 1, "{door:?}: one call, one row");
        assert!(effects[0] > 0, "{door:?}: the row names an effect");
        assert_eq!(server.hits.load(Ordering::SeqCst), 1, "{door:?}");
    }
}

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
            certificate.door_get(door, &url(hostile.port, "/probe")).await,
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

/// An https call is IRREVERSIBLE exactly as a plain one: one row, the
/// effect id on it, and the row REDACTED — the shape of the call, never
/// its content. Re-proven over TLS rather than inherited from M2-K14.
#[tokio::test]
async fn an_https_call_is_recorded_irreversible_and_redacted() {
    for door in DOORS {
        let server = tls_target(Identity::Good, ok_body("pong"));
        let rig = rig_pair(server.port);
        let secret = "s3cr3t-token";
        rig.through(
            door,
            "POST",
            &format!("https://127.0.0.1:{}/probe?access_token={secret}", server.port),
            b"body bytes",
        )
        .await
        .unwrap_or_else(|code| panic!("{door:?}: {code:?}"));
        let rows = rig.requested();
        assert_eq!(rows.len(), 1, "{door:?}");
        let LedgerEventKind::NetRequested {
            effect,
            method,
            host,
            path,
            status,
            ..
        } = &rows[0]
        else {
            panic!("{door:?}: not a request row")
        };
        assert!(effect.0 > 0, "{door:?}: irreversible, durably");
        assert_eq!(method, "POST", "{door:?}");
        assert_eq!(host, &format!("127.0.0.1:{}", server.port), "{door:?}");
        assert_eq!(path, "/probe", "{door:?}: never the query string");
        assert_eq!(*status, 200, "{door:?}");
        let row = format!("{:?}", rows[0]);
        assert!(!row.contains(secret), "{door:?}: no credential on the row");
        assert!(!row.contains("body bytes"), "{door:?}: no body on the row");
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
            told.message
                .contains(&format!("127.0.0.1:{}", server.port)),
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

/// A plain-HTTP loopback target still answers over both doors: TLS is a
/// second transport beside the M2-K14 one, never a replacement for it.
#[tokio::test]
async fn plain_loopback_http_still_answers() {
    for door in DOORS {
        let server = target(ok_body("pong"));
        let rig = rig_pair(server.port);
        assert_eq!(
            rig.door_get(door, &format!("http://127.0.0.1:{}/probe", server.port))
                .await
                .unwrap_or_else(|code| panic!("{door:?}: {code:?}")),
            b"pong",
            "{door:?}"
        );
    }
}

/// CERTIFICATE VERIFICATION HAS NO OFF SWITCH, and this asserts it rather
/// than inspecting for it.
///
/// rustls puts every way to weaken verification behind two named doors:
/// the escape-hatch configuration accessor, and a hand-written server
/// certificate verifier. Neither appears in this crate, in production code
/// or in test code, so there is no path from a profile, an environment
/// variable, or a plugin to a client that skips verification: the test
/// certificates are trusted because a test ANCHOR was added, which is a
/// different thing entirely.
///
/// The needles are assembled from fragments, and this file never spells
/// one out even in prose — a scan a comment can defeat is not a scan.
#[test]
fn certificate_verification_has_no_off_switch_anywhere_in_this_crate() {
    // The API tokens, never the English words: a scan that fires on the
    // word "dangerous" in an unrelated doc comment is a scan nobody keeps.
    let forbidden = [
        concat!("dang", "erous()"),
        concat!("Dang", "erousClientConfig"),
        concat!("Server", "CertVerifier"),
        concat!("Server", "CertVerified"),
        concat!("accept_invalid", "_certs"),
        concat!("with_custom_certificate", "_verifier"),
    ];
    for (path, source) in sources() {
        for needle in forbidden {
            assert!(
                !source.contains(needle),
                "{path} names {needle:?}: verification must have no off switch"
            );
        }
    }
}

/// The ONE test-only seam — the extra trust anchor — is `#[cfg(test)]` on
/// every occurrence, so it cannot exist in a release build. Asserted by
/// reading the source, because "I checked" is not evidence.
#[test]
fn the_extra_anchor_seam_is_cfg_test_at_every_occurrence() {
    let seam = concat!("extra_", "anchors");
    let mut found = 0;
    for (path, source) in sources() {
        let lines: Vec<&str> = source.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if !line.contains(seam) || line.trim_start().starts_with("//") {
                continue;
            }
            found += 1;
            let guard = lines[..index]
                .iter()
                .rev()
                .find(|earlier| !earlier.trim().is_empty())
                .map(|earlier| earlier.trim())
                .unwrap_or_default();
            assert_eq!(
                guard, "#[cfg(test)]",
                "{path}:{}: the anchor seam is not test-only",
                index + 1
            );
        }
    }
    assert_eq!(found, 2, "the seam is its declaration and its one call");
}
