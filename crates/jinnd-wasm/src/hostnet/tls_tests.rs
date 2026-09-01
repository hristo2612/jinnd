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

use jinnd_api::{ErrorCode, LedgerEventKind};

use super::outbound_rig_tests::{DOORS, ok_body, rig_pair, target};
use super::tls_rig_tests::{Identity, tls_target};

/// The three refusal classes an outbound call answers with, as the caller
/// classifies them: the allowlist, the certificate, the network. Each is a
/// DIFFERENT next move, so each is a different answer (R3).
pub(super) const OFF_ALLOWLIST: ErrorCode = ErrorCode::EffectFailed;
pub(super) const BAD_CERTIFICATE: ErrorCode = ErrorCode::Untrusted;
pub(super) const TRANSPORT: ErrorCode = ErrorCode::PluginFailed;

pub(super) fn url(port: u16, path: &str) -> String {
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
            &format!(
                "https://127.0.0.1:{}/probe?access_token={secret}",
                server.port
            ),
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
