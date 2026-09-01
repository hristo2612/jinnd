//! The kernel's outbound TLS (M2-K15): one client configuration, built
//! once, with verification permanently on.
//!
//! THE STACK, AND WHY (R10 — small and boring). rustls with the `ring`
//! provider, over tokio-rustls, verifying against the VENDORED
//! `webpki-roots` public root bundle. Eight crates enter the tree, the
//! same eight on every platform, and none of them is a C library or needs
//! a C toolchain. The alternatives were measured, not guessed: `native-tls`
//! adds five crates on macOS and eight DIFFERENT ones on Linux — including
//! OpenSSL and its `-sys` shim — so its verification semantics vary by
//! platform, which is precisely the class of thing M2-K12 caught going red
//! on Linux behind three days of honest macOS greens. A full HTTP client
//! (`reqwest`) would have been a third option and is the worst of them
//! here: it replaces the HTTP/1.1 layer M2-K14 already owns, and it
//! follows redirects by default — the exact behaviour the allowlist
//! depends on us NOT having (`admit_hop`).
//!
//! WHY THIS IS NOT A PLUGIN (R10). TLS is the transport under an effect
//! the kernel already owns end to end: the allowlist decides the peer, the
//! ledger records the call, and Law 3 declares it irreversible. A plugin
//! doing the handshake would have to be handed the plaintext of every
//! outbound call — every credential the keystore exists to protect — and
//! would then hold, off-ledger, the decision about whether the peer is
//! genuine. That is authority, not a service, and Law 1 does not admit a
//! side door for it. What CAN be a plugin here already is: which hosts a
//! profile may reach is data, not code.
//!
//! WHY VENDORED ANCHORS AND NOT THE PLATFORM STORE. Law 4 — a device is a
//! profile — makes trust anchors kernel behaviour, so they must not
//! silently differ between a laptop, a server, and CI. A vendored bundle
//! is one named, version-pinned set an audit reads out of the lock file,
//! and it adds no platform-specific code path. The cost, stated rather
//! than hidden: an enterprise root installed in the OS store is not
//! trusted, and rotating anchors is a dependency bump. Under the packet's
//! threat model — an ordinary hostile or misconfigured peer, not a
//! compromised public root — that trade runs the right way.
//!
//! Nothing here blocks (R1): the handshake is one await inside the call's
//! own bound.

use std::sync::{Arc, OnceLock};

use jinnd_api::{ErrorCode, KernelError};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, RootCertStore};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;

use super::http;
use crate::broker_state::refusal;

/// A peer that did not PROVE it is the authority the allowlist named.
///
/// The third answer, distinct from the allowlist's `denied` and the
/// network's `failed`, because it is a third next move: a caller widens a
/// profile, retries a network, and does NEITHER with this one (R3).
pub(super) fn untrusted(detail: String) -> KernelError {
    refusal(ErrorCode::Untrusted, format!("net request: {detail}"))
}

/// The trust anchors every outbound handshake is verified against: the
/// vendored public roots, and in a test build the one extra anchor the
/// suite adds so a local target can face the SAME verification a public
/// host faces.
fn anchors() -> RootCertStore {
    let mut store = RootCertStore::empty();
    store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    #[cfg(test)]
    extra_anchors(&mut store);
    store
}

/// THE ONE TEST-ONLY SEAM (M2-K15). It adds trust anchors; it never
/// weakens verification, and there is no code in this crate that can —
/// rustls' bypass door is not called anywhere and no verifier of our own
/// exists. `#[cfg(test)]`, so it is not in a release build at all.
#[cfg(test)]
fn extra_anchors(store: &mut RootCertStore) {
    for anchor in super::tls_rig_tests::anchor() {
        store
            .add(anchor)
            .unwrap_or_else(|error| panic!("test anchor: {error}"));
    }
}

/// The provider's ONE client configuration. Built once and shared: there
/// is no per-call, per-profile, or per-plugin variant to weaken.
fn config() -> Result<Arc<ClientConfig>, KernelError> {
    static CONFIG: OnceLock<Result<Arc<ClientConfig>, String>> = OnceLock::new();
    CONFIG
        .get_or_init(|| {
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            ClientConfig::builder_with_provider(provider)
                // TLS 1.2 and 1.3 only — a downgrade has nothing to reach.
                .with_safe_default_protocol_versions()
                .map(|builder| {
                    Arc::new(
                        builder
                            .with_root_certificates(anchors())
                            .with_no_client_auth(),
                    )
                })
                .map_err(|error| error.to_string())
        })
        .clone()
        .map_err(|error| http::failed(format!("no tls client configuration: {error}")))
}

/// Completes the TLS handshake to `host` over an established `stream`,
/// verifying the peer's certificate chain against the anchors and its name
/// against `host`.
///
/// # Errors
///
/// [`untrusted`] naming `authority` when the peer failed to authenticate —
/// an unanchored issuer, a name the certificate does not cover, a
/// certificate outside its dates. [`http::failed`] for every other
/// handshake failure, so an identity refusal is never confused with a
/// network one. Neither message carries anything the peer presented: the
/// authority named is the one the CALLER already knows.
pub(super) async fn connect(
    host: &str,
    authority: &str,
    stream: TcpStream,
) -> Result<TlsStream<TcpStream>, KernelError> {
    let name = ServerName::try_from(host.to_owned())
        .map_err(|_| http::invalid(format!("{host:?} is not a server name TLS can verify")))?;
    let connector = TlsConnector::from(config()?);
    connector.connect(name, stream).await.map_err(|error| {
        if identity_failure(&error) {
            untrusted(format!(
                "{authority} did not prove it is {authority}: its certificate failed verification"
            ))
        } else {
            http::failed(format!("tls handshake with {authority} failed"))
        }
    })
}

/// Whether a handshake failure was the peer failing to AUTHENTICATE, as
/// opposed to the connection failing underneath it.
///
/// Deliberately narrow. Only rustls' certificate verdicts and a peer that
/// presented nothing count as `untrusted`; a reset, a timeout, a protocol
/// mismatch, or an alert is the network's `failed`. Widening this would
/// make `untrusted` the catch-all the packet exists to prevent, and
/// narrowing it would report an unverified peer as a retryable blip.
fn identity_failure(error: &std::io::Error) -> bool {
    matches!(
        error.get_ref().and_then(|inner| inner.downcast_ref()),
        Some(rustls::Error::InvalidCertificate(_) | rustls::Error::NoCertificatesPresented)
    )
}
