//! The TLS loopback target and the trust anchors the M2-K15 pins share.
//!
//! CERTIFICATE VERIFICATION IS NEVER DISABLED — not in production, and not
//! here. rustls' verification-bypass door is not called anywhere in this
//! crate, and no custom verifier exists; a test that needs a certificate
//! the kernel accepts gets one SIGNED BY A CERTIFICATE AUTHORITY THE TEST
//! ADDS AS AN ANCHOR, through the one `#[cfg(test)]` seam in `tls.rs`. So
//! the untrusted cases are untrusted for the real reason: the peer failed
//! the same verification a public host faces.
//!
//! One authority per test binary ([`anchor`]), because the client config
//! is built once. Everything the tests want to be REFUSED is signed by a
//! second authority that is never an anchor, or is malformed in time or in
//! name.

use std::sync::{Arc, Mutex, OnceLock};

use rcgen::{
    BasicConstraints, Certificate, CertificateParams, IsCa, Issuer, KeyPair, KeyUsagePurpose,
    date_time_ymd,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

use super::outbound_rig_tests::{Answer, Target};

/// One certificate authority and the leaves it signs.
pub(super) struct Authority {
    params: CertificateParams,
    key: KeyPair,
    der: CertificateDer<'static>,
}

fn authority(name: &str) -> Authority {
    let key = KeyPair::generate().unwrap_or_else(|error| panic!("ca key: {error}"));
    let mut params =
        CertificateParams::new(Vec::new()).unwrap_or_else(|error| panic!("ca params: {error}"));
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, name);
    let der = params
        .self_signed(&key)
        .unwrap_or_else(|error| panic!("ca cert: {error}"))
        .der()
        .clone();
    Authority { params, key, der }
}

impl Authority {
    /// One leaf for `names`, valid over `[not_before, not_after]`.
    fn leaf(&self, names: &[&str], not_before: (i32, u8, u8), not_after: (i32, u8, u8)) -> Leaf {
        let key = KeyPair::generate().unwrap_or_else(|error| panic!("leaf key: {error}"));
        let mut params = CertificateParams::new(
            names
                .iter()
                .map(|name| (*name).to_owned())
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|error| panic!("leaf params: {error}"));
        params.not_before = date_time_ymd(not_before.0, not_before.1, not_before.2);
        params.not_after = date_time_ymd(not_after.0, not_after.1, not_after.2);
        let issuer = Issuer::from_params(&self.params, &self.key);
        let cert: Certificate = params
            .signed_by(&key, &issuer)
            .unwrap_or_else(|error| panic!("leaf cert: {error}"));
        Leaf {
            chain: vec![cert.der().clone(), self.der.clone()],
            key: PrivateKeyDer::try_from(key.serialize_der())
                .unwrap_or_else(|error| panic!("leaf key der: {error}")),
        }
    }
}

/// One server identity: the chain it presents and the key it proves.
pub(super) struct Leaf {
    chain: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

/// THE test authority: the ONE extra trust anchor this test binary adds,
/// through the `#[cfg(test)]` seam in `tls.rs`. Nothing else is trusted
/// beyond the vendored public roots.
fn trusted() -> &'static Authority {
    static TRUSTED: OnceLock<Authority> = OnceLock::new();
    TRUSTED.get_or_init(|| authority("jinnd test anchor"))
}

/// A second authority that is NEVER an anchor: what a self-signed or
/// foreign-issued certificate looks like to the kernel.
fn foreign() -> &'static Authority {
    static FOREIGN: OnceLock<Authority> = OnceLock::new();
    FOREIGN.get_or_init(|| authority("jinnd untrusted issuer"))
}

/// The extra anchors the test seam feeds the client config.
pub(super) fn anchor() -> Vec<CertificateDer<'static>> {
    vec![trusted().der.clone()]
}

/// What a TLS target proves about itself.
#[derive(Clone, Copy, Debug)]
pub(super) enum Identity {
    /// Anchored, in date, and named for loopback: the kernel accepts it.
    Good,
    /// Signed by an authority the kernel does not anchor.
    Foreign,
    /// Anchored and in date, but named for a host that is not the target.
    WrongHost,
    /// Anchored and correctly named, but out of date.
    Expired,
}

fn identity(identity: Identity) -> Leaf {
    const FAR: (i32, u8, u8) = (2999, 1, 1);
    const NEAR: (i32, u8, u8) = (2000, 1, 1);
    match identity {
        Identity::Good => trusted().leaf(&["127.0.0.1", "localhost"], NEAR, FAR),
        Identity::Foreign => foreign().leaf(&["127.0.0.1", "localhost"], NEAR, FAR),
        Identity::WrongHost => trusted().leaf(&["elsewhere.invalid"], NEAR, FAR),
        Identity::Expired => trusted().leaf(&["127.0.0.1", "localhost"], NEAR, (2001, 1, 1)),
    }
}

/// A loopback HTTPS target proving `identity` and answering `answer`.
///
/// Its own runtime on its own thread, so a test drives it from the kernel's
/// runtime without either one owning the other.
pub(super) fn tls_target(identity_of: Identity, answer: Answer) -> Target {
    let leaf = identity(identity_of);
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap_or_else(|error| panic!("server versions: {error}"))
    .with_no_client_auth()
    .with_single_cert(leaf.chain, leaf.key)
    .unwrap_or_else(|error| panic!("server cert: {error}"));
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap_or_else(|error| panic!("bind: {error}"));
    let port = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("addr: {error}"))
        .port();
    let target = Target::empty(port);
    let (hits, seen) = (Arc::clone(&target.hits), Arc::clone(&target.seen));
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap_or_else(|error| panic!("runtime: {error}"));
        runtime.block_on(serve(listener, acceptor, answer, hits, seen));
    });
    target
}

async fn serve(
    listener: std::net::TcpListener,
    acceptor: tokio_rustls::TlsAcceptor,
    answer: Answer,
    hits: Arc<std::sync::atomic::AtomicUsize>,
    seen: Arc<Mutex<Vec<u8>>>,
) {
    listener
        .set_nonblocking(true)
        .unwrap_or_else(|error| panic!("nonblocking: {error}"));
    let listener = tokio::net::TcpListener::from_std(listener)
        .unwrap_or_else(|error| panic!("adopt: {error}"));
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // A handshake the client refuses ends here, counted: the test can
        // prove the connection WAS made and the answer still withheld.
        let Ok(mut stream) = acceptor.accept(stream).await else {
            continue;
        };
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            match stream.read(&mut byte).await {
                Ok(0) | Err(_) => break,
                Ok(_) => head.push(byte[0]),
            }
        }
        seen.lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .extend(head);
        match &answer {
            Answer::Raw(text) => {
                let _ = stream.write_all(text.as_bytes()).await;
            }
            Answer::Body(size) => {
                let _ = stream
                    .write_all(
                        format!("HTTP/1.1 200 OK\r\ncontent-length: {size}\r\n\r\n").as_bytes(),
                    )
                    .await;
                let _ = stream.write_all(&vec![b'x'; *size]).await;
            }
            Answer::Silent => tokio::time::sleep(std::time::Duration::from_secs(30)).await,
        }
        let _ = stream.shutdown().await;
    }
}
