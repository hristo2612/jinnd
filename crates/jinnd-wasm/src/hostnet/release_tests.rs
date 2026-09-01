//! M2-K20: a listener's release, pinned as the property the kernel owns —
//! the descriptor is dropped (closed) before `withdraw` returns, and no
//! wake of the handle is ever ledgered again — and NOT as the property the
//! box owns, that nothing else answers the port afterwards. The flood test
//! in `tests.rs` asserted the latter (`TcpStream::connect(..).is_err()`,
//! "closed at release") and failed nondeterministically: Rust's
//! `TcpListener::bind` sets `SO_REUSEADDR`, and under BSD bind rules a
//! specific `127.0.0.1:P` listener coexists with a wildcard `*:P` bound by
//! number — the most specific answers while both live, and once the
//! kernel's is released a connect reaches the foreigner. The window is
//! forced open here on the platform whose rules admit it; Linux refuses a
//! wildcard beside a listening specific (`EADDRINUSE`), so that supply and
//! this flake do not exist there.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::tests::{Face, descriptor_of, rig, settle, with_handle};

/// The foreign listener the box can supply: a wildcard bind by number on
/// the kernel's port, beside the kernel's live specific listener.
#[cfg(target_os = "macos")]
fn foreign_wildcard(port: u16) -> std::net::TcpListener {
    let foreign = std::net::TcpListener::bind(("0.0.0.0", port))
        .unwrap_or_else(|error| panic!("a wildcard beside a live specific listener: {error}"));
    foreign
        .set_nonblocking(true)
        .unwrap_or_else(|error| panic!("{error}"));
    foreign
}

/// Whether `foreign` has a connection to accept within `wait`.
#[cfg(target_os = "macos")]
fn accepts_within(foreign: &std::net::TcpListener, wait: Duration) -> bool {
    let deadline = Instant::now() + wait;
    loop {
        match foreign.accept() {
            Ok(_) => return true,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() > deadline {
                    return false;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("foreign accept: {error}"),
        }
    }
}

/// The window the flake lived in, forced open: with a foreign wildcard
/// listener on the kernel's port, every connect reaches the kernel's
/// listener while it lives and the foreigner only once it is released —
/// the kernel's contract holds throughout (descriptor dropped before the
/// release returns; no wake ledgered after it), and the connect the old
/// assertion expected to be refused is answered, by someone else.
#[cfg(target_os = "macos")]
#[tokio::test]
async fn a_foreign_wildcard_listener_answers_the_released_port_not_the_kernel() {
    let rig = rig();
    let face = Arc::new(Face(Mutex::new(Vec::new())));
    rig.broker.attach_target(
        rig.guest,
        Arc::clone(&face) as Arc<dyn crate::topics::EventTarget>,
    );
    let listener = rig.listen().await;
    let foreign = foreign_wildcard(rig.port);
    let descriptor = descriptor_of(&rig, listener);

    let _client = std::net::TcpStream::connect(("127.0.0.1", rig.port))
        .unwrap_or_else(|error| panic!("{error}"));
    let conn = rig
        .accept(listener)
        .await
        .unwrap_or_else(|| panic!("the kernel's listener answers while it lives"));
    settle(|| face.wakes(listener) == 1).await;
    assert!(
        !accepts_within(&foreign, Duration::from_millis(50)),
        "the most specific listener answers, not the foreigner"
    );
    rig.call(rig.guest, "close", with_handle(conn, &[]))
        .await
        .unwrap_or_else(|code| panic!("close: {code:?}"));

    let wakes = rig.ledger.readable_wakes(listener);
    rig.provider
        .withdraw(listener)
        .await
        .unwrap_or_else(|error| panic!("release: {error:?}"));
    assert!(
        descriptor.upgrade().is_none(),
        "the descriptor is dropped — closed — before the release returns"
    );
    let _stray = std::net::TcpStream::connect(("127.0.0.1", rig.port))
        .unwrap_or_else(|error| panic!("the foreigner answers the released port: {error}"));
    assert!(
        accepts_within(&foreign, Duration::from_millis(500)),
        "the connect landed on the foreign wildcard listener"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        rig.ledger.readable_wakes(listener),
        wakes,
        "no wake after release"
    );
    assert_eq!(face.wakes(listener), 1, "nothing delivered after release");
    assert_eq!(rig.provider.live(), 0);
}

/// Every platform: across many releases, the descriptor is gone the
/// moment `withdraw` returns — the property the old probe stood in for.
#[tokio::test]
async fn a_release_drops_the_descriptor_before_it_returns_every_time() {
    let rig = rig();
    for round in 0..32 {
        let listener = rig.listen().await;
        let descriptor = descriptor_of(&rig, listener);
        rig.provider
            .withdraw(listener)
            .await
            .unwrap_or_else(|error| panic!("release {round}: {error:?}"));
        assert!(
            descriptor.upgrade().is_none(),
            "round {round}: the descriptor outlived its release"
        );
    }
    assert_eq!(rig.provider.live(), 0);
}
