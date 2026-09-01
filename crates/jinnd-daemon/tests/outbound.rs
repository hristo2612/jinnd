//! M2-K14 acceptance, through the real daemon assembly: a fixture plugin
//! makes a real outbound call to a loopback HTTP target it was granted,
//! and every other reading is a DIFFERENT answer — an off-allowlist
//! authority denied without a dial, a malformed URL invalid, a redirect
//! off the allowlist answered and never followed. The Law-2 record carries
//! the call's shape and none of its credentials, an entry cannot widen its
//! own allowlist, and a revert unit containing the call is REJECTED WHOLE
//! with nothing in it applied (Law 2, Law 3, R1, R3, R9).

mod support;

use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use jinnd_api::{ErrorCode, LedgerEventKind, LedgerRecord};
use jinnd_daemon::{Daemon, DaemonPaths, UnitMember};

/// The secret the fixture carries in an `authorization` header AND in a
/// query string: the test greps the whole ledger file for these bytes.
const SECRET: &str = "sk-live-0xDEADBEEF-fixture-secret";

struct Home(PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn home(name: &str) -> Home {
    let root =
        std::env::temp_dir().join(format!("jinnd-outbound-test-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
    Home(root)
}

fn paths(home: &Home, grants: serde_json::Value, mode: &str) -> DaemonPaths {
    let (bytes, hash) = support::pinned_fixture();
    std::fs::write(home.0.join("artifacts/counter-plugin.wasm"), &bytes)
        .unwrap_or_else(|error| panic!("{error}"));
    let profile = serde_json::json!({ "entries": [{
        "id": "worker",
        "package": "demo/counter-plugin",
        "version": "0.0.1",
        "hash": hash,
        "config": { "grants": grants, "data": mode },
    }]});
    std::fs::write(
        home.0.join("profile.json"),
        serde_json::to_string_pretty(&profile).unwrap_or_else(|error| panic!("{error}")),
    )
    .unwrap_or_else(|error| panic!("{error}"));
    DaemonPaths {
        profile: home.0.join("profile.json"),
        ledger: home.0.join("ledger.sqlite"),
        artifacts: home.0.join("artifacts"),
        data: home.0.join("data"),
    }
}

async fn booted(paths: DaemonPaths) -> Daemon {
    let daemon = Daemon::open(paths).unwrap_or_else(|error| panic!("open: {error:?}"));
    let report = daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot: {error:?}"));
    assert!(report.errors.is_empty(), "clean boot: {:?}", report.errors);
    daemon
}

async fn events(daemon: &Daemon) -> Vec<LedgerRecord> {
    daemon
        .ledger_events()
        .await
        .unwrap_or_else(|error| panic!("ledger read: {error:?}"))
}

/// A loopback HTTP target: `/probe` answers 200 `pong`, `/redirect`
/// answers a 302 pointing at `elsewhere`. `hits` counts every accepted
/// connection, so a test can prove a target was never dialled.
struct Target {
    port: u16,
    hits: Arc<AtomicUsize>,
}

fn target(elsewhere: Option<u16>) -> Target {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").unwrap_or_else(|error| panic!("bind: {error}"));
    let port = listener
        .local_addr()
        .unwrap_or_else(|error| panic!("addr: {error}"))
        .port();
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&hits);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };
            counter.fetch_add(1, Ordering::SeqCst);
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                match stream.read(&mut byte) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => head.push(byte[0]),
                }
            }
            let line = String::from_utf8_lossy(&head).into_owned();
            let answer = if line.starts_with("GET /redirect") {
                format!(
                    "HTTP/1.1 302 Found\r\nlocation: http://127.0.0.1:{}/taken\r\ncontent-length: 0\r\n\r\n",
                    elsewhere.unwrap_or(0)
                )
            } else {
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 4\r\n\r\npong"
                    .to_owned()
            };
            let _ = stream.write_all(answer.as_bytes());
        }
    });
    Target { port, hits }
}

fn requests(records: &[LedgerRecord]) -> Vec<&LedgerEventKind> {
    records
        .iter()
        .map(|record| &record.kind)
        .filter(|kind| matches!(kind, LedgerEventKind::NetRequested { .. }))
        .collect()
}

/// The whole outbound matrix through the real daemon: the fixture's
/// activation ASSERTS each reading in-guest (a wrong answer faults the
/// entry, so a clean boot is the proof), and the kernel's own record
/// agrees — two calls made, the off-allowlist target never dialled.
#[tokio::test]
async fn the_outbound_matrix_holds_through_the_real_daemon() {
    let denied = target(None);
    let allowed = target(Some(denied.port));
    let home = home("matrix");
    let grants = serde_json::json!([
        "jinn:fs",
        { "contract": "jinn:net", "scope": { "outbound": [format!("127.0.0.1:{}", allowed.port)] } }
    ]);
    let daemon = booted(paths(
        &home,
        grants,
        &format!("net-out:{},{}", allowed.port, denied.port),
    ))
    .await;
    let records = events(&daemon).await;

    assert_eq!(
        denied.hits.load(Ordering::SeqCst),
        0,
        "the off-allowlist target was never dialled, redirect included"
    );
    assert_eq!(
        allowed.hits.load(Ordering::SeqCst),
        2,
        "two calls, two dials"
    );
    let rows = requests(&records);
    assert_eq!(rows.len(), 2, "one record per AUTHORIZED call: {rows:?}");
    let LedgerEventKind::NetRequested {
        method,
        host,
        path,
        status,
        response_bytes,
        ..
    } = rows[0]
    else {
        panic!("not a request row")
    };
    assert_eq!(
        (method.as_str(), path.as_str(), *status),
        ("GET", "/probe", 200)
    );
    assert_eq!(host, &format!("127.0.0.1:{}", allowed.port));
    assert_eq!(*response_bytes, 4);
    // The refusals are on the record too, and they are not the same class.
    let refusals: Vec<String> = records
        .iter()
        .filter_map(|record| match &record.kind {
            LedgerEventKind::GrantRefused {
                contract, detail, ..
            } if contract == "jinn:net" => detail.clone(),
            _ => None,
        })
        .collect();
    assert_eq!(
        refusals.len(),
        2,
        "the off-allowlist call and the redirect follow-up: {refusals:?}"
    );
    assert!(
        refusals.iter().all(|detail| detail.contains("allowlist")),
        "each names the allowlist: {refusals:?}"
    );

    // Law 2 vs 02 §Redaction: the credential reached the target (the guest
    // asserted the 200) and reaches NO byte of the durable record.
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
    let ledger = std::fs::read(home.0.join("ledger.sqlite"))
        .unwrap_or_else(|error| panic!("ledger file: {error}"));
    assert!(
        !ledger
            .windows(SECRET.len())
            .any(|window| window == SECRET.as_bytes()),
        "the ledger file carries no byte of the credential"
    );
}

/// Law 3 through the real daemon: a revert unit that contains the
/// irreversible call is REJECTED WHOLE — typed, naming what it could not
/// revert — and NOTHING in the unit is applied. The revertible member
/// alone still reverts, so the rejection is the unit's, not a broken
/// revert lane.
#[tokio::test]
async fn a_revert_unit_containing_a_request_is_rejected_whole() {
    let denied = target(None);
    let allowed = target(Some(denied.port));
    let home = home("revert");
    let grants = serde_json::json!([
        "jinn:fs",
        { "contract": "jinn:net", "scope": { "outbound": [format!("127.0.0.1:{}", allowed.port)] } }
    ]);
    let daemon = booted(paths(
        &home,
        grants,
        &format!("net-out:{},{}", allowed.port, denied.port),
    ))
    .await;

    let written = home.0.join("data/kept");
    assert!(written.exists(), "the fs effect landed");
    let (fs_effect, _) = daemon
        .fs_effects()
        .into_iter()
        .next()
        .unwrap_or_else(|| panic!("one revertible fs effect"));
    let calls = daemon.net_effects();
    assert_eq!(calls.len(), 2, "two irreversible calls: {calls:?}");
    let (net_effect, label) = calls[0].clone();

    // The unit: a revertible write and the call that cannot be un-sent.
    let refused = daemon
        .revert_unit(
            &[UnitMember::Fs(fs_effect), UnitMember::Net(net_effect)],
            "unit-1",
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("a unit containing an irreversible effect is rejected"));
    assert_eq!(refused.code, ErrorCode::Irreversible, "typed, not prose");
    assert!(
        refused.message.contains(&label) && refused.message.contains("irreversible"),
        "the refusal names WHAT and WHY: {}",
        refused.message
    );
    assert!(
        written.exists(),
        "nothing in the rejected unit was applied — the write still stands"
    );

    // Not a broken lane: the revertible member alone still reverts, and
    // the rejection survives the member order (the scan is over the WHOLE
    // unit, never just its head).
    let reversed = daemon
        .revert_unit(
            &[UnitMember::Net(net_effect), UnitMember::Fs(fs_effect)],
            "unit-2",
        )
        .await
        .err()
        .unwrap_or_else(|| panic!("member order does not matter"));
    assert_eq!(reversed.code, ErrorCode::Irreversible);
    let resolved = daemon
        .revert_unit(&[UnitMember::Fs(fs_effect)], "unit-3")
        .await
        .unwrap_or_else(|error| panic!("the revertible member alone: {error:?}"));
    assert_eq!(resolved, vec![jinnd_api::RevertResolution::Reverted]);
    assert!(!written.exists(), "and it really did revert");
}

/// The widening demonstration, stated: an entry cannot widen its OWN
/// outbound allowlist — `jinn:net` has no operation that writes a grant,
/// and `jinn:profile` refuses an entry that patches itself, even holding
/// the `*` scope. The fixture asserts both in-guest; a clean boot with the
/// target never dialled is the proof.
#[tokio::test]
async fn an_entry_cannot_widen_its_own_outbound_allowlist() {
    let server = target(None);
    let home = home("widen");
    let grants = serde_json::json!([
        { "contract": "jinn:net", "scope": { "outbound": ["127.0.0.1:1"] } },
        { "contract": "jinn:profile", "scope": ["*"] }
    ]);
    let daemon = booted(paths(
        &home,
        grants,
        &format!("net-widen:worker@{}", server.port),
    ))
    .await;
    let records = events(&daemon).await;
    assert_eq!(server.hits.load(Ordering::SeqCst), 0, "never reached");
    assert!(
        records.iter().any(|record| matches!(&record.kind,
            LedgerEventKind::AmendmentRefused { detail } if detail.contains("itself"))),
        "the refused self-patch is on the record"
    );
    assert!(
        !records
            .iter()
            .any(|record| matches!(&record.kind, LedgerEventKind::ProfilePatched { .. })),
        "and no patch was ever committed"
    );
}
