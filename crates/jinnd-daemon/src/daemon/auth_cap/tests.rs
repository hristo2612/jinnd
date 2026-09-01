//! M2-K21 acceptance at the provider, through the broker's single dispatch
//! point as a native peer (transport-agnostic by ruling: a native peer, a
//! wasm instance, and a future sandboxed process all cross the same
//! choke point). Every refusal test first proves the RIGHT credential
//! grants in the same daemon — a refusal that would pass with no
//! credential configured proves nothing (the M2-K14 precedent). The
//! guest-side proof, through a real component, is `tests/auth.rs`.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use jinnd_api::{ErrorCode, LedgerEventKind, LedgerRecord};
use jinnd_wasm::{AUTH_CONTRACT, MasterKeySource, PeerId, hex_digest};

use super::{MAX_FILE, MIN_LEN, TAG_GRANTED, TAG_UNAUTHENTICATED};
use crate::{Daemon, DaemonPaths};

const TOKEN: &str = "operator-token-0xFEEDFACE-not-in-any-row";
const ROTATED: &str = "operator-token-0xCAFEBABE-the-second-one";

struct Home(PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn home(name: &str) -> Home {
    let root = std::env::temp_dir().join(format!("jinnd-auth-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("artifacts")).unwrap_or_else(|error| panic!("{error}"));
    std::fs::write(root.join("profile.json"), r#"{"entries":[]}"#)
        .unwrap_or_else(|error| panic!("{error}"));
    Home(root)
}

fn paths(home: &Home) -> DaemonPaths {
    DaemonPaths {
        profile: home.0.join("profile.json"),
        ledger: home.0.join("ledger.sqlite"),
        artifacts: home.0.join("artifacts"),
        data: home.0.join("data"),
    }
}

/// Writes the credential the way a launcher does: the bytes, mode 0600.
fn write_credential(path: &Path, bytes: &[u8], mode: u32) {
    std::fs::write(path, bytes).unwrap_or_else(|error| panic!("{error}"));
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .unwrap_or_else(|error| panic!("{error}"));
}

/// A booted daemon over an empty profile, plus a granted native peer.
async fn rig(name: &str) -> (Home, Daemon, PeerId) {
    let home = home(name);
    let daemon = Daemon::open_with(paths(&home), MasterKeySource::Absent)
        .unwrap_or_else(|error| panic!("open: {error:?}"));
    daemon
        .boot()
        .await
        .unwrap_or_else(|error| panic!("boot: {error:?}"));
    let peer = daemon.lane.broker.register_peer(None);
    daemon.lane.broker.grant(peer, AUTH_CONTRACT);
    (home, daemon, peer)
}

async fn verify(daemon: &Daemon, peer: PeerId, presented: &str) -> Vec<u8> {
    daemon
        .lane
        .broker
        .dispatch(peer, AUTH_CONTRACT, "verify", presented.as_bytes().to_vec())
        .await
        .unwrap_or_else(|error| panic!("verify crosses the broker: {error:?}"))
}

fn granted_as(answer: &[u8]) -> Option<&str> {
    (answer.first() == Some(&TAG_GRANTED))
        .then(|| std::str::from_utf8(&answer[1..]).unwrap_or_else(|error| panic!("{error}")))
}

fn refused_for(answer: &[u8]) -> Option<&str> {
    (answer.first() == Some(&TAG_UNAUTHENTICATED))
        .then(|| std::str::from_utf8(&answer[1..]).unwrap_or_else(|error| panic!("{error}")))
}

async fn records(daemon: &Daemon) -> Vec<LedgerRecord> {
    daemon
        .ledger_events()
        .await
        .unwrap_or_else(|error| panic!("ledger read: {error:?}"))
}

fn decisions(records: &[LedgerRecord]) -> Vec<(Option<String>, String, bool)> {
    records
        .iter()
        .filter_map(|record| match &record.kind {
            LedgerEventKind::AuthDecided {
                name,
                presented,
                granted,
            } => Some((name.clone(), presented.clone(), *granted)),
            _ => None,
        })
        .collect()
}

/// Deny by default and the record: with NO credential file nothing grants;
/// once the launcher writes one, the right value grants as `operator`,
/// the row carries the name and the presented DIGEST, and the credential
/// bytes appear in no row of the whole ledger.
#[tokio::test]
async fn the_right_credential_grants_and_the_row_carries_name_and_digest_only() {
    let (home, daemon, peer) = rig("grant").await;
    let credential = paths(&home).credential();
    assert!(
        refused_for(&verify(&daemon, peer, TOKEN).await).is_some(),
        "no credential file: deny by default"
    );
    write_credential(&credential, TOKEN.as_bytes(), 0o600);
    assert_eq!(
        granted_as(&verify(&daemon, peer, TOKEN).await),
        Some("operator")
    );
    let records = records(&daemon).await;
    assert_eq!(
        decisions(&records),
        vec![
            (None, hex_digest(TOKEN.as_bytes()), false),
            (
                Some("operator".to_owned()),
                hex_digest(TOKEN.as_bytes()),
                true
            ),
        ],
        "one row per decision, in order: {records:?}"
    );
    let rendered = serde_json::to_string(&records).unwrap_or_else(|error| panic!("{error}"));
    assert!(
        !rendered.contains(TOKEN),
        "the credential's bytes are in no ledger row"
    );
}

/// The refusal is its own typed class and asserts its own precondition:
/// the right value grants, then a wrong one is `unauthenticated` (tag 1),
/// recorded with `name: None` and the digest of what was PRESENTED.
#[tokio::test]
async fn a_wrong_credential_is_unauthenticated_after_the_right_one_granted() {
    let (home, daemon, peer) = rig("wrong").await;
    write_credential(&paths(&home).credential(), TOKEN.as_bytes(), 0o600);
    assert_eq!(
        granted_as(&verify(&daemon, peer, TOKEN).await),
        Some("operator"),
        "precondition: the credential is live"
    );
    let answer = verify(&daemon, peer, "operator-token-0xBADC0FFEE-wrong-one").await;
    let reason = refused_for(&answer).unwrap_or_else(|| panic!("tag 1: {answer:?}"));
    assert!(reason.contains("match"), "names the precondition: {reason}");
    assert!(!reason.contains(TOKEN), "the reason carries no credential");
    let last = decisions(&records(&daemon).await)
        .pop()
        .unwrap_or_else(|| panic!("the refusal is on the record"));
    assert_eq!(
        last,
        (
            None,
            hex_digest(b"operator-token-0xBADC0FFEE-wrong-one"),
            false
        )
    );
}

/// Rotation and revocation without a restart, on ONE daemon: overwrite
/// the file and the new value grants while the old refuses from the very
/// next call; delete it and everything refuses.
#[tokio::test]
async fn rotation_and_revocation_take_effect_on_the_next_call_without_a_restart() {
    let (home, daemon, peer) = rig("rotate").await;
    let credential = paths(&home).credential();
    write_credential(&credential, TOKEN.as_bytes(), 0o600);
    assert!(granted_as(&verify(&daemon, peer, TOKEN).await).is_some());
    write_credential(&credential, ROTATED.as_bytes(), 0o600);
    assert!(
        refused_for(&verify(&daemon, peer, TOKEN).await).is_some(),
        "the rotated-out value refuses at once"
    );
    assert_eq!(
        granted_as(&verify(&daemon, peer, ROTATED).await),
        Some("operator"),
        "the new value grants at once"
    );
    std::fs::remove_file(&credential).unwrap_or_else(|error| panic!("{error}"));
    let answer = verify(&daemon, peer, ROTATED).await;
    assert!(
        refused_for(&answer).is_some_and(|reason| reason.contains("absent")),
        "revoked: {answer:?}"
    );
    assert_eq!(
        decisions(&records(&daemon).await)
            .iter()
            .map(|(_, _, granted)| *granted)
            .collect::<Vec<_>>(),
        vec![true, false, true, false]
    );
}

/// A credential file another uid could read is NOT a credential: the
/// preconditions on the file itself each refuse, and each answer names
/// which one. Trailing whitespace is trimmed, as a launcher's `echo` leaves.
#[tokio::test]
async fn an_exposed_short_or_oversized_credential_file_is_not_a_credential() {
    let (home, daemon, peer) = rig("file").await;
    let credential = paths(&home).credential();
    write_credential(&credential, format!("{TOKEN}\n").as_bytes(), 0o600);
    assert!(
        granted_as(&verify(&daemon, peer, TOKEN).await).is_some(),
        "precondition: trimmed, mode 0600, grants"
    );
    for (mode, bytes, names) in [
        (0o640, TOKEN.as_bytes().to_vec(), "accessible"),
        (0o604, TOKEN.as_bytes().to_vec(), "accessible"),
        (0o600, TOKEN.as_bytes()[..MIN_LEN - 1].to_vec(), "short"),
        (
            0o600,
            vec![b'x'; usize::try_from(MAX_FILE).unwrap_or(usize::MAX) + 1],
            "bound",
        ),
    ] {
        write_credential(&credential, &bytes, mode);
        let presented = String::from_utf8_lossy(&bytes).into_owned();
        let answer = verify(&daemon, peer, &presented).await;
        let reason = refused_for(&answer).unwrap_or_else(|| panic!("mode {mode:o}: {answer:?}"));
        assert!(reason.contains(names), "mode {mode:o}: {reason}");
    }
    write_credential(&credential, TOKEN.as_bytes(), 0o600);
    assert!(
        granted_as(&verify(&daemon, peer, TOKEN).await).is_some(),
        "restored to 0600, grants again"
    );
}

/// The DISTINCT CLASSES: an ungranted peer is refused by the BROKER as a
/// grant refusal (never reaching `verify`, no decision row), while a
/// granted peer with the wrong credential is `unauthenticated` on the
/// contract's own wire. Same daemon, two different answers.
#[tokio::test]
async fn a_grant_refusal_and_an_unauthenticated_answer_are_different_classes() {
    let (home, daemon, peer) = rig("classes").await;
    write_credential(&paths(&home).credential(), TOKEN.as_bytes(), 0o600);
    assert!(
        granted_as(&verify(&daemon, peer, TOKEN).await).is_some(),
        "precondition: the credential is live"
    );
    let stranger = daemon.lane.broker.register_peer(None);
    let refused = daemon
        .lane
        .broker
        .dispatch(stranger, AUTH_CONTRACT, "verify", TOKEN.as_bytes().to_vec())
        .await;
    let error = refused
        .err()
        .unwrap_or_else(|| panic!("no grant, no crossing"));
    assert_eq!(error.code, ErrorCode::EffectFailed);
    let records = records(&daemon).await;
    assert!(
        records.iter().any(|record| matches!(&record.kind, LedgerEventKind::GrantRefused { contract, .. } if contract == AUTH_CONTRACT)),
        "the broker's refusal is on the record: {records:?}"
    );
    assert!(
        decisions(&records).is_empty(),
        "a stranger's RIGHT credential never reaches the decision point"
    );
    assert!(refused_for(&verify(&daemon, peer, "nope-not-the-operator-token").await).is_some());
}

/// A refused call reaches NO effect: between the crossing and the answer
/// the ledger gains exactly the broker's line and the decision row —
/// nothing registered, nothing written back, nothing patched — and the
/// document of record is byte-identical.
#[tokio::test]
async fn a_refusal_lands_before_any_effect_and_touches_nothing() {
    let (home, daemon, peer) = rig("no-effect").await;
    write_credential(&paths(&home).credential(), TOKEN.as_bytes(), 0o600);
    let profile = paths(&home).profile;
    let before = std::fs::read(&profile).unwrap_or_else(|error| panic!("{error}"));
    let mark = records(&daemon).await.len();
    assert!(refused_for(&verify(&daemon, peer, "not-the-operator-token-at-all").await).is_some());
    let since: Vec<String> = records(&daemon)
        .await
        .into_iter()
        .skip(mark)
        .map(|record| match record.kind {
            LedgerEventKind::ContractCall {
                contract,
                operation,
            } => format!("call {contract}.{operation}"),
            LedgerEventKind::AuthDecided { granted, .. } => format!("decided {granted}"),
            other => format!("UNEXPECTED {other:?}"),
        })
        .collect();
    assert_eq!(since, vec!["call jinn:auth.verify", "decided false"]);
    assert_eq!(
        std::fs::read(&profile).unwrap_or_else(|error| panic!("{error}")),
        before
    );
}

/// An operation the bundle does not declare is a typed refusal, never a
/// hang and never a grant.
#[tokio::test]
async fn an_unknown_operation_is_typed() {
    let (home, daemon, peer) = rig("unknown").await;
    write_credential(&paths(&home).credential(), TOKEN.as_bytes(), 0o600);
    let refused = daemon
        .lane
        .broker
        .dispatch(peer, AUTH_CONTRACT, "grant", TOKEN.as_bytes().to_vec())
        .await;
    assert_eq!(
        refused.err().map(|error| error.code),
        Some(ErrorCode::PluginFailed)
    );
}

/// Every non-test source file of this provider, off disk — a scan that
/// must be extended by hand stops covering the module the day someone
/// forgets.
fn provider_sources() -> Vec<(String, String)> {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/daemon");
    let mut sources = vec![("auth_cap.rs".to_owned(), read(&dir.join("auth_cap.rs")))];
    for entry in std::fs::read_dir(dir.join("auth_cap")).unwrap_or_else(|error| panic!("{error}")) {
        let path = entry.unwrap_or_else(|error| panic!("{error}")).path();
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_owned();
        if name.ends_with("tests.rs") {
            continue;
        }
        sources.push((format!("auth_cap/{name}"), read(&path)));
    }
    sources
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

/// NO BYPASS, asserted over the provider's source rather than inspected:
/// no environment read, no build-flag or debug-only gate, no `cfg(not
/// (test))` twin of a test path, and every `cfg(test)` guards a test
/// MODULE and nothing else — so there is no seam a release build could
/// carry. The needles are assembled from fragments so this file never
/// spells one out, even in prose.
#[test]
fn the_provider_has_no_off_switch_in_its_source() {
    let forbidden = [
        concat!("env::", "var"),
        concat!("var", "_os("),
        concat!("option_", "env!"),
        concat!("env!", "("),
        concat!("cfg(", "feature"),
        concat!("cfg(", "debug_assertions"),
        concat!("cfg(", "not(test"),
        concat!("cfg_", "attr"),
    ];
    let sources = provider_sources();
    assert!(!sources.is_empty(), "the walk found the provider");
    for (path, source) in &sources {
        for needle in forbidden {
            assert!(
                !source.contains(needle),
                "{path} names {needle:?}: the check must have no off switch"
            );
        }
        let lines: Vec<&str> = source.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if line.trim() != concat!("#[cfg(", "test)]") {
                continue;
            }
            let guarded = lines
                .get(index + 1)
                .map(|next| next.trim())
                .unwrap_or_default();
            assert!(
                guarded.starts_with("mod ") && guarded.ends_with("_tests;")
                    || guarded == "mod tests;",
                "{path}:{}: a test guard on {guarded:?} — only test modules are test-only",
                index + 2
            );
        }
    }
}
