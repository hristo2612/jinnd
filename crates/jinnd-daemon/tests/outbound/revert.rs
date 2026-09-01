//! Law 3 through the real daemon (M2-K14): a revert unit containing the
//! irreversible outbound call is REJECTED WHOLE, typed, naming what it
//! could not revert — and the guarantee is DURABLE, not merely live. A
//! guarantee that expires when the process does is not a guarantee: it is
//! the vacuity class this packet exists to prevent, so the reopen proof
//! sits beside the in-process one.

use jinnd_api::{EffectId, ErrorCode};
use jinnd_daemon::{Daemon, UnitMember};

use super::{booted, home, paths, target};

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
    let calls = daemon
        .net_effects()
        .await
        .unwrap_or_else(|error| panic!("net effects: {error:?}"));
    assert_eq!(calls.len(), 3, "three irreversible calls: {calls:?}");
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

/// R5 / Law 3, THE ROUND-2 PIN: a sent request is still irreversible after
/// the daemon is closed and reopened over the same ledger.
///
/// Round 1 held outbound effects in a live in-memory map, so a reopened
/// daemon answered `NotFound` for a call it had really made — the Law-3
/// guarantee silently expired with the process. The record is now the
/// register (R5: one mutation primitive), so this test asserts three
/// things a live map cannot give: the OLD id still refuses and still names
/// its own call; the reopened daemon's NEW ids never collide with it; and
/// an id no record carries is still the distinct third answer.
#[tokio::test]
async fn a_sent_request_is_still_irreversible_after_a_reopen() {
    let denied = target(None);
    let allowed = target(Some(denied.port));
    let home = home("reopen");
    let grants = serde_json::json!([
        "jinn:fs",
        { "contract": "jinn:net", "scope": { "outbound": [format!("127.0.0.1:{}", allowed.port)] } }
    ]);
    let paths = paths(
        &home,
        grants,
        &format!("net-out:{},{}", allowed.port, denied.port),
    );

    let first = booted(paths.clone()).await;
    let before: Vec<(EffectId, String)> = first
        .net_effects()
        .await
        .unwrap_or_else(|error| panic!("net effects: {error:?}"));
    assert_eq!(before.len(), 3, "the first run really called: {before:?}");
    first
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
    drop(first);

    // A DIFFERENT process-lifetime over the same durable record.
    let second = Daemon::open(paths).unwrap_or_else(|error| panic!("reopen: {error:?}"));
    let report = second
        .boot()
        .await
        .unwrap_or_else(|error| panic!("reboot: {error:?}"));
    assert!(
        report.errors.is_empty(),
        "clean reboot: {:?}",
        report.errors
    );

    // BOTH DOORS, after the reopen (COO round-2 steer). The fixture's
    // calls [0] and [1] entered through `send-request`; call [2] entered
    // through the 0.1.0 `request` declaration. A legacy door whose calls
    // were merely SENT — not recorded as irreversible — would leave a way
    // to make an untakeable-back call the kernel later fails to name.
    for (which, (effect, label)) in [(0, before[0].clone()), (2, before[2].clone())] {
        let refused = second
            .revert_unit(&[UnitMember::Net(effect)], &format!("after-reopen-{which}"))
            .await
            .err()
            .unwrap_or_else(|| panic!("call {which} stays irreversible across a reopen"));
        assert_eq!(
            refused.code,
            ErrorCode::Irreversible,
            "call {which}: not NotFound, not a generic failure: {}",
            refused.message
        );
        assert!(
            refused.message.contains(&label),
            "call {which} still names what it could not take back: {}",
            refused.message
        );
    }

    // The reopened run minted its own ids ABOVE the durable high-water
    // mark: an irreversible id names ONE call forever, never two.
    let after = second
        .net_effects()
        .await
        .unwrap_or_else(|error| panic!("net effects: {error:?}"));
    assert_eq!(after.len(), 6, "three more calls: {after:?}");
    let fresh: Vec<u64> = after[3..].iter().map(|(id, _)| id.0).collect();
    let stale: Vec<u64> = before.iter().map(|(id, _)| id.0).collect();
    assert!(
        fresh.iter().all(|id| !stale.contains(id)),
        "no reopened id reuses a spent one: {fresh:?} against {stale:?}"
    );

    // And the third answer stays distinct: an id no record carries is
    // NotFound, never a refusal dressed up as one.
    let unknown = second
        .revert_unit(&[UnitMember::Net(EffectId(9_999))], "never-sent")
        .await
        .err()
        .unwrap_or_else(|| panic!("an unknown effect is refused"));
    assert_eq!(unknown.code, ErrorCode::NotFound);
}
