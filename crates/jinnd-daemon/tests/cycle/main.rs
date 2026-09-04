//! M2-K10 acceptance (harness FINDINGS #32), through the real daemon: the
//! window K9 did not close, and the one the harness finally caught with a
//! transcript. Two HONEST plugins under ordinary composition — a provider
//! that answers calls while also notifying its listeners, an owner that
//! calls back on the notice — park on each other. Each is inside the very
//! crossing the other is waiting for, a Tier A instance serves one guest
//! entry at a time, and before this packet both simply ran to the guest
//! deadline and died.
//!
//! No restart appears anywhere in this shape: it is not K9's window
//! narrowed, it is a different defect. The contract now: the crossing that
//! would CLOSE the cycle is refused immediately and whole, with a typed
//! refusal naming both ends and the wait between them; the refusal is a
//! ledger row of its own kind, told from a pending-transition refusal and
//! from a scope refusal without parsing prose; and the live wait is
//! ASKABLE through `jinn:introspect.waits` from inside the window itself.
//!
//! Both orderings are driven, because either edge can be the one that
//! arrives second: the provider's dispatch first (the call back closes),
//! and the owner's call first (the dispatch closes).

mod harness;

use jinnd_api::{EntryId, FiberState, LedgerEventKind};

use harness::{booted, cycle, entry, events, home, json, paths, wait_for};

/// The ordering the harness transcribed: the provider is mid-dispatch into
/// the owner, and the owner's call back into the provider is the edge that
/// would close. That CALL is refused — typed, ledgered, inside the guest
/// deadline — the notice handler returns normally, and the provider's walk
/// settles instead of dying.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_that_would_close_a_cycle_refuses_typed_and_ledgered() {
    let home = home("call");
    let paths = paths(
        &home,
        vec![
            entry(
                "provider",
                // Its notify walk is covered by the topic's grant (M2-K26 (e)).
                serde_json::json!([
                    "jinn:test/settings",
                    "jinn:test/cycle-notice",
                    "jinn:fs",
                    "jinn:clock"
                ]),
                "cycle-provider",
            ),
            entry(
                "owner",
                serde_json::json!([
                    "jinn:test/settings",
                    "jinn:test/cycle-notice",
                    "jinn:introspect",
                    "jinn:fs",
                    "jinn:clock"
                ]),
                "cycle-owner",
            ),
            entry(
                "trigger",
                serde_json::json!(["jinn:test/settings", "jinn:fs", "jinn:clock"]),
                "cycle-trigger",
            ),
        ],
    );
    let daemon = booted(paths.clone()).await;

    // What the kernel answered the OWNER: the typed cycle, not a stall.
    let refusal = cycle(&wait_for(&paths.data.join("owner.out")).await);
    assert_eq!(
        refusal["on"],
        serde_json::json!("jinn:test/settings.get"),
        "the record names the crossing refused: {refusal}"
    );
    assert_eq!(
        refusal["waiter"],
        serde_json::json!("owner"),
        "and the end that would have parked: {refusal}"
    );
    assert_eq!(
        refusal["target"],
        serde_json::json!("provider"),
        "and the end already awaiting it: {refusal}"
    );
    assert_eq!(
        refusal["through"],
        serde_json::json!(["jinn:test/cycle-notice"]),
        "and the wait that makes it a cycle — evidence, not prose: {refusal}"
    );

    // The provider's own walk SETTLED: it was delivered and answered. The
    // refusal cost the composition one call, not the dispatch.
    let delivered = wait_for(&paths.data.join("cycle.out")).await;
    assert_eq!(
        delivered.first(),
        Some(&0),
        "the notice itself went through: {:?}",
        String::from_utf8_lossy(&delivered)
    );

    // The wait was ASKABLE from inside the window, in the refusal's own
    // vocabulary — the ask that replaces discovering it by stalling.
    let live = json(&wait_for(&paths.data.join("cycle-waits.json")).await);
    let seen = live
        .as_array()
        .and_then(|waits| {
            waits
                .iter()
                .find(|wait| wait["waiter-entry"] == "provider" && wait["target-entry"] == "owner")
        })
        .unwrap_or_else(|| panic!("introspect shows the waiting edge: {live}"));
    assert_eq!(
        seen["on"],
        serde_json::json!("jinn:test/cycle-notice"),
        "naming what that end is parked on: {seen}"
    );

    let records = events(&daemon).await;
    let refused = records
        .iter()
        .find(|record| matches!(record.kind, LedgerEventKind::CycleRefused { .. }))
        .unwrap_or_else(|| panic!("the refusal is a ledger row: {records:?}"));
    match &refused.kind {
        LedgerEventKind::CycleRefused {
            on,
            target_entry,
            through,
            ..
        } => {
            assert_eq!(on, "jinn:test/settings.get");
            assert_eq!(
                target_entry.as_ref(),
                Some(&EntryId("provider".to_owned())),
                "the row names the end already awaiting the caller"
            );
            assert_eq!(through.len(), 1, "and the one hop back: {through:?}");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        refused.entry,
        Some(EntryId("owner".to_owned())),
        "attributed to the refused caller"
    );
    // Told apart by KIND, never by prose: this is not a grant refusal and
    // not a pending-transition refusal (Law 2, R3).
    assert!(
        !records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::GrantRefused { contract, .. } if contract == "jinn:test/settings"
        )),
        "a wait cycle is not a grant refusal: {records:?}"
    );
    assert!(
        !records
            .iter()
            .any(|record| matches!(record.kind, LedgerEventKind::DispatchRefused { .. })),
        "and not a pending-transition refusal — nothing here is restarting: {records:?}"
    );
    // The refused call never crossed: no `ContractCall` for it.
    assert!(
        !records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::ContractCall { contract, operation }
                if contract == "jinn:test/settings" && operation == "get"
        )),
        "a refused crossing does not cross: {records:?}"
    );
    // R11, and the whole point: refusing cost nobody their fiber. Before
    // this packet both ends died on the guest deadline.
    assert!(
        !records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::FiberTransition(transition) if transition.to == FiberState::Failed
        )),
        "nothing failed: {records:?}"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}

/// The other ordering, where the second edge arrives first: the owner is
/// already parked on the provider, so the provider's DISPATCH is the edge
/// that would close. The walk is refused whole — before any listener runs
/// — and traces nothing, because it dispatched nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dispatch_that_would_close_a_cycle_refuses_whole() {
    let home = home("dispatch");
    let paths = paths(
        &home,
        vec![
            entry(
                "provider",
                // Its notify walk is covered by the topic's grant (M2-K26 (e)).
                serde_json::json!([
                    "jinn:test/settings",
                    "jinn:test/cycle-notice",
                    "jinn:fs",
                    "jinn:clock"
                ]),
                "cycle-provider",
            ),
            entry(
                "caller",
                serde_json::json!([
                    "jinn:test/settings",
                    "jinn:test/cycle-notice",
                    "jinn:introspect",
                    "jinn:fs",
                    "jinn:clock"
                ]),
                "cycle-caller",
            ),
        ],
    );
    let daemon = booted(paths.clone()).await;

    // The provider's verdict, travelling back as its call's answer: the
    // typed cycle, with the ends the other way round.
    let refusal = cycle(&wait_for(&paths.data.join("caller.out")).await);
    assert_eq!(
        refusal["on"],
        serde_json::json!("jinn:test/cycle-notice"),
        "the dispatch is what refused this time: {refusal}"
    );
    assert_eq!(refusal["waiter"], serde_json::json!("provider"));
    assert_eq!(refusal["target"], serde_json::json!("caller"));
    assert_eq!(
        refusal["through"],
        serde_json::json!(["jinn:test/settings.cycle-notify"]),
        "the hop back is the caller's own in-flight call: {refusal}"
    );

    // Nothing landed: the walk was refused before any listener ran, so the
    // notice handler never wrote its outcome.
    assert!(
        !paths.data.join("owner.out").exists(),
        "the notice was never delivered"
    );

    let records = events(&daemon).await;
    let refused = records
        .iter()
        .find(|record| matches!(record.kind, LedgerEventKind::CycleRefused { .. }))
        .unwrap_or_else(|| panic!("the refusal is a ledger row: {records:?}"));
    match &refused.kind {
        LedgerEventKind::CycleRefused {
            on, target_entry, ..
        } => {
            assert_eq!(on, "jinn:test/cycle-notice");
            assert_eq!(target_entry.as_ref(), Some(&EntryId("caller".to_owned())));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        refused.entry,
        Some(EntryId("provider".to_owned())),
        "attributed to the emitter, like a dispatch trace"
    );
    // A refused walk lands its refusal INSTEAD of a trace: it dispatched
    // nothing, so there is nothing to trace.
    assert!(
        !records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::DispatchTrace { topic, .. } if topic == "jinn:test/cycle-notice"
        )),
        "a refused walk traces nothing: {records:?}"
    );
    assert!(
        !records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::FiberTransition(transition) if transition.to == FiberState::Failed
        )),
        "nothing failed: {records:?}"
    );
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
}
