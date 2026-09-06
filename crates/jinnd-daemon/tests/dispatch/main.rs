//! M2-K9 acceptance (harness FINDINGS #31), through the real daemon: the
//! two-hop shape K8 left open. A settings provider patches its CONSUMER —
//! answered `accepted(seq)` the instant the document commits and the
//! restart is SCHEDULED — and then, inside that window, dispatches its
//! `changed` notice SERIALLY to the very entry it just patched. The
//! consumer still holds a live, routed seat and a listener, so before this
//! packet the notice was delivered into an incarnation the loader was
//! already replacing, whose handler waited on a peer that could not answer
//! (the provider, inside the very call that emitted) until the guest
//! deadline killed them both.
//!
//! The contract now: the walk is refused WHOLE, before any listener runs,
//! with a typed refusal whose CASE is the caller's next move and whose
//! record names the target, the incarnation being replaced, and the topic; the refusal is a ledger row of its own kind
//! (a reader tells it from a scope refusal without parsing prose); and the
//! pending restart is ASKABLE through `jinn:introspect` rather than
//! discoverable only by stalling.

mod harness;
mod observation;

use std::time::{Duration, Instant};

use jinnd_api::{DispatchMode, EntryId, FiberState, LedgerEventKind, Owed};

use harness::{booted, entry, events, home, json, paths, wait_for};

/// The packet's acceptance case: patch an entry, then serially dispatch to
/// it before the swap commits. The dispatch is refused, typed and
/// ledgered, within the guest deadline; the consumer's handler never runs;
/// the ledger row is its own kind (never a scope refusal); no
/// `DispatchTrace` is recorded for a walk that never dispatched; and
/// `jinn:introspect` reports the pending restart, asked from inside the
/// very window. Afterwards the restart lands and the consumer is Active on
/// its patched config — the refusal never cost the restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_serial_dispatch_to_a_restarting_fiber_refuses_typed_and_ledgered() {
    let home = home("restarting");
    let paths = paths(
        &home,
        vec![
            entry(
                "provider",
                serde_json::json!([
                    "jinn:test/settings",
                    // The provider's walk is covered by the topic's grant
                    // (M2-K26 (e)).
                    "jinn:test/settings-changed",
                    "jinn:introspect",
                    "jinn:fs",
                    "jinn:clock",
                    { "contract": "jinn:profile", "scope": ["consumer"] }
                ]),
                "notify-provider-observed",
            ),
            entry(
                "consumer",
                serde_json::json!([
                    "jinn:test/settings",
                    "jinn:test/settings-changed",
                    "jinn:fs",
                    "jinn:clock"
                ]),
                "notify-consumer",
            ),
            entry(
                "trigger",
                serde_json::json!(["jinn:test/settings", "jinn:fs", "jinn:clock"]),
                "notify-trigger:consumer",
            ),
        ],
    );
    let daemon = booted(paths.clone()).await;
    let consumer = daemon
        .entry_fiber("consumer")
        .unwrap_or_else(|| panic!("consumer live"));

    // The kernel's answer to the emitting guest: the TYPED refusal, naming
    // the target — never a stall, never an empty successful walk.
    let outcome = wait_for(&paths.data.join("notify.out"), |bytes| !bytes.is_empty()).await;
    let body = String::from_utf8_lossy(&outcome[1.min(outcome.len())..]).into_owned();
    assert_eq!(
        outcome.first(),
        Some(&1),
        "the typed `restarting` refusal (tag 1), got {:?}: {body}",
        outcome.first()
    );
    // The guest read IDENTITY off the record — it parsed no sentence.
    let refusal = json(body.as_bytes());
    assert_eq!(refusal["case"], serde_json::json!("restarting"));
    assert_eq!(
        refusal["entry"],
        serde_json::json!("consumer"),
        "the record names the target: {refusal}"
    );
    assert_eq!(
        refusal["topic"],
        serde_json::json!("jinn:test/settings-changed"),
        "and the refused topic: {refusal}"
    );
    assert!(
        refusal["incarnation"].as_u64().is_some_and(|born| born > 0),
        "and the incarnation being replaced: {refusal}"
    );

    // The pending restart was ASKABLE, from inside the window itself.
    let composition = json(
        &std::fs::read(paths.data.join("notify-introspect.json"))
            .unwrap_or_else(|error| panic!("the window's introspect snapshot: {error}")),
    );
    let seen = composition
        .as_array()
        .and_then(|entries| entries.iter().find(|entry| entry["id"] == "consumer"))
        .unwrap_or_else(|| panic!("the consumer is in the composition: {composition}"));
    assert_eq!(
        seen["unserved"],
        serde_json::json!("restarting"),
        "introspect names the pending transition in the refusal's own \
         vocabulary — a replacement IS scheduled here: {seen}"
    );
    // During Loading, introspect may have no installed incarnation yet;
    // the selected tombstone's identity is in the typed refusal and row.
    assert_eq!(seen["fiber"].as_u64(), Some(consumer.0));

    let records = events(&daemon).await;
    let attempt = observation::attempt(&paths.data, "dispatch-refused");
    let walk = observation::walk(&records, &attempt);
    let refusals: Vec<_> = walk
        .iter()
        .filter(|record| matches!(record.kind, LedgerEventKind::DispatchRefused { .. }))
        .collect();
    assert_eq!(refusals.len(), 1, "one refusal for {attempt}: {walk:?}");
    let refused = refusals[0];
    match &refused.kind {
        LedgerEventKind::DispatchRefused {
            topic,
            mode,
            target,
            incarnation,
            owed,
        } => {
            assert_eq!(topic, "jinn:test/settings-changed");
            assert_eq!(*mode, DispatchMode::Serial);
            assert_eq!(target.0, "consumer", "the row names the target entry");
            assert_eq!(Some(*incarnation), refusal["incarnation"].as_u64());
            assert_eq!(
                *owed,
                Owed::Reload,
                "and WHY, so a ledger reader tells this from a refusal by a \
                 fiber that is never coming back"
            );
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        refused.entry,
        Some(EntryId("provider".to_owned())),
        "attributed to the emitter, like a dispatch trace"
    );
    // Law 2, told apart from a scope refusal by KIND, not by prose.
    assert!(
        !walk.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::GrantRefused { contract, .. }
                if contract == "jinn:test/settings-changed"
        )),
        "a restart refusal is not a grant refusal: {records:?}"
    );
    assert_eq!(
        observation::no_delivery(&records, &paths.data, &attempt),
        Ok(())
    );
    assert_eq!(observation::no_trace(&walk), Ok(()));

    // Negative controls use an ACTUAL earlier delivery, not fabricated log
    // text. Each no-delivery check must reject it independently. The same
    // observer accepts the later refusal without assuming different incarnations.
    let control = observation::attempt(&paths.data, "dispatch-control");
    assert_ne!(control, attempt);
    let control_walk = observation::walk(&records, &control);
    assert!(control_walk.last().unwrap().sequence < walk.first().unwrap().sequence);
    assert_eq!(
        observation::no_delivery(&records, &paths.data, &control),
        Err("request reached a listener")
    );
    assert_eq!(
        observation::no_trace(&control_walk),
        Err("request dispatched a traced walk")
    );
    assert!(control_walk.iter().any(|record| matches!(&record.kind,
        LedgerEventKind::DispatchTrace { topic, listeners, .. }
            if topic == "jinn:test/settings-changed" && *listeners > 0
    )));
    // R11: refusing cost nobody their fiber — the emitter was never held
    // to its deadline, and the target restarted cleanly.
    assert!(
        !records.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::FiberTransition(transition) if transition.to == FiberState::Failed
        )),
        "nothing failed: {records:?}"
    );

    // The restart the refusal pointed at lands: the consumer comes back on
    // its patched config, and its second activation is on the record.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        daemon.sync_transitions();
        let log = std::fs::read(paths.data.join("consumer.log")).unwrap_or_default();
        if daemon.fiber_state(consumer) == Some(FiberState::Active)
            && String::from_utf8_lossy(&log)
                .lines()
                .filter(|line| *line == "act")
                .count()
                >= 2
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the scheduled restart lands: state={:?} log={:?}",
            daemon.fiber_state(consumer),
            String::from_utf8_lossy(&log)
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let document = json(&std::fs::read(&paths.profile).unwrap_or_else(|error| panic!("{error}")));
    let patched = document["entries"]
        .as_array()
        .and_then(|entries| entries.iter().find(|entry| entry["id"] == "consumer"))
        .map(|entry| entry["config"]["data"].clone());
    assert_eq!(patched, Some(serde_json::json!("notify-consumer:v2")));
    daemon
        .shutdown()
        .await
        .unwrap_or_else(|error| panic!("shutdown: {error:?}"));
    let final_records = events(&daemon).await;
    assert_eq!(
        observation::no_delivery(&final_records, &paths.data, &attempt),
        Ok(())
    );
    assert_eq!(
        observation::no_trace(&observation::walk(&final_records, &attempt)),
        Ok(())
    );
    assert!(
        !final_records.iter().any(|record| matches!(&record.kind,
            LedgerEventKind::FiberTransition(transition) if transition.to == FiberState::Failed
        )),
        "nothing failed through replacement and shutdown: {final_records:?}"
    );
    println!(
        "request={attempt}, incarnation={}, control={control}: refusal identity, no delivery/trace, both real-delivery negative controls, replacement and liveness passed",
        refusal["incarnation"]
    );
}
