//! Tier A hosting behaviors, pinned end to end against the real fixture
//! component (M1-P8 acceptance): pin-by-hash admission, the one broker choke
//! point shared by native and guest callers, effect/undo replay, containment
//! of traps and spins, I1 disposal, and Mode-1 swap with state handoff.

#![cfg(not(feature = "loom"))]

mod support;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jinnd_api::{EntryId, ErrorCode, FiberId, KernelFuture, LedgerEventKind, SwapPhaseKind};
use jinnd_wasm::{
    Alarms, Broker, DEFAULT_MIN_PERIOD_MS, InstanceHandle, LedgerSink, LoadedComponent,
    LocalTopics, NoRealms, Peer, PeerId, Seat, SwapSlots, WasmHost, swap_batch,
};

struct Recording {
    events: Mutex<Vec<LedgerEventKind>>,
}

impl LedgerSink for Recording {
    fn append(&self, kind: LedgerEventKind, _: Option<FiberId>) {
        self.events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(kind);
    }
}

impl Recording {
    fn kinds(&self) -> Vec<LedgerEventKind> {
        self.events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }
}

struct Rig {
    host: WasmHost,
    broker: Arc<Broker>,
    topics: Arc<LocalTopics>,
    alarms: Arc<Alarms>,
    ledger: Arc<Recording>,
    component: LoadedComponent,
}

fn rig() -> Rig {
    let ledger = Arc::new(Recording {
        events: Mutex::new(Vec::new()),
    });
    let broker = Arc::new(Broker::new(ledger.clone()));
    let host = WasmHost::new().unwrap_or_else(|error| panic!("host: {error:?}"));
    let (bytes, hash) = support::pinned_fixture();
    let component = host
        .load(bytes, &hash, ledger.as_ref())
        .unwrap_or_else(|error| panic!("fixture refused: {error:?}"));
    let alarms = Arc::new(Alarms::new(ledger.clone() as Arc<dyn LedgerSink>));
    Rig {
        host,
        broker,
        topics: Arc::new(LocalTopics::default()),
        alarms,
        ledger,
        component,
    }
}

impl Rig {
    fn seat(&self, fiber: u64, deadline: Duration) -> (PeerId, Seat) {
        self.seat_with_floor(fiber, deadline, DEFAULT_MIN_PERIOD_MS)
    }

    fn seat_with_floor(&self, fiber: u64, deadline: Duration, floor_ms: u64) -> (PeerId, Seat) {
        let peer = self.broker.register_peer(Some(FiberId(fiber)));
        (
            peer,
            Seat {
                broker: Arc::clone(&self.broker),
                topics: Arc::clone(&self.topics),
                alarms: Arc::clone(&self.alarms),
                oracle: Arc::new(NoRealms),
                peer,
                fiber: Some(FiberId(fiber)),
                context: fiber,
                deadline,
                clock_floor_ms: floor_ms,
                slot: None,
                staging: false,
            },
        )
    }

    fn spawn(&self, fiber: u64) -> (PeerId, InstanceHandle) {
        let (peer, seat) = self.seat(fiber, Duration::from_secs(5));
        (peer, self.host.instantiate(&self.component, seat))
    }

    fn spawn_with_floor(&self, fiber: u64, floor_ms: u64) -> (PeerId, InstanceHandle) {
        let (peer, seat) = self.seat_with_floor(fiber, Duration::from_secs(5), floor_ms);
        (peer, self.host.instantiate(&self.component, seat))
    }
}

const COUNTER: &str = "jinn:test/counter";
const EVENT_TOPIC: &str = "jinn:test/topic";
const NESTED_TOPIC: &str = "jinn:test/nested";

#[tokio::test]
async fn wrong_hash_refuses_to_load_and_the_refusal_is_recorded() {
    let ledger = Arc::new(Recording {
        events: Mutex::new(Vec::new()),
    });
    let host = WasmHost::new().unwrap_or_else(|error| panic!("host: {error:?}"));
    let (bytes, _) = support::pinned_fixture();
    let refused = host.load(bytes, "0000000000000000", ledger.as_ref());
    assert_eq!(
        refused.err().map(|error| error.code),
        Some(ErrorCode::InvalidProfile)
    );
    assert!(matches!(
        ledger.kinds().as_slice(),
        [LedgerEventKind::ArtifactRefused { .. }]
    ));
}

/// Round-2 blocker-1 pin (card: "WIT files pass wasmtime validation"; Law 1):
/// `Component::new` proves the bytes are a component, not that they are a
/// PLUGIN — a valid component that does not implement the `jinn:plugin`
/// world must be refused at registration, recorded, never admitted.
#[tokio::test]
async fn a_component_without_the_plugin_world_is_refused_at_registration() {
    // A minimal empty core module, encoded to a VALID component that
    // exports nothing.
    const EMPTY_CORE: &[u8] = &[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
    let bytes = wit_component::ComponentEncoder::default()
        .module(EMPTY_CORE)
        .unwrap_or_else(|error| panic!("module: {error:#}"))
        .validate(true)
        .encode()
        .unwrap_or_else(|error| panic!("encode: {error:#}"));
    // The pin is real only while wasmtime itself accepts these bytes as a
    // well-formed component: the refusal below must come from the world
    // check, not from malformed input.
    let mut config = wasmtime::Config::new();
    config.wasm_component_model(true);
    let engine = wasmtime::Engine::new(&config).unwrap_or_else(|error| panic!("engine: {error:#}"));
    assert!(
        wasmtime::component::Component::new(&engine, &bytes).is_ok(),
        "the probe must be a VALID component"
    );

    let ledger = Arc::new(Recording {
        events: Mutex::new(Vec::new()),
    });
    let host = WasmHost::new().unwrap_or_else(|error| panic!("host: {error:?}"));
    let hash = jinnd_wasm::hex_digest(&bytes);
    let refused = host.load(bytes, &hash, ledger.as_ref());
    assert_eq!(
        refused.err().map(|error| error.code),
        Some(ErrorCode::InvalidProfile),
        "a component without the required plugin world must be rejected at registration"
    );
    assert!(
        ledger
            .kinds()
            .iter()
            .any(|kind| matches!(kind, LedgerEventKind::ArtifactRefused { .. })),
        "the refusal is a ledger event, never silent"
    );
}

#[tokio::test]
async fn activation_collects_guest_effects_and_undo_replays_over_the_boundary() {
    let rig = rig();
    let (_, instance) = rig.spawn(1);
    let (outcome, contributed) = instance.activate(b"plain".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("activate: {error:?}"));
    assert_eq!(
        contributed.effects().collect::<Vec<_>>(),
        vec![("fixture effect", 1)]
    );
    instance
        .undo(1)
        .await
        .unwrap_or_else(|error| panic!("undo: {error:?}"));
}

#[tokio::test]
async fn native_and_guest_callers_cross_the_same_broker_choke_point() {
    let rig = rig();

    // Guest provider (granted its own contract: providing is authority).
    let (provider_peer, provider) = rig.spawn(1);
    rig.broker.grant(provider_peer, COUNTER);
    let (outcome, _) = provider.activate(b"provider".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("provider activate: {error:?}"));

    // Harness-native caller through the broker (the harness lane).
    let native = rig.broker.register_peer(Some(FiberId(9)));
    rig.broker.grant(native, COUNTER);
    let handle = rig
        .broker
        .resolve(native, COUNTER)
        .unwrap_or_else(|error| panic!("resolve: {error:?}"));
    let answer = rig
        .broker
        .call(native, handle, "add", 2u64.to_le_bytes().to_vec())
        .await
        .unwrap_or_else(|error| panic!("call: {error:?}"));
    assert_eq!(answer, 2u64.to_le_bytes().to_vec());

    // Guest caller through the SAME broker: a native provider answers.
    struct Greeter;
    impl Peer for Greeter {
        fn call(
            &self,
            _: PeerId,
            _: &str,
            _: &str,
            payload: Vec<u8>,
        ) -> KernelFuture<'static, Vec<u8>> {
            let mut answer = b"hello ".to_vec();
            answer.extend(payload);
            Box::pin(async move { Ok(answer) })
        }
    }
    let native_provider = rig.broker.register_peer(Some(FiberId(8)));
    rig.broker.grant(native_provider, "jinn:test/greeter");
    rig.broker
        .provide(native_provider, "jinn:test/greeter", Arc::new(Greeter))
        .unwrap_or_else(|error| panic!("provide: {error:?}"));
    let (caller_peer, caller) = rig.spawn(2);
    rig.broker.grant(caller_peer, "jinn:test/greeter");
    let (outcome, _) = caller.activate(b"caller".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("caller activate: {error:?}"));
    // The guest stashed the native provider's answer: the call crossed
    // guest → broker → native and back.
    let stashed = caller
        .contract_call(0, "jinn:test/counter", "stash", Vec::new())
        .await
        .unwrap_or_else(|error| panic!("stash read: {error:?}"));
    assert_eq!(stashed, b"hello from-guest".to_vec());

    // R4: the guest provider observes the caller's identity on the call the
    // broker dispatched — the handle carried the caller's scope.
    let whoami_handle = rig
        .broker
        .resolve(native, COUNTER)
        .unwrap_or_else(|error| panic!("resolve: {error:?}"));
    let observed = rig
        .broker
        .call(native, whoami_handle, "whoami", Vec::new())
        .await
        .unwrap_or_else(|error| panic!("whoami: {error:?}"));
    assert_eq!(observed, native.to_le_bytes().to_vec());

    // The choke-point proof: BOTH callers' crossings are in ONE ledger with
    // the same event shape, appended by the same broker.
    let calls: Vec<(String, String)> = rig
        .ledger
        .kinds()
        .into_iter()
        .filter_map(|kind| match kind {
            LedgerEventKind::ContractCall {
                contract,
                operation,
            } => Some((contract, operation)),
            _ => None,
        })
        .collect();
    assert!(calls.contains(&(COUNTER.to_owned(), "add".to_owned())));
    assert!(calls.contains(&("jinn:test/greeter".to_owned(), "greet".to_owned())));
}

#[tokio::test]
async fn a_guest_resolve_without_a_grant_is_refused_and_recorded() {
    let rig = rig();
    let (_, instance) = rig.spawn(1);
    let (outcome, _) = instance.activate(b"ungranted".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("the guest observed no refusal: {error:?}"));
    assert!(rig.kinds_contains_refusal("jinn:test/secret"));
}

impl Rig {
    fn kinds_contains_refusal(&self, contract: &str) -> bool {
        self.ledger.kinds().iter().any(
            |kind| matches!(kind, LedgerEventKind::GrantRefused { contract: c, .. } if c == contract),
        )
    }
}

#[tokio::test]
async fn an_ungranted_guest_provide_is_refused_and_recorded() {
    let rig = rig();
    let (_, instance) = rig.spawn(1);
    // `provider` mode provides the counter contract; without its grant the
    // provision is refused at the broker and the activation faults (Law 1 —
    // never accepted by default).
    let (outcome, _) = instance.activate(b"provider".to_vec()).await;
    assert!(outcome.is_err(), "the ungranted provide must refuse");
    assert!(
        rig.kinds_contains_refusal(COUNTER),
        "mechanical closure: the denial is a ledger event"
    );
}

#[tokio::test]
async fn listening_is_grant_gated_and_a_granted_listener_receives_deliveries() {
    use jinnd_api::DispatchMode;
    use jinnd_wasm::Selector;

    let rig = rig();
    const TOPIC: &str = "jinn:test/topic";

    // Granted listener registers and receives.
    let (granted_peer, granted) = rig.spawn(1);
    rig.broker.grant(granted_peer, TOPIC);
    let (outcome, contributed) = granted.activate(b"listener".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("listener activate: {error:?}"));
    assert_eq!(contributed.listens().count(), 1);

    // Ungranted listener is refused; the guest observes the refusal and the
    // denial is recorded (constitution 01: subscriptions are covered by the
    // contract grant in v0.1).
    let (_, denied) = rig.spawn(2);
    let (outcome, _) = denied.activate(b"eavesdrop".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("the guest observed no refusal: {error:?}"));
    assert!(rig.kinds_contains_refusal(TOPIC));

    // Only the granted listener is registered: one delivery, one output.
    let report = rig
        .topics
        .emit(
            0,
            TOPIC,
            DispatchMode::Serial,
            &Selector::All,
            b"ping".to_vec(),
            None,
            &NoRealms,
        )
        .await;
    assert_eq!(report.outputs, vec![b"ping".to_vec()]);
    assert!(report.failures.is_empty());
}

#[tokio::test]
async fn the_fs_import_is_grant_gated_and_routes_over_the_broker() {
    let rig = rig();

    // A native provider answers the jinn:fs contract over the broker.
    struct Files;
    impl Peer for Files {
        fn call(
            &self,
            _: PeerId,
            _: &str,
            operation: &str,
            payload: Vec<u8>,
        ) -> KernelFuture<'static, Vec<u8>> {
            let mut answer = format!("{operation}=").into_bytes();
            answer.extend(payload);
            Box::pin(async move { Ok(answer) })
        }
    }
    let files = rig.broker.register_peer(None);
    rig.broker.grant(files, "jinn:fs");
    rig.broker
        .provide(files, "jinn:fs", Arc::new(Files))
        .unwrap_or_else(|error| panic!("provide: {error:?}"));

    // Granted guest: the import call crosses the broker and is answered.
    let (reader_peer, reader) = rig.spawn(1);
    rig.broker.grant(reader_peer, "jinn:fs");
    let (outcome, _) = reader.activate(b"fs".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("fs activate: {error:?}"));
    let stashed = reader
        .contract_call(0, COUNTER, "stash", Vec::new())
        .await
        .unwrap_or_else(|error| panic!("stash: {error:?}"));
    assert_eq!(stashed, b"read=/probe".to_vec());
    assert!(
        rig.ledger.kinds().contains(&LedgerEventKind::ContractCall {
            contract: "jinn:fs".into(),
            operation: "read".into()
        }),
        "the host-provider import crossing is ledgered (Law 2)"
    );

    // Ungranted guest: refused, recorded, observed by the guest.
    let (_, denied) = rig.spawn(2);
    let (outcome, _) = denied.activate(b"fs-denied".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("the guest observed no refusal: {error:?}"));
    assert!(rig.kinds_contains_refusal("jinn:fs"));
}

#[tokio::test]
async fn a_trapping_guest_deactivates_only_its_own_instance() {
    let rig = rig();
    let (_, trapping) = rig.spawn(1);
    let (outcome, _) = trapping.activate(b"trap".to_vec()).await;
    let error = match outcome {
        Err(error) => error,
        Ok(()) => panic!("a trap must surface as a contained error"),
    };
    assert_eq!(error.code, ErrorCode::PluginFailed);
    assert!(error.message.contains("trap"), "attributed: {error:?}");

    // The sibling is untouched (R11).
    let (_, sibling) = rig.spawn(2);
    let (outcome, contributed) = sibling.activate(b"plain".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("sibling: {error:?}"));
    assert_eq!(contributed.effects().count(), 1);

    // The trapped instance is gone, and says so instead of hanging.
    let late = trapping.undo(1).await;
    assert!(late.is_err());
}

#[tokio::test]
async fn a_spinning_guest_is_killed_at_the_deadline_not_the_executor() {
    let rig = rig();
    let (_, seat) = rig.seat(1, Duration::from_millis(300));
    let spinning = rig.host.instantiate(&rig.component, seat);
    let started = std::time::Instant::now();
    let (outcome, _) = spinning.activate(b"spin".to_vec()).await;
    let error = match outcome {
        Err(error) => error,
        Ok(()) => panic!("a spin must hit the deadline"),
    };
    assert!(error.message.contains("deadline"), "{error:?}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the deadline bounded the spin"
    );

    // The executor survived — a sibling instance still activates.
    let (_, sibling) = rig.spawn(2);
    let (outcome, _) = sibling.activate(b"plain".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("sibling: {error:?}"));
}

#[tokio::test]
async fn a_listener_spends_its_own_fuel_and_retains_its_death_notice() {
    let mut messages = Vec::new();
    for _ in 0..3 {
        let rig = rig();
        let (peer, listener) = rig.spawn(1);
        rig.broker.grant(peer, EVENT_TOPIC);
        listener
            .activate(b"listener-budget".to_vec())
            .await
            .0
            .unwrap_or_else(|error| panic!("listener activation: {error:?}"));
        let mut deaths = listener.deaths();

        let started = std::time::Instant::now();
        let report = rig
            .topics
            .emit(
                2,
                EVENT_TOPIC,
                jinnd_api::DispatchMode::Emit,
                &jinnd_wasm::Selector::All,
                b"ping".to_vec(),
                Some(FiberId(2)),
                &NoRealms,
            )
            .await;
        assert_eq!(report.failures.len(), 1);
        assert!(started.elapsed() < Duration::from_secs(1));
        deaths
            .changed()
            .await
            .unwrap_or_else(|error| panic!("death: {error}"));
        let death = deaths
            .borrow()
            .clone()
            .unwrap_or_else(|| panic!("the fatal notice is retained"));
        assert_eq!(death.fiber, Some(FiberId(1)));
        messages.push(death.message);
    }
    assert_eq!(
        messages,
        vec!["guest exhausted its delivery fuel budget"; 3]
    );
}

#[tokio::test]
async fn a_walk_parks_the_emitter_while_the_listener_spends_its_deadline() {
    let rig = rig();
    jinnd_wasm::HostClock::register(&rig.broker)
        .unwrap_or_else(|error| panic!("clock provider: {error:?}"));
    let (listener_peer, listener_seat) = rig.seat(1, Duration::from_millis(300));
    rig.broker.grant(listener_peer, EVENT_TOPIC);
    rig.broker.grant(listener_peer, jinnd_wasm::CLOCK_CONTRACT);
    let listener = rig.host.instantiate(&rig.component, listener_seat);
    listener
        .activate(b"listener-slow".to_vec())
        .await
        .0
        .unwrap_or_else(|error| panic!("listener activation: {error:?}"));
    let probe = rig
        .topics
        .emit(
            99,
            EVENT_TOPIC,
            jinnd_api::DispatchMode::Serial,
            &jinnd_wasm::Selector::All,
            Vec::new(),
            Some(FiberId(99)),
            &NoRealms,
        )
        .await;
    assert_eq!(
        probe.outputs.len(),
        1,
        "the listener registration must be live"
    );
    assert!(probe.failures.is_empty(), "listener probe: {probe:?}");

    let (_, emitter_seat) = rig.seat(2, Duration::from_millis(80));
    let emitter = rig.host.instantiate(&rig.component, emitter_seat);
    let started = std::time::Instant::now();
    emitter
        .activate(b"emitter".to_vec())
        .await
        .0
        .unwrap_or_else(|error| panic!("the emitter must survive the walk: {error:?}"));
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(100),
        "the listener walk returned after only {elapsed:?}"
    );
}

#[tokio::test]
async fn a_listener_that_emits_a_nested_walk_is_not_charged_as_the_emitter() {
    let rig = rig();
    jinnd_wasm::HostClock::register(&rig.broker)
        .unwrap_or_else(|error| panic!("clock provider: {error:?}"));
    // Two nested listeners, each delivery well under its own 300 ms bound;
    // the nested walk sums to ~240 ms.
    for fiber in [1, 2] {
        let (peer, seat) = rig.seat(fiber, Duration::from_millis(300));
        rig.broker.grant(peer, NESTED_TOPIC);
        rig.broker.grant(peer, jinnd_wasm::CLOCK_CONTRACT);
        let nested = rig.host.instantiate(&rig.component, seat);
        nested
            .activate(b"nested-listener-slow".to_vec())
            .await
            .0
            .unwrap_or_else(|error| panic!("nested listener activation: {error:?}"));
    }
    // The middle listener's own bound is SHORTER than the walk it parks on:
    // card (a) says its clock does not run while it is the emitter.
    let (peer, seat) = rig.seat(3, Duration::from_millis(150));
    rig.broker.grant(peer, EVENT_TOPIC);
    let middle = rig.host.instantiate(&rig.component, seat);
    middle
        .activate(b"nested-emitter".to_vec())
        .await
        .0
        .unwrap_or_else(|error| panic!("middle listener activation: {error:?}"));

    let report = rig
        .topics
        .emit(
            9,
            EVENT_TOPIC,
            jinnd_api::DispatchMode::Emit,
            &jinnd_wasm::Selector::All,
            b"ping".to_vec(),
            Some(FiberId(9)),
            &NoRealms,
        )
        .await;
    assert!(
        report.failures.is_empty(),
        "the middle listener was charged for the nested walk: {:?}",
        report.failures
    );
    assert_eq!(report.outputs, vec![b"ping".to_vec()]);
    assert!(
        middle.deaths().borrow().is_none(),
        "the middle listener died: {:?}",
        middle.deaths().borrow()
    );
}

#[tokio::test]
async fn an_unbudgeted_listener_keeps_the_guest_deadline_bound() {
    let rig = rig();
    let (peer, seat) = rig.seat(1, Duration::from_millis(80));
    rig.broker.grant(peer, EVENT_TOPIC);
    let listener = rig.host.instantiate(&rig.component, seat);
    listener
        .activate(b"listener-spin".to_vec())
        .await
        .0
        .unwrap_or_else(|error| panic!("listener activation: {error:?}"));
    let report = rig
        .topics
        .emit(
            2,
            EVENT_TOPIC,
            jinnd_api::DispatchMode::Emit,
            &jinnd_wasm::Selector::All,
            Vec::new(),
            Some(FiberId(2)),
            &NoRealms,
        )
        .await;
    assert_eq!(report.failures.len(), 1);
    assert!(report.failures[0].message.contains("deadline"));
}

#[tokio::test]
async fn a_queued_delivery_starts_its_listener_horizon_at_selection() {
    let rig = rig();
    jinnd_wasm::HostClock::register(&rig.broker)
        .unwrap_or_else(|error| panic!("clock provider: {error:?}"));
    let (peer, seat) = rig.seat(1, Duration::from_millis(200));
    rig.broker.grant(peer, EVENT_TOPIC);
    rig.broker.grant(peer, jinnd_wasm::CLOCK_CONTRACT);
    let listener = rig.host.instantiate(&rig.component, seat);
    listener
        .activate(b"listener-slow".to_vec())
        .await
        .0
        .unwrap_or_else(|error| panic!("listener activation: {error:?}"));

    let busy = {
        let listener = listener.clone();
        tokio::spawn(async move {
            listener
                .contract_call(2, COUNTER, "stall", Vec::new())
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    let started = std::time::Instant::now();
    let report = rig
        .topics
        .emit(
            2,
            EVENT_TOPIC,
            jinnd_api::DispatchMode::Serial,
            &jinnd_wasm::Selector::All,
            Vec::new(),
            Some(FiberId(2)),
            &NoRealms,
        )
        .await;
    assert!(
        busy.await
            .unwrap_or_else(|error| panic!("busy call: {error}"))
            .is_ok()
    );
    assert_eq!(report.failures.len(), 1, "the queued delivery expires");
    assert!(
        started.elapsed() < Duration::from_millis(260),
        "queue delay cannot extend the listener horizon"
    );
}

#[tokio::test]
async fn a_zero_delivery_budget_is_refused_on_record_without_registration() {
    let rig = rig();
    let (peer, listener) = rig.spawn(1);
    rig.broker.grant(peer, EVENT_TOPIC);
    listener
        .activate(b"listener-zero".to_vec())
        .await
        .0
        .unwrap_or_else(|error| panic!("the guest expected the refusal: {error:?}"));
    assert!(rig.ledger.kinds().iter().any(|kind| matches!(
        kind,
        LedgerEventKind::ErrorRecorded { error }
            if error.code == ErrorCode::InvalidProfile && error.fiber == Some(FiberId(1))
    )));
    let report = rig
        .topics
        .emit(
            2,
            EVENT_TOPIC,
            jinnd_api::DispatchMode::Serial,
            &jinnd_wasm::Selector::All,
            Vec::new(),
            Some(FiberId(2)),
            &NoRealms,
        )
        .await;
    assert!(report.outputs.is_empty());
    assert!(report.failures.is_empty());
}

#[tokio::test]
async fn disposal_withdraws_exactly_the_instance_contribution() {
    let rig = rig();
    let (peer, provider) = rig.spawn(1);
    rig.broker.grant(peer, COUNTER);
    let (outcome, _) = provider.activate(b"provider".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("activate: {error:?}"));

    provider.dispose().await;
    rig.broker.remove_peer(peer);

    let native = rig.broker.register_peer(None);
    rig.broker.grant(native, COUNTER);
    let handle = rig
        .broker
        .resolve(native, COUNTER)
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    assert_eq!(
        rig.broker
            .call(native, handle, "get", Vec::new())
            .await
            .err()
            .map(|error| error.code),
        Some(ErrorCode::MissingDependency),
        "no trace of the disposed provider remains (I1)"
    );
    assert!(
        rig.ledger
            .kinds()
            .contains(&LedgerEventKind::ServiceWithdrawn {
                service: COUNTER.to_owned()
            })
    );

    // A fresh instance starts from nothing: state died with the store.
    let (fresh_peer, fresh) = rig.spawn(3);
    rig.broker.grant(fresh_peer, COUNTER);
    let (outcome, _) = fresh.activate(b"provider".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("fresh activate: {error:?}"));
    let handle = rig
        .broker
        .resolve(native, COUNTER)
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    let answer = rig
        .broker
        .call(native, handle, "get", Vec::new())
        .await
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    assert_eq!(answer, 0u64.to_le_bytes().to_vec());
}

/// M1-P9b (R5, Law 2, LAW §3): the seat's teardown is ONE LIFO replay of the
/// interleaved registration journal — never per-category loops — and every
/// withdrawal is ledgered at the moment it runs, so the recorded dispose
/// trail is strictly reverse of the registration sequence.
#[tokio::test]
async fn retire_replays_the_whole_contribution_in_reverse_registration_order() {
    let rig = rig();
    let (peer, instance) = rig.spawn(1);
    rig.broker.grant(peer, COUNTER);
    rig.broker.grant(peer, "jinn:test/topic");
    let (outcome, contributed) = instance.activate(b"interleave".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("interleave activate: {error:?}"));
    let seat = jinnd_wasm::SeatState::live(instance, contributed);
    let before = rig.ledger.kinds().len();
    seat.retire(
        &rig.broker,
        &rig.topics,
        &rig.alarms,
        peer,
        Some((rig.ledger.as_ref() as &dyn LedgerSink, FiberId(1))),
    )
    .await
    .unwrap_or_else(|error| panic!("retire: {error:?}"));
    let trail: Vec<String> = rig.ledger.kinds()[before..]
        .iter()
        .filter_map(|kind| match kind {
            LedgerEventKind::EffectWithdrawn { label, clean: true } => Some(label.clone()),
            LedgerEventKind::ServiceWithdrawn { service } => Some(format!("provide {service}")),
            _ => None,
        })
        .collect();
    assert_eq!(
        trail,
        vec![
            "second effect".to_owned(),
            "listen jinn:test/topic".to_owned(),
            format!("provide {COUNTER}"),
            "first effect".to_owned(),
        ],
        "teardown is one LIFO replay of the interleaved journal"
    );
}

#[tokio::test]
async fn the_vitality_seam_answers_per_consumer_over_the_broker() {
    let rig = rig();
    let (picky_peer, picky) = rig.spawn(1);
    rig.broker.grant(picky_peer, COUNTER);
    let (outcome, _) = picky.activate(b"picky".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("activate: {error:?}"));
    assert_eq!(rig.broker.vitality(COUNTER, 2).await, Ok(true));
    assert_eq!(rig.broker.vitality(COUNTER, 3).await, Ok(false));
}

struct Lane {
    rig: Rig,
    next_fiber: Mutex<u64>,
    live: Mutex<HashMap<EntryId, InstanceHandle>>,
    swap_config: HashMap<EntryId, Vec<u8>>,
}

impl Lane {
    fn seat(&self) -> Seat {
        let mut next = self.next_fiber.lock().unwrap_or_else(|p| p.into_inner());
        *next += 1;
        let (_, seat) = self.rig.seat(*next + 100, Duration::from_secs(5));
        seat
    }
}

impl SwapSlots for Lane {
    type Prepared = (InstanceHandle, jinnd_wasm::ActivationOutcome);
    type Displaced = InstanceHandle;

    fn entries_pinned_to(&self, _: &str) -> Vec<(EntryId, u64)> {
        let mut entries: Vec<EntryId> = self
            .live
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .keys()
            .cloned()
            .collect();
        entries.sort();
        entries
            .into_iter()
            .enumerate()
            .map(|(index, entry)| (entry, index as u64 + 1))
            .collect()
    }

    fn prepare(&self, entry: &EntryId) -> KernelFuture<'_, Self::Prepared> {
        let entry = entry.clone();
        Box::pin(async move {
            let old = self
                .live
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&entry)
                .cloned()
                .ok_or_else(|| jinnd_api::KernelError {
                    code: ErrorCode::InvalidProfile,
                    message: "unknown entry".into(),
                    fiber: None,
                })?;
            let handoff = old.snapshot().await?;
            let fresh = self.rig.host.instantiate(&self.rig.component, self.seat());
            let config = self
                .swap_config
                .get(&entry)
                .cloned()
                .unwrap_or_else(|| b"plain".to_vec());
            let (outcome, contributed) = fresh.activate(config).await;
            let healthy = match outcome {
                Ok(()) => fresh.restore(handoff).await,
                Err(error) => Err(error),
            };
            if let Err(error) = healthy {
                for (_, token) in contributed.effects().rev() {
                    let _ = fresh.undo(token).await;
                }
                fresh.dispose().await;
                return Err(error);
            }
            Ok((fresh, contributed))
        })
    }

    fn commit(&self, entry: &EntryId, prepared: Self::Prepared) -> Option<InstanceHandle> {
        // Sync, infallible bookkeeping: the seat pointer swap (round-3).
        self.live
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(entry.clone(), prepared.0)
    }

    fn retire_displaced(&self, _: &EntryId, displaced: InstanceHandle) -> KernelFuture<'_, ()> {
        Box::pin(async move {
            displaced.dispose().await;
            Ok(())
        })
    }

    fn discard(&self, prepared: Self::Prepared) -> KernelFuture<'_, ()> {
        Box::pin(async move {
            // Replay the staged effects in reverse before disposal (R5, I1).
            let (instance, contributed) = prepared;
            for (_, token) in contributed.effects().rev() {
                let _ = instance.undo(token).await;
            }
            instance.dispose().await;
            Ok(())
        })
    }
}

async fn seeded_lane(rig: Rig) -> Lane {
    let lane = Lane {
        rig,
        next_fiber: Mutex::new(0),
        live: Mutex::new(HashMap::new()),
        swap_config: HashMap::new(),
    };
    for (name, seed) in [("entry-a", 5u64), ("entry-b", 9u64)] {
        let seat = lane.seat();
        let instance = lane.rig.host.instantiate(&lane.rig.component, seat);
        let (outcome, _) = instance.activate(b"plain".to_vec()).await;
        outcome.unwrap_or_else(|error| panic!("seed activate: {error:?}"));
        instance
            .restore(seed.to_le_bytes().to_vec())
            .await
            .unwrap_or_else(|error| panic!("seed: {error:?}"));
        lane.live
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(EntryId(name.to_owned()), instance);
    }
    lane
}

async fn counter_of(lane: &Lane, entry: &str) -> u64 {
    let instance = lane
        .live
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(&EntryId(entry.to_owned()))
        .cloned()
        .unwrap_or_else(|| panic!("no instance for {entry}"));
    // A direct provider-face call: reads the live instance's counter.
    let answer = jinnd_wasm::Peer::call(&TestFace(instance), 0, COUNTER, "get", Vec::new())
        .await
        .unwrap_or_else(|error| panic!("get: {error:?}"));
    u64::from_le_bytes(answer.try_into().unwrap_or_else(|_| panic!("bad answer")))
}

/// Reaches one instance's contract face directly for observation.
struct TestFace(InstanceHandle);

impl Peer for TestFace {
    fn call(
        &self,
        caller: PeerId,
        contract: &str,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let instance = self.0.clone();
        let (contract, operation) = (contract.to_owned(), operation.to_owned());
        Box::pin(async move {
            instance
                .contract_call(caller, &contract, &operation, payload)
                .await
        })
    }
}

#[tokio::test]
async fn mode1_swap_batches_by_artifact_and_hands_state_across() {
    let lane = seeded_lane(rig()).await;
    let hash = lane.rig.component.hash().to_owned();
    let core = jinnd_wasm::SwapCore::default();
    let outcome = swap_batch(&lane, &core, &hash, &hash, lane.rig.ledger.as_ref())
        .await
        .unwrap_or_else(|error| panic!("swap: {error:?}"));
    assert!(!outcome.rolled_back);
    assert_eq!(
        outcome.swapped.len(),
        2,
        "both entries sharing the hash swapped"
    );
    assert_eq!(counter_of(&lane, "entry-a").await, 5, "state handed off");
    assert_eq!(counter_of(&lane, "entry-b").await, 9, "state handed off");
    let phases: Vec<SwapPhaseKind> = lane
        .rig
        .ledger
        .kinds()
        .into_iter()
        .filter_map(|kind| match kind {
            LedgerEventKind::SwapPhase { phase, .. } => Some(phase),
            _ => None,
        })
        .collect();
    assert_eq!(
        phases,
        vec![
            SwapPhaseKind::Began,
            SwapPhaseKind::InstanceHealthy,
            SwapPhaseKind::InstanceHealthy,
            SwapPhaseKind::Committed
        ]
    );
}

#[tokio::test]
async fn mode1_swap_rolls_back_whole_batch_with_old_instances_warm() {
    let mut lane = seeded_lane(rig()).await;
    lane.swap_config
        .insert(EntryId("entry-b".to_owned()), b"trap".to_vec());
    let hash = lane.rig.component.hash().to_owned();
    let core = jinnd_wasm::SwapCore::default();
    let outcome = swap_batch(&lane, &core, &hash, &hash, lane.rig.ledger.as_ref())
        .await
        .unwrap_or_else(|error| panic!("swap: {error:?}"));
    assert!(outcome.rolled_back);
    assert!(outcome.swapped.is_empty());
    assert_eq!(
        counter_of(&lane, "entry-a").await,
        5,
        "old instance still warm"
    );
    assert_eq!(
        counter_of(&lane, "entry-b").await,
        9,
        "old instance still warm"
    );
    let phases: Vec<SwapPhaseKind> = lane
        .rig
        .ledger
        .kinds()
        .into_iter()
        .filter_map(|kind| match kind {
            LedgerEventKind::SwapPhase { phase, .. } => Some(phase),
            _ => None,
        })
        .collect();
    assert_eq!(
        phases,
        vec![
            SwapPhaseKind::Began,
            SwapPhaseKind::InstanceHealthy,
            SwapPhaseKind::RolledBack
        ]
    );
}

/// M2-K2 (Law 2, R6): the granted clock read crosses the SAME broker choke
/// point as every capability — the call is ledgered, the answer is the
/// 8-byte LE epoch-millisecond wire the contract declares.
#[tokio::test]
async fn clock_now_reads_through_the_broker_choke_point() {
    let rig = rig();
    jinnd_wasm::HostClock::register(&rig.broker)
        .unwrap_or_else(|error| panic!("clock provider: {error:?}"));
    let (peer, instance) = rig.spawn(1);
    rig.broker.grant(peer, jinnd_wasm::CLOCK_CONTRACT);
    rig.broker.grant(peer, COUNTER);
    let (outcome, _) = instance.activate(b"clock-now".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("clock-now activate: {error:?}"));

    let native = rig.broker.register_peer(None);
    rig.broker.grant(native, COUNTER);
    // The stash holds the guest's clock reading — a plausible epoch instant.
    let stashed = instance
        .contract_call(native, COUNTER, "stash", Vec::new())
        .await
        .unwrap_or_else(|error| panic!("stash: {error:?}"));
    assert_eq!(stashed.len(), 8, "8-byte LE instant per the contract wire");
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&stashed);
    assert!(u64::from_le_bytes(bytes) > 1_577_836_800_000);

    assert!(
        rig.ledger.kinds().iter().any(|kind| matches!(
            kind,
            LedgerEventKind::ContractCall { contract, operation }
                if contract == jinnd_wasm::CLOCK_CONTRACT && operation == "now"
        )),
        "the read's only obligation is the call ledger line — and it is there"
    );
}

/// M2-K2 (Law 1, constitution 01): without the grant, both the read and the
/// alarm surface refuse, and every refusal is a ledger event.
#[tokio::test]
async fn an_ungranted_clock_call_is_refused_and_recorded() {
    let rig = rig();
    jinnd_wasm::HostClock::register(&rig.broker)
        .unwrap_or_else(|error| panic!("clock provider: {error:?}"));
    let (_, instance) = rig.spawn(1);
    let (outcome, _) = instance.activate(b"clock-denied".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("the guest observed no refusal: {error:?}"));
    assert!(rig.kinds_contains_refusal(jinnd_wasm::CLOCK_CONTRACT));
}

/// M2-K2 (R9): a periodic alarm finer than the granted resolution floor is
/// refused at the request — no free high-frequency wake hazard.
#[tokio::test]
async fn an_alarm_period_finer_than_the_floor_is_refused() {
    let rig = rig();
    let (peer, instance) = rig.spawn(1);
    rig.broker.grant(peer, jinnd_wasm::CLOCK_CONTRACT);
    let (outcome, _) = instance.activate(b"clock-fast".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("the guest observed no refusal: {error:?}"));
}

/// M2-K2 (R9, card acceptance): grants scope alarm resolution PER ENTRY —
/// a seat whose grant holds a coarser floor refuses a period the default
/// floor would admit (the fixture's 250ms request against a 1000ms scope).
#[tokio::test]
async fn a_scoped_grant_caps_how_fine_a_timer_an_entry_may_hold() {
    let rig = rig();
    let (peer, instance) = rig.spawn_with_floor(1, 1000);
    rig.broker.grant(peer, jinnd_wasm::CLOCK_CONTRACT);
    rig.broker.grant(peer, COUNTER);
    let (outcome, _) = instance.activate(b"clock-alarm".to_vec()).await;
    let refused = match outcome {
        Err(refused) => refused,
        Ok(()) => panic!("a 250ms request must refuse under a 1000ms granted floor (R9)"),
    };
    assert!(
        refused.message.contains("1000ms"),
        "the refusal names the entry's own granted floor: {}",
        refused.message
    );
}

/// M2-K2 acceptance: a fixture plugin requests a periodic alarm, receives
/// typed wakes (right token, topic, and 8-byte payload — the fixture faults
/// on anything else), every wake is a ledger event, and the seat's teardown
/// — the effect's undo — cancels the alarm: no wake is ledgered after it.
#[tokio::test]
async fn a_periodic_alarm_wakes_the_guest_until_teardown_cancels_it() {
    let rig = rig();
    let (peer, instance) = rig.spawn(1);
    rig.broker.grant(peer, jinnd_wasm::CLOCK_CONTRACT);
    rig.broker.grant(peer, COUNTER);
    let (outcome, contributed) = instance.activate(b"clock-alarm".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("clock-alarm activate: {error:?}"));

    let native = rig.broker.register_peer(None);
    rig.broker.grant(native, COUNTER);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let count = instance
            .contract_call(native, COUNTER, "get", Vec::new())
            .await
            .unwrap_or_else(|error| panic!("get: {error:?}"));
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&count);
        if u64::from_le_bytes(bytes) >= 2 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "two typed wakes should arrive well within the deadline"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let wakes = |kinds: &[LedgerEventKind]| {
        kinds
            .iter()
            .filter(|kind| matches!(kind, LedgerEventKind::AlarmWake { .. }))
            .count()
    };
    assert!(
        wakes(&rig.ledger.kinds()) >= 2,
        "every wake is a ledger event (Law 2)"
    );

    // Teardown IS the undo (R5): retire the seat, the alarm cancels.
    let seat = jinnd_wasm::SeatState::live(instance, contributed);
    seat.retire(
        &rig.broker,
        &rig.topics,
        &rig.alarms,
        peer,
        Some((rig.ledger.as_ref() as &dyn LedgerSink, FiberId(1))),
    )
    .await
    .unwrap_or_else(|error| panic!("retire: {error:?}"));
    assert!(
        rig.ledger.kinds().iter().any(|kind| matches!(
            kind,
            LedgerEventKind::EffectWithdrawn { label, clean: true }
                if label == "alarm every 250ms"
        )),
        "the cancellation is ledgered under the request's own label"
    );
    let after_retire = wakes(&rig.ledger.kinds());
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(
        wakes(&rig.ledger.kinds()),
        after_retire,
        "after the undo returned, no wake is ever appended again"
    );
}

/// M2-K2 acceptance (R8): a Mode-1 commit carries live alarms through the
/// seat's staged outcome — the staged request is recorded but not armed,
/// the commit arms it against the NEW instance's own face and cancels the
/// displaced seat's alarm, and the minted id lands back in the journal.
#[tokio::test]
async fn a_swap_commit_carries_live_alarms_through_the_staged_outcome() {
    let rig = rig();
    let slot = Arc::new(jinnd_wasm::SharedSlot::default());
    let (peer, seat) = rig.seat(1, Duration::from_secs(5));
    rig.broker.grant(peer, jinnd_wasm::CLOCK_CONTRACT);
    rig.broker.grant(peer, COUNTER);
    let old = rig.host.instantiate(&rig.component, seat);
    let (outcome, contributed) = old.activate(b"clock-alarm".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("old activate: {error:?}"));
    slot.install(jinnd_wasm::SeatState::live(old, contributed));
    let (_, _, old_alarms) = slot.registrations();
    assert_eq!(old_alarms.len(), 1, "the live seat armed its alarm");

    // Stage the successor: its alarm request is recorded, never armed.
    let (_, mut staged_seat) = rig.seat(1, Duration::from_secs(5));
    staged_seat.peer = peer;
    staged_seat.staging = true;
    let staged = rig.host.instantiate(&rig.component, staged_seat);
    let (outcome, contributed) = staged.activate(b"clock-alarm".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("staged activate: {error:?}"));
    let recorded: Vec<_> = contributed.alarms().collect();
    assert_eq!(recorded.len(), 1);
    assert!(
        recorded[0].id.is_none(),
        "a staged alarm is recorded, not armed (R8)"
    );

    let displaced = jinnd_wasm::commit_staged(
        &slot,
        staged,
        contributed,
        &rig.broker,
        &rig.topics,
        &rig.alarms,
        peer,
        Some(FiberId(1)),
        1,
        rig.ledger.as_ref(),
    );
    if let Some(seat) = displaced {
        seat.instance.dispose().await;
    }

    let (_, _, new_alarms) = slot.registrations();
    assert_eq!(new_alarms.len(), 1, "the committed seat's alarm is live");
    assert_ne!(new_alarms[0], old_alarms[0], "ids are never reused");
    assert!(
        !rig.alarms.cancel(old_alarms[0]),
        "the displaced seat's alarm was cancelled at commit"
    );
    assert!(
        rig.alarms.cancel(new_alarms[0]),
        "the successor's alarm survived the swap, live until ITS undo"
    );
}

/// M2-K4: a sealed seat refuses registrations on the record, and a sealed
/// instance runs no further guest entry — inverses still run, so the
/// journal that teardown replays is exactly the contribution (I1). The
/// journal seal alone is the BACKSTOP (M2-K5 #16: production closes door →
/// drain → seal, so only a handler past its deadline ever meets it).
#[tokio::test]
async fn a_sealed_seat_refuses_registrations_and_the_instance_no_entries() {
    let rig = rig();
    const TOPIC: &str = "jinn:test/topic";
    let slot = Arc::new(jinnd_wasm::SharedSlot::default());
    let (peer, mut seat) = rig.seat(1, Duration::from_secs(5));
    seat.slot = Some(Arc::clone(&slot));
    rig.broker.grant(peer, TOPIC);
    let instance = rig.host.instantiate(&rig.component, seat);
    slot.seal();

    let (outcome, contributed) = instance.activate(b"listener".to_vec()).await;
    assert!(
        outcome.is_err(),
        "the listen refuses against a sealed journal"
    );
    assert_eq!(contributed.registrations.len(), 0, "nothing escaped");
    assert!(
        rig.ledger.kinds().iter().any(|kind| matches!(
            kind,
            LedgerEventKind::ErrorRecorded { error } if error.message.contains("sealed")
        )),
        "the refusal is on the record"
    );

    instance.seal().await;
    let delivered = instance.deliver(7, TOPIC, b"ping".to_vec()).await;
    assert_eq!(
        delivered.err().map(|error| error.code),
        Some(ErrorCode::InactiveContext),
        "a sealed instance runs no handler"
    );
    assert!(instance.undo(1).await.is_ok(), "inverses still run");
    instance.dispose().await;
}

/// M2-K4 ruling 4: suspension releases kernel registrations — the listener
/// unlistens, the provision withdraws, each ledgered — and hands the world
/// effects back once each, in registration order; guest inverses do not
/// run (they are instance-bound, and the instance disposes).
#[tokio::test]
async fn suspend_releases_registrations_and_hands_back_world_effects() {
    let rig = rig();
    const TOPIC: &str = "jinn:test/topic";
    let (peer, instance) = rig.spawn(1);
    rig.broker.grant(peer, COUNTER);
    rig.broker.grant(peer, TOPIC);
    let (outcome, mut contributed) = instance.activate(b"interleave".to_vec()).await;
    outcome.unwrap_or_else(|error| panic!("interleave activate: {error:?}"));
    let listen_id = contributed
        .listens()
        .next()
        .and_then(|record| record.id)
        .unwrap_or_else(|| panic!("the listener was minted an id"));
    for effect in [41, 42, 41] {
        contributed
            .registrations
            .push(jinnd_wasm::Registration::Host(jinnd_wasm::HostRecord {
                contract: "jinn:fs".to_owned(),
                label: format!("fs write x [effect {effect}]"),
                effect,
            }));
    }
    let seat = jinnd_wasm::SeatState::live(instance, contributed);
    let before = rig.ledger.kinds().len();
    let retained = seat
        .suspend(
            &rig.broker,
            &rig.topics,
            &rig.alarms,
            peer,
            Some((rig.ledger.as_ref() as &dyn LedgerSink, FiberId(1))),
        )
        .await;
    let ids: Vec<u64> = retained.iter().map(|record| record.effect).collect();
    assert_eq!(
        ids,
        vec![41, 42],
        "world effects hand back once each, in order"
    );
    assert!(
        rig.topics.unlisten(listen_id).is_none(),
        "the listener released"
    );
    let trail = &rig.ledger.kinds()[before..];
    assert!(
        trail.contains(&LedgerEventKind::EffectWithdrawn {
            label: "listen jinn:test/topic".to_owned(),
            clean: true,
        }),
        "the release is ledgered: {trail:?}"
    );
    assert!(
        trail.contains(&LedgerEventKind::ServiceWithdrawn {
            service: COUNTER.to_owned()
        }),
        "the provision withdrew: {trail:?}"
    );
    assert!(
        !trail.iter().any(|kind| matches!(
            kind,
            LedgerEventKind::EffectWithdrawn { label, .. } if label.ends_with(" effect")
        )),
        "no guest inverse ran or was ledgered as withdrawn: {trail:?}"
    );
}
