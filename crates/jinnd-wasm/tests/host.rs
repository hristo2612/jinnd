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
    let alarms = Arc::new(Alarms::new(
        ledger.clone() as Arc<dyn LedgerSink>,
        DEFAULT_MIN_PERIOD_MS,
    ));
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
                slot: None,
                staging: false,
            },
        )
    }

    fn spawn(&self, fiber: u64) -> (PeerId, InstanceHandle) {
        let (peer, seat) = self.seat(fiber, Duration::from_secs(5));
        (peer, self.host.instantiate(&self.component, seat))
    }
}

const COUNTER: &str = "jinn:test/counter";

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
            |kind| matches!(kind, LedgerEventKind::GrantRefused { contract: c } if c == contract),
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

    fn retire_displaced(&self, displaced: InstanceHandle) -> KernelFuture<'_, ()> {
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
