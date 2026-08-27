//! Broker unit tests (TDD lane of this crate; the invariant suite stays
//! verifier-owned). The pinned behaviors: grant refusal, append-before-
//! dispatch ordering, caller-scoped handles, no lock across a peer (R1).

use std::sync::{Arc, Mutex};

use jinnd_api::{ErrorCode, FiberId, KernelFuture, LedgerEventKind};

use crate::broker::Broker;
use crate::peer::{LedgerSink, Peer, PeerId};

#[derive(Default)]
pub(crate) struct CapturedLedger {
    pub(crate) events: Mutex<Vec<(LedgerEventKind, Option<FiberId>)>>,
}

impl LedgerSink for CapturedLedger {
    fn append(&self, kind: LedgerEventKind, fiber: Option<FiberId>) {
        self.events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((kind, fiber));
    }
}

impl CapturedLedger {
    fn kinds(&self) -> Vec<LedgerEventKind> {
        self.events
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .iter()
            .map(|(kind, _)| kind.clone())
            .collect()
    }
}

struct Echo;

impl Peer for Echo {
    fn call(
        &self,
        _: PeerId,
        contract: &str,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let mut answer = format!("{contract}/{operation}:").into_bytes();
        answer.extend(payload);
        Box::pin(async move { Ok(answer) })
    }
}

/// A provider that re-enters the broker while answering: if any broker lock
/// were held across dispatch, this call would deadlock (R1).
struct Reentrant {
    broker: Arc<Broker>,
    own_peer: PeerId,
}

impl Peer for Reentrant {
    fn call(&self, _: PeerId, _: &str, _: &str, _: Vec<u8>) -> KernelFuture<'static, Vec<u8>> {
        let broker = Arc::clone(&self.broker);
        let peer = self.own_peer;
        Box::pin(async move {
            broker.grant(peer, "jinn:inner");
            let refused = broker.resolve(peer, "jinn:never");
            assert!(refused.is_err());
            Ok(b"reentered".to_vec())
        })
    }
}

fn fixture() -> (Arc<Broker>, Arc<CapturedLedger>) {
    let ledger = Arc::new(CapturedLedger::default());
    (Arc::new(Broker::new(Arc::clone(&ledger) as _)), ledger)
}

#[tokio::test]
async fn ungranted_resolve_is_refused_and_the_denial_is_a_ledger_event() {
    let (broker, ledger) = fixture();
    let caller = broker.register_peer(Some(FiberId(7)));
    let refused = broker.resolve(caller, "jinn:fs");
    assert_eq!(
        refused.err().map(|error| error.code),
        Some(ErrorCode::EffectFailed)
    );
    assert_eq!(
        ledger.kinds(),
        vec![LedgerEventKind::GrantRefused {
            contract: "jinn:fs".into()
        }]
    );
    let events = ledger.events.lock().unwrap_or_else(|p| p.into_inner());
    assert_eq!(events[0].1, Some(FiberId(7)), "the refusal is attributed");
}

#[tokio::test]
async fn granted_call_appends_the_crossing_before_dispatching() {
    let (broker, ledger) = fixture();
    let provider = broker.register_peer(Some(FiberId(1)));
    broker.grant(provider, "jinn:echo");
    broker
        .provide(provider, "jinn:echo", Arc::new(Echo))
        .unwrap_or_else(|error| panic!("provision refused: {error:?}"));
    let caller = broker.register_peer(Some(FiberId(2)));
    broker.grant(caller, "jinn:echo");
    let handle = broker
        .resolve(caller, "jinn:echo")
        .unwrap_or_else(|error| panic!("resolve refused: {error:?}"));
    let answer = broker
        .call(caller, handle, "ping", b"x".to_vec())
        .await
        .unwrap_or_else(|error| panic!("call failed: {error:?}"));
    assert_eq!(answer, b"jinn:echo/ping:x".to_vec());
    assert_eq!(
        ledger.kinds(),
        vec![
            LedgerEventKind::ServiceProvided {
                service: "jinn:echo".into()
            },
            LedgerEventKind::ContractResolved {
                contract: "jinn:echo".into()
            },
            LedgerEventKind::ContractCall {
                contract: "jinn:echo".into(),
                operation: "ping".into()
            },
        ]
    );
}

#[tokio::test]
async fn a_handle_is_caller_scoped_never_transferable() {
    let (broker, _) = fixture();
    let provider = broker.register_peer(None);
    broker.grant(provider, "jinn:echo");
    broker
        .provide(provider, "jinn:echo", Arc::new(Echo))
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    let owner = broker.register_peer(None);
    broker.grant(owner, "jinn:echo");
    let handle = broker
        .resolve(owner, "jinn:echo")
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    let thief = broker.register_peer(None);
    let stolen = broker.call(thief, handle, "ping", Vec::new()).await;
    assert_eq!(
        stolen.err().map(|error| error.code),
        Some(ErrorCode::EffectFailed)
    );
}

#[tokio::test]
async fn second_provider_for_an_occupied_slot_is_refused_never_silent() {
    let (broker, _) = fixture();
    let first = broker.register_peer(None);
    broker.grant(first, "jinn:echo");
    broker
        .provide(first, "jinn:echo", Arc::new(Echo))
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    let second = broker.register_peer(None);
    broker.grant(second, "jinn:echo");
    let refused = broker.provide(second, "jinn:echo", Arc::new(Echo));
    assert_eq!(
        refused.err().map(|error| error.code),
        Some(ErrorCode::DuplicateProvision)
    );
}

#[tokio::test]
async fn peer_removal_withdraws_exactly_its_contribution() {
    let (broker, ledger) = fixture();
    let dying = broker.register_peer(Some(FiberId(3)));
    broker.grant(dying, "jinn:doomed");
    broker
        .provide(dying, "jinn:doomed", Arc::new(Echo))
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    let survivor = broker.register_peer(Some(FiberId(4)));
    broker.grant(survivor, "jinn:kept");
    broker
        .provide(survivor, "jinn:kept", Arc::new(Echo))
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    broker.remove_peer(dying);

    let caller = broker.register_peer(None);
    broker.grant(caller, "jinn:kept");
    broker.grant(caller, "jinn:doomed");
    let kept = broker
        .resolve(caller, "jinn:kept")
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    assert!(broker.call(caller, kept, "ping", Vec::new()).await.is_ok());
    let doomed = broker
        .resolve(caller, "jinn:doomed")
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    assert_eq!(
        broker
            .call(caller, doomed, "ping", Vec::new())
            .await
            .err()
            .map(|error| error.code),
        Some(ErrorCode::MissingDependency)
    );
    assert!(
        ledger.kinds().contains(&LedgerEventKind::ServiceWithdrawn {
            service: "jinn:doomed".into()
        }),
        "the withdrawal is recorded"
    );
}

#[tokio::test]
async fn dispatch_holds_no_broker_lock_across_the_peer() {
    let (broker, _) = fixture();
    let provider = broker.register_peer(None);
    let reentrant = Arc::new(Reentrant {
        broker: Arc::clone(&broker),
        own_peer: provider,
    });
    broker.grant(provider, "jinn:reenter");
    broker
        .provide(provider, "jinn:reenter", reentrant)
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    let caller = broker.register_peer(None);
    broker.grant(caller, "jinn:reenter");
    let handle = broker
        .resolve(caller, "jinn:reenter")
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    let answer = broker
        .call(caller, handle, "go", Vec::new())
        .await
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    assert_eq!(answer, b"reentered".to_vec());
}

#[tokio::test]
async fn an_ungranted_provide_is_refused_and_the_denial_is_recorded() {
    let (broker, ledger) = fixture();
    let peer = broker.register_peer(Some(FiberId(6)));
    let refused = broker.provide(peer, "jinn:unearned", Arc::new(Echo));
    assert_eq!(
        refused.err().map(|error| error.code),
        Some(ErrorCode::EffectFailed),
        "providing is authority: without a grant it is refused (Law 1)"
    );
    assert_eq!(
        ledger.kinds(),
        vec![LedgerEventKind::GrantRefused {
            contract: "jinn:unearned".into()
        }],
        "mechanical closure: the refusal is a ledger event, not a default-accept"
    );
}

#[tokio::test]
async fn an_ungranted_dispatch_is_refused_and_a_granted_one_crosses_the_broker() {
    let (broker, ledger) = fixture();
    let provider = broker.register_peer(None);
    broker.grant(provider, "jinn:fs");
    broker
        .provide(provider, "jinn:fs", Arc::new(Echo))
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));

    let caller = broker.register_peer(Some(FiberId(7)));
    let refused = broker
        .dispatch(caller, "jinn:fs", "read", b"/probe".to_vec())
        .await;
    assert_eq!(
        refused.err().map(|error| error.code),
        Some(ErrorCode::EffectFailed)
    );
    assert!(ledger.kinds().contains(&LedgerEventKind::GrantRefused {
        contract: "jinn:fs".into()
    }));

    broker.grant(caller, "jinn:fs");
    let answer = broker
        .dispatch(caller, "jinn:fs", "read", b"/probe".to_vec())
        .await
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    assert_eq!(answer, b"jinn:fs/read:/probe".to_vec());
    assert!(
        ledger.kinds().contains(&LedgerEventKind::ContractCall {
            contract: "jinn:fs".into(),
            operation: "read".into()
        }),
        "a host-provider import crossing is ledgered like any other (Law 2)"
    );
}

#[tokio::test]
async fn a_stale_handle_is_refused_never_silently_retargeted() {
    let (broker, ledger) = fixture();
    let first = broker.register_peer(None);
    broker.grant(first, "jinn:swappable");
    broker
        .provide(first, "jinn:swappable", Arc::new(Echo))
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    let caller = broker.register_peer(Some(FiberId(5)));
    broker.grant(caller, "jinn:swappable");
    let handle = broker
        .resolve(caller, "jinn:swappable")
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));

    // The provider changes: withdraw, then a DIFFERENT peer provides.
    broker.withdraw(first, "jinn:swappable");
    let second = broker.register_peer(None);
    broker.grant(second, "jinn:swappable");
    broker
        .provide(second, "jinn:swappable", Arc::new(Echo))
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));

    // The old handle pinned the old provider generation: refused, recorded —
    // never silently retargeted to the new provider (R4, R9, epoch gating).
    let refused = broker
        .call(caller, handle, "ping", b"x".to_vec())
        .await
        .err();
    assert!(
        refused
            .as_ref()
            .is_some_and(|error| error.message.contains("stale")),
        "a provider change must refuse the old handle: {refused:?}"
    );
    assert!(
        ledger
            .kinds()
            .contains(&LedgerEventKind::StaleHandleRefused {
                contract: "jinn:swappable".into()
            }),
        "the refusal is a ledger event"
    );

    // A fresh resolve pins the new generation and works.
    let fresh = broker
        .resolve(caller, "jinn:swappable")
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    assert!(broker.call(caller, fresh, "ping", Vec::new()).await.is_ok());
}

#[tokio::test]
async fn the_provider_observes_the_caller_scope_on_every_call() {
    struct Observing {
        seen: Mutex<Vec<PeerId>>,
    }
    impl Peer for Observing {
        fn call(
            &self,
            caller: PeerId,
            _: &str,
            _: &str,
            _: Vec<u8>,
        ) -> KernelFuture<'static, Vec<u8>> {
            self.seen
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(caller);
            Box::pin(async { Ok(Vec::new()) })
        }
    }
    let (broker, _) = fixture();
    let provider = broker.register_peer(None);
    let observing = Arc::new(Observing {
        seen: Mutex::new(Vec::new()),
    });
    broker.grant(provider, "jinn:observed");
    broker
        .provide(provider, "jinn:observed", Arc::clone(&observing) as _)
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    let caller = broker.register_peer(None);
    broker.grant(caller, "jinn:observed");
    let handle = broker
        .resolve(caller, "jinn:observed")
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    broker
        .call(caller, handle, "ping", Vec::new())
        .await
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    // The handle pairs the implementation with the CALLER's scope (R4):
    // the provider is told who is calling, by construction.
    assert_eq!(
        *observing.seen.lock().unwrap_or_else(|p| p.into_inner()),
        vec![caller]
    );
}

#[tokio::test]
async fn vitality_routes_to_the_provider_per_consumer() {
    struct Picky;
    impl Peer for Picky {
        fn call(&self, _: PeerId, _: &str, _: &str, _: Vec<u8>) -> KernelFuture<'static, Vec<u8>> {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn check(&self, consumer: PeerId) -> KernelFuture<'static, bool> {
            Box::pin(async move { Ok(consumer % 2 == 0) })
        }
    }
    let (broker, _) = fixture();
    let provider = broker.register_peer(None);
    broker.grant(provider, "jinn:picky");
    broker
        .provide(provider, "jinn:picky", Arc::new(Picky))
        .unwrap_or_else(|error| panic!("unexpected: {error:?}"));
    assert_eq!(broker.vitality("jinn:picky", 2).await, Ok(true));
    assert_eq!(broker.vitality("jinn:picky", 3).await, Ok(false));
    assert_eq!(
        broker.vitality("jinn:absent", 2).await,
        Ok(false),
        "no provider means not vital"
    );
}
