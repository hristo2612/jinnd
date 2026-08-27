//! Broker unit tests (TDD lane of this crate; the invariant suite stays
//! verifier-owned). The pinned behaviors: grant refusal, append-before-
//! dispatch ordering, caller-scoped handles, no lock across a peer (R1).

use std::sync::{Arc, Mutex};

use jinnd_api::{ErrorCode, FiberId, KernelFuture, LedgerEventKind};

use crate::broker::{Broker, LedgerSink, Peer, PeerId};

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
    fn call(&self, _: &str, _: &str, _: Vec<u8>) -> KernelFuture<'static, Vec<u8>> {
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
    broker
        .provide(provider, "jinn:echo", Arc::new(Echo))
        .unwrap();
    let owner = broker.register_peer(None);
    broker.grant(owner, "jinn:echo");
    let handle = broker.resolve(owner, "jinn:echo").unwrap();
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
    broker.provide(first, "jinn:echo", Arc::new(Echo)).unwrap();
    let second = broker.register_peer(None);
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
    broker
        .provide(dying, "jinn:doomed", Arc::new(Echo))
        .unwrap();
    let survivor = broker.register_peer(Some(FiberId(4)));
    broker
        .provide(survivor, "jinn:kept", Arc::new(Echo))
        .unwrap();
    broker.remove_peer(dying);

    let caller = broker.register_peer(None);
    broker.grant(caller, "jinn:kept");
    broker.grant(caller, "jinn:doomed");
    let kept = broker.resolve(caller, "jinn:kept").unwrap();
    assert!(broker.call(caller, kept, "ping", Vec::new()).await.is_ok());
    let doomed = broker.resolve(caller, "jinn:doomed").unwrap();
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
    broker.provide(provider, "jinn:reenter", reentrant).unwrap();
    let caller = broker.register_peer(None);
    broker.grant(caller, "jinn:reenter");
    let handle = broker.resolve(caller, "jinn:reenter").unwrap();
    let answer = broker.call(caller, handle, "go", Vec::new()).await.unwrap();
    assert_eq!(answer, b"reentered".to_vec());
}

#[tokio::test]
async fn vitality_routes_to_the_provider_per_consumer() {
    struct Picky;
    impl Peer for Picky {
        fn call(&self, _: &str, _: &str, _: Vec<u8>) -> KernelFuture<'static, Vec<u8>> {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn check(&self, consumer: PeerId) -> KernelFuture<'static, bool> {
            Box::pin(async move { Ok(consumer % 2 == 0) })
        }
    }
    let (broker, _) = fixture();
    let provider = broker.register_peer(None);
    broker
        .provide(provider, "jinn:picky", Arc::new(Picky))
        .unwrap();
    assert_eq!(broker.vitality("jinn:picky", 2).await, Ok(true));
    assert_eq!(broker.vitality("jinn:picky", 3).await, Ok(false));
    assert_eq!(
        broker.vitality("jinn:absent", 2).await,
        Ok(false),
        "no provider means not vital"
    );
}
