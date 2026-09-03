//! The gate as a value (M2-K24): what it waits on, what admission
//! refuses, and the epoch it computes — no runtime, no guest.

use std::sync::Arc;

use jinnd_api::{FiberId, FiberState};

use super::{Declaration, Gate, compute, covered};
use crate::broker::Broker;
use crate::broker_tests::CapturedLedger;
use crate::grants::{Grant, ScopeValue};

fn grant(contract: &str) -> Grant {
    Grant {
        contract: contract.to_owned(),
        scope: None,
        ops: None,
    }
}

fn declaration(contracts: &[&str]) -> Declaration {
    Declaration {
        contracts: contracts.iter().map(|c| (*c).to_owned()).collect(),
        faults: Vec::new(),
    }
}

struct Echo;

impl crate::peer::Peer for Echo {
    fn call(
        &self,
        _: u64,
        _: &str,
        _: &str,
        payload: Vec<u8>,
    ) -> jinnd_api::KernelFuture<'static, Vec<u8>> {
        Box::pin(async move { Ok(payload) })
    }
}

/// The gate waits on the declared contracts a grant covers by name,
/// in declaration order; an ungranted one is admission's to refuse.
#[test]
fn the_gate_waits_on_granted_declarations_in_order() {
    let declared = declaration(&["b:x", "a:y", "c:z"]);
    assert_eq!(
        covered(&declared, &[grant("c:z"), grant("b:x")]),
        ["b:x", "c:z"]
    );
    let gate = Gate::new(&declared, &[grant("b:x")]);
    let refused = gate.admission(&[grant("b:x")]);
    assert_eq!(
        refused.len(),
        2,
        "two declared contracts hold no admitted grant"
    );
    assert!(refused[0].message.contains("a:y"));
    assert!(refused[1].message.contains("c:z"));
    let faulted = Declaration {
        contracts: Vec::new(),
        faults: vec!["not an injects entry: 7".to_owned()],
    };
    let gate = Gate::new(&faulted, &[]);
    assert_eq!(
        gate.admission(&[]).len(),
        1,
        "a malformed element is a fault"
    );
}

/// Round-1 ruling 2: the gate waits only on declarations an ADMITTED
/// grant covers. A grant the admission judgment refuses (a scope on a
/// contract that declares no scope type) covers nothing: the entry is
/// left to activation, where admission faults it on the record, instead
/// of resting `Pending` forever on authority it does not hold.
#[test]
fn the_gate_ignores_a_declaration_whose_grant_admission_refuses() {
    let refused = Grant {
        contract: "jinn:ledger".to_owned(),
        scope: Some(ScopeValue::Path("nope".to_owned())),
        ops: None,
    };
    let gate = Gate::new(
        &declaration(&["jinn:ledger"]),
        std::slice::from_ref(&refused),
    );
    assert!(gate.gated().is_empty(), "a refused grant gates nothing");
    let gate = Gate::new(&declaration(&["jinn:ledger"]), &[grant("jinn:ledger")]);
    assert_eq!(gate.gated(), ["jinn:ledger"]);
    gate.restate(&declaration(&["jinn:ledger"]), &[refused]);
    assert!(
        gate.gated().is_empty(),
        "a restated refused grant gates nothing"
    );
}

/// (a) as a value: `None` while the provider is missing or merely
/// `Loading`; the epoch once it is `Active`; the unmet names on the
/// gate meanwhile; a kernel provider trivially ready; the loader's own
/// dependencies kept ahead of the declaration's.
#[test]
fn the_epoch_is_none_until_every_declared_provider_is_active() {
    let broker = Broker::new(Arc::new(CapturedLedger::default()));
    let gate = Gate::new(
        &declaration(&["jinn:kernel", "jinn:x"]),
        &[grant("jinn:kernel"), grant("jinn:x")],
    );
    let kernel = broker.register_peer(None);
    broker.grant(kernel, "jinn:kernel");
    broker
        .provide(kernel, "jinn:kernel", Arc::new(Echo))
        .unwrap_or_else(|error| panic!("{error:?}"));
    let empty = || {
        Some(jinnd_api::Epoch {
            dependencies: Vec::new(),
        })
    };
    let loading = |_: FiberId| Some(FiberState::Loading);
    assert_eq!(compute(&broker, loading, &gate, empty()), None, "missing");
    assert_eq!(gate.unmet(), ["jinn:x"]);
    let provider = broker.register_peer(Some(FiberId(4)));
    broker.grant(provider, "jinn:x");
    broker
        .provide(provider, "jinn:x", Arc::new(Echo))
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        compute(&broker, loading, &gate, empty()),
        None,
        "provided, not Active"
    );
    assert_eq!(gate.unmet(), ["jinn:x"]);
    let active = |_: FiberId| Some(FiberState::Active);
    let epoch = compute(&broker, active, &gate, empty()).unwrap_or_else(|| panic!("ready"));
    assert_eq!(epoch.dependencies.len(), 2);
    assert_eq!(
        epoch.dependencies[0].provider,
        FiberId(0),
        "kernel provider"
    );
    assert_eq!(epoch.dependencies[1].provider, FiberId(4));
    assert_eq!(epoch.dependencies[1].generation.0, 1);
    assert!(gate.unmet().is_empty());
    // The provider is replaced: a new generation is a new identity.
    broker.withdraw(provider, "jinn:x");
    assert_eq!(compute(&broker, active, &gate, empty()), None, "withdrawn");
    let successor = broker.register_peer(Some(FiberId(5)));
    broker.grant(successor, "jinn:x");
    broker
        .provide(successor, "jinn:x", Arc::new(Echo))
        .unwrap_or_else(|error| panic!("{error:?}"));
    let moved = compute(&broker, active, &gate, empty()).unwrap_or_else(|| panic!("ready"));
    assert_ne!(moved, epoch, "a replaced provider changes the epoch");
    assert_eq!(moved.dependencies[1].generation.0, 2);
    // The loader's own signal still gates.
    assert_eq!(compute(&broker, active, &gate, None), None);
}
