//! Facade-level ledger, revert, providing-lane, and raw-document wiring
//! (M1-P7): the surfaces the verifier's body session converts IOUs against.

use std::sync::{Arc, Mutex};

use jinnd_api::{
    Activation, EntryId, ErrorCode, Inject, Kernel, KernelError, KernelFuture, LedgerEventKind,
    LedgerQuery, PluginContract, PluginRef, Profile, ProfileEntry, RevertKey, RevertResolution,
    ServiceContract, ServiceHandle, ServiceResolver, ServiceType, Undo, Witness,
};

struct MarkUndo(Arc<Mutex<Vec<u32>>>, u32);

impl Undo for MarkUndo {
    fn undo(self: Box<Self>) -> KernelFuture<'static, ()> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push(self.1);
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn revert_is_keyed_exactly_once_with_witness_gating() {
    let kernel = jinnd_adapter::kernel();
    let log = Arc::new(Mutex::new(Vec::new()));
    let effect = kernel
        .register_effect(
            kernel.root_context(),
            "revertible".to_owned(),
            Box::new(MarkUndo(Arc::clone(&log), 1)),
        )
        .unwrap_or_else(|error| panic!("register: {error:?}"));

    let witness: Witness = Arc::new(|| true);
    let state = kernel
        .revert_effect(effect, RevertKey("k".to_owned()), witness.clone())
        .await
        .unwrap_or_else(|error| panic!("revert: {error:?}"));
    assert_eq!(state, RevertResolution::Reverted);

    let retry = kernel
        .revert_effect(effect, RevertKey("k".to_owned()), witness.clone())
        .await
        .unwrap_or_else(|error| panic!("retry: {error:?}"));
    assert_eq!(
        retry,
        RevertResolution::Reverted,
        "same-key retry is idempotent"
    );
    assert_eq!(
        log.lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .len(),
        1,
        "the inverse ran exactly once"
    );

    assert!(
        kernel
            .revert_effect(effect, RevertKey("other".to_owned()), witness)
            .await
            .is_err(),
        "a distinct key against the branch is refused"
    );
    assert_eq!(
        kernel.revert_resolution(effect),
        Some(RevertResolution::Reverted)
    );

    let records = kernel
        .ledger_events(LedgerQuery::default())
        .await
        .unwrap_or_else(|error| panic!("ledger: {error:?}"));
    let mut protocol = records.iter().filter_map(|record| match &record.kind {
        LedgerEventKind::RevertIntent { .. } => Some("intent"),
        LedgerEventKind::RevertCompleted { .. } => Some("completed"),
        LedgerEventKind::RevertResolved { .. } => Some("resolved"),
        _ => None,
    });
    assert_eq!(protocol.next(), Some("intent"), "intent lands first");
    assert_eq!(protocol.next(), Some("completed"));
    assert_eq!(protocol.next(), Some("resolved"));
}

#[tokio::test]
async fn a_failing_witness_stays_pending_and_compensation_is_never_reverted() {
    let kernel = jinnd_adapter::kernel();
    let log = Arc::new(Mutex::new(Vec::new()));
    let effect = kernel
        .register_effect(
            kernel.root_context(),
            "unwitnessed".to_owned(),
            Box::new(MarkUndo(Arc::clone(&log), 1)),
        )
        .unwrap_or_else(|error| panic!("register: {error:?}"));

    let witness: Witness = Arc::new(|| false);
    let state = kernel
        .revert_effect(effect, RevertKey("k".to_owned()), witness)
        .await
        .unwrap_or_else(|error| panic!("revert: {error:?}"));
    assert_eq!(state, RevertResolution::PendingRevert);
    assert_eq!(
        kernel.revert_resolution(effect),
        Some(RevertResolution::PendingRevert),
        "the branch stays pending-revert, visibly"
    );

    assert!(
        kernel
            .compensate_effect(
                effect,
                RevertKey("comp".to_owned()),
                Box::new(MarkUndo(Arc::clone(&log), 2)),
                false,
            )
            .await
            .is_err(),
        "compensation requires operator confirmation"
    );
    let state = kernel
        .compensate_effect(
            effect,
            RevertKey("comp".to_owned()),
            Box::new(MarkUndo(Arc::clone(&log), 2)),
            true,
        )
        .await
        .unwrap_or_else(|error| panic!("compensate: {error:?}"));
    assert_eq!(
        state,
        RevertResolution::Compensated { clean: false },
        "the original witness still fails, so the branch stays marked unclean"
    );
    assert_ne!(state, RevertResolution::Reverted);
}

#[derive(Debug)]
struct LinkA;
#[derive(Debug)]
struct LinkB;

impl ServiceContract for LinkA {
    type Observation = ();
    const NAME: &'static str = "jinn.test/link-a";
    fn observe(&self) {}
}

impl ServiceContract for LinkB {
    type Observation = ();
    const NAME: &'static str = "jinn.test/link-b";
    fn observe(&self) {}
}

macro_rules! needs {
    ($name:ident, $service:ident) => {
        #[derive(Debug)]
        struct $name {
            _handle: ServiceHandle<$service>,
        }

        impl Inject for $name {
            fn declare() -> Vec<ServiceType> {
                vec![ServiceType::of::<$service>()]
            }

            fn inject<R: ServiceResolver + ?Sized>(resolver: &R) -> Result<Self, KernelError> {
                Ok(Self {
                    _handle: resolver.resolve::<$service>()?,
                })
            }
        }
    };
}

needs!(NeedsB, LinkB);
needs!(NeedsA, LinkA);

macro_rules! plugin {
    ($name:ident, $deps:ident, $label:literal) => {
        #[derive(Debug)]
        struct $name;

        impl PluginContract for $name {
            type Config = u8;
            type Dependencies = $deps;

            const NAME: &'static str = $label;

            fn activate<'a>(
                &'a self,
                _activation: Activation<'a, $deps>,
                _config: u8,
            ) -> KernelFuture<'a, ()> {
                Box::pin(async { Ok(()) })
            }
        }
    };
}

plugin!(PluginA, NeedsB, "jinn.test/cycle-a");
plugin!(PluginB, NeedsA, "jinn.test/cycle-b");

fn entry(id: &str, package: &str) -> ProfileEntry<u8> {
    ProfileEntry {
        id: EntryId(id.to_owned()),
        plugin: PluginRef {
            package: package.to_owned(),
            version: "1".to_owned(),
            artifact_hash: String::new(),
        },
        config: 0,
        disabled: false,
        parent: None,
        isolation: Vec::new(),
    }
}

#[tokio::test]
async fn a_providing_lane_cycle_is_detected_and_attributed_in_the_ledger() {
    let kernel = jinnd_adapter::kernel();
    kernel
        .register_providing_package("jinn.test/provides-a", |config: u8| {
            Ok((PluginA, config, Arc::new(LinkA)))
        })
        .unwrap_or_else(|error| panic!("lane a: {error:?}"));
    kernel
        .register_providing_package("jinn.test/provides-b", |config: u8| {
            Ok((PluginB, config, Arc::new(LinkB)))
        })
        .unwrap_or_else(|error| panic!("lane b: {error:?}"));

    let report = kernel
        .reconcile(Profile {
            entries: vec![
                entry("a", "jinn.test/provides-a"),
                entry("b", "jinn.test/provides-b"),
            ],
        })
        .await
        .unwrap_or_else(|error| panic!("reconcile: {error:?}"));
    let cycle_faults: Vec<&EntryId> = report
        .errors
        .iter()
        .filter(|fault| fault.error.code == ErrorCode::DependencyCycle)
        .map(|fault| &fault.entry)
        .collect();
    assert_eq!(cycle_faults.len(), 2, "both cycle members fault statically");

    let attributed = kernel
        .ledger_events(LedgerQuery {
            entry: Some(EntryId("a".to_owned())),
            ..LedgerQuery::default()
        })
        .await
        .unwrap_or_else(|error| panic!("ledger: {error:?}"));
    assert!(
        attributed.iter().any(|record| matches!(
            &record.kind,
            LedgerEventKind::ErrorRecorded { error } if error.code == ErrorCode::DependencyCycle
        )),
        "the cycle diagnostic is reachable from the entry's ledger events"
    );
}
