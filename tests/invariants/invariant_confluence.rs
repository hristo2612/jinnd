use jinnd_api::{
    Activation, EntryId, ErrorCode, FiberState, Kernel, KernelError, KernelFuture, PluginContract,
    PluginRef, Profile, ProfileEntry, Undo,
};
use proptest::prelude::*;

#[derive(Clone, Debug)]
enum HistoryOp {
    Load { entry: u8 },
    Unload { entry: u8 },
    Crash { entry: u8 },
    HotSwap { entry: u8, generation: u8 },
    ConfigEdit { entry: u8, value: u8 },
}

fn history_op() -> impl Strategy<Value = HistoryOp> {
    (0_u8..4, 0_u8..8).prop_flat_map(|(entry, value)| {
        prop_oneof![
            Just(HistoryOp::Load { entry }),
            Just(HistoryOp::Unload { entry }),
            Just(HistoryOp::Crash { entry }),
            Just(HistoryOp::HotSwap {
                entry,
                generation: value,
            }),
            Just(HistoryOp::ConfigEdit { entry, value }),
        ]
    })
}

struct Noop;

impl Undo for Noop {
    fn undo(self: Box<Self>) -> KernelFuture<'static, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Debug)]
struct ConfluencePlugin;

impl PluginContract for ConfluencePlugin {
    type Config = u8;
    type Dependencies = ();

    const NAME: &'static str = "jinn.test/confluence";

    fn activate<'a>(&'a self, activation: Activation<'a, ()>, config: u8) -> KernelFuture<'a, ()> {
        Box::pin(async move {
            activation
                .effects
                .register(format!("config {config}"), Box::new(Noop))?;
            if config == u8::MAX {
                return Err(KernelError {
                    code: ErrorCode::PluginFailed,
                    message: "history crash".to_owned(),
                    fiber: None,
                });
            }
            Ok(())
        })
    }
}

fn profile(state: &[Option<u8>; 4]) -> Profile<u8> {
    Profile {
        entries: state
            .iter()
            .enumerate()
            .filter_map(|(index, value)| {
                value.map(|config| ProfileEntry {
                    id: EntryId(format!("entry-{index}")),
                    plugin: PluginRef {
                        package: ConfluencePlugin::NAME.to_owned(),
                        version: "1".to_owned(),
                        artifact_hash: String::new(),
                    },
                    config,
                    disabled: false,
                    parent: None,
                    isolation: Vec::new(),
                })
            })
            .collect(),
    }
}

fn apply(state: &mut [Option<u8>; 4], operation: &HistoryOp) {
    let (entry, value) = match operation {
        HistoryOp::Load { entry } => (*entry, Some(0)),
        HistoryOp::Unload { entry } => (*entry, None),
        HistoryOp::Crash { entry } => (*entry, Some(u8::MAX)),
        HistoryOp::HotSwap { entry, generation } => (*entry, Some(*generation)),
        HistoryOp::ConfigEdit { entry, value } => (*entry, Some(*value)),
    };
    state[usize::from(entry)] = value;
}

fn snapshot(
    kernel: &impl Kernel,
    state: &[Option<u8>; 4],
) -> Vec<(usize, FiberState, Vec<String>)> {
    state
        .iter()
        .enumerate()
        .filter_map(|(index, value)| {
            value.map(|_| {
                let fiber = kernel
                    .entry_fiber(&EntryId(format!("entry-{index}")))
                    .unwrap_or_else(|| panic!("live entry {index} should have a fiber"));
                let labels = kernel
                    .effect_tree(fiber)
                    .into_iter()
                    .map(|effect| effect.label)
                    .collect();
                (index, kernel.state(fiber), labels)
            })
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 32,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    /// Paper origin: confluence theorem; SOURCE-OF-TRUTH §4 invariant I4.
    #[test]
    fn randomized_history_is_observationally_equal_to_fresh_final_boot(
        history in prop::collection::vec(history_op(), 1..65),
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap_or_else(|error| panic!("invariant runtime: {error}"));
        runtime.block_on(async {
            let historical = jinnd_adapter::kernel();
            historical
                .register_package(ConfluencePlugin::NAME, |config: u8| Ok((ConfluencePlugin, config)))
                .unwrap_or_else(|error| panic!("historical lane: {error:?}"));
            let mut final_state = [None, None, None, None];
            for operation in &history {
                apply(&mut final_state, operation);
                historical
                    .reconcile(profile(&final_state))
                    .await
                    .unwrap_or_else(|error| panic!("history reconcile: {error:?}"));
                historical
                    .wait_for_quiescence()
                    .await
                    .unwrap_or_else(|error| panic!("history quiescence: {error:?}"));
            }

            let fresh = jinnd_adapter::kernel();
            fresh
                .register_package(ConfluencePlugin::NAME, |config: u8| Ok((ConfluencePlugin, config)))
                .unwrap_or_else(|error| panic!("fresh lane: {error:?}"));
            fresh
                .reconcile(profile(&final_state))
                .await
                .unwrap_or_else(|error| panic!("fresh reconcile: {error:?}"));
            fresh
                .wait_for_quiescence()
                .await
                .unwrap_or_else(|error| panic!("fresh quiescence: {error:?}"));

            prop_assert_eq!(snapshot(&historical, &final_state), snapshot(&fresh, &final_state));
            Ok(())
        })?;
    }
}
