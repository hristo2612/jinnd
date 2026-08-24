use proptest::prelude::*;

const NO_KERNEL_REASON: &str =
    "M1-P0 has no kernel adapter: compare incremental quiescent observation with fresh boot";

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

fn touch_fields(history: &[HistoryOp]) -> u64 {
    history.iter().fold(0_u64, |checksum, operation| {
        checksum
            ^ match operation {
                HistoryOp::Load { entry } => u64::from(*entry),
                HistoryOp::Unload { entry } => 16 + u64::from(*entry),
                HistoryOp::Crash { entry } => 32 + u64::from(*entry),
                HistoryOp::HotSwap { entry, generation } => {
                    48 + u64::from(*entry) + u64::from(*generation)
                }
                HistoryOp::ConfigEdit { entry, value } => {
                    64 + u64::from(*entry) + u64::from(*value)
                }
            }
    })
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
        let checksum = touch_fields(&history);
        prop_assert!(
            false,
            "{NO_KERNEL_REASON}; generated_history={history:?}; checksum={checksum}"
        );
    }
}
