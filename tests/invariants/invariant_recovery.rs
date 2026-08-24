mod support;

use support::spec_case;

const SUBSYSTEM: support::Subsystem = support::Subsystem::Effects;
const FACADE_GAP_REASON: &str = "the facade cannot execute forward effects, dispose effect ids, or observe sibling state after failed activation";

spec_case! {
    /// Paper origin: recovery exactness theorem; SOURCE-OF-TRUTH §4 invariant I1.
    failed_mid_load_withdraws_exactly_the_partial_contribution,
    origin: "paper: recovery exactness theorem / I1",
    test: "recovery under mid-load failure",
    setup: ["capture observational baseline", "plugin applies two reversible mutations then fails before a third"],
    actions: ["allow failure to settle", "remove failed plugin", "compare all service observations and siblings with baseline"],
    expected: ["both applied inverses run once in LIFO order", "no partial contribution remains", "unrelated state is unchanged"]
}

spec_case! {
    /// Paper origin: recovery exactness theorem; SOURCE-OF-TRUTH §4 invariant I1.
    removal_after_arbitrary_restart_history_withdraws_only_owned_effects,
    origin: "paper: recovery exactness theorem / I1",
    test: "history-sensitive recovery exactness",
    setup: ["two sibling plugins contribute observationally distinct reversible effects", "target plugin has restarted across provider generations"],
    actions: ["remove only the target and wait for quiescence"],
    expected: ["target contribution is absent", "sibling contribution and generation are unchanged", "result equals assembly without target"]
}
