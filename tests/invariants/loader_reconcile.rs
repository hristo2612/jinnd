mod support;

use support::spec_case;

spec_case! {
    /// TS origin: `packages/loader/tests/index.spec.ts`, test `loader initiate`.
    initial_profile_activates_only_enabled_entries,
    origin: "packages/loader/tests/index.spec.ts",
    test: "loader initiate",
    setup: ["profile contains enabled foo, enabled nested bar, and disabled nested qux"],
    actions: ["reconcile from an empty runtime"],
    expected: ["foo and bar activate once", "qux remains inactive", "entry ids are retained"]
}

spec_case! {
    /// TS origin: `packages/loader/tests/index.spec.ts`, test `loader update`.
    reconcile_by_id_preserves_unchanged_and_swaps_only_affected_entries,
    origin: "packages/loader/tests/index.spec.ts",
    test: "loader update",
    setup: ["active profile has foo and nested bar while qux is disabled"],
    actions: ["reconcile final profile containing unchanged foo and enabled qux but no bar"],
    expected: ["foo does not restart", "bar disposes", "qux activates exactly once"]
}

spec_case! {
    /// TS origin: `packages/loader/tests/index.spec.ts`, test `plugin self-update`.
    runtime_config_update_writes_back_to_matching_profile_entry,
    origin: "packages/loader/tests/index.spec.ts",
    test: "plugin self-update",
    setup: ["profile has foo id=1 and qux id=4"],
    actions: ["fiber id=1 updates config to a=3", "wait for atomic write-back"],
    expected: ["persisted entry id=1 has config a=3", "sibling id=4 is unchanged"]
}

spec_case! {
    /// TS origin: `packages/loader/tests/index.spec.ts`, test `plugin self-dispose`.
    runtime_disposal_writes_disabled_state_back_to_profile,
    origin: "packages/loader/tests/index.spec.ts",
    test: "plugin self-dispose",
    setup: ["profile entry id=1 is active with config a=3"],
    actions: ["fiber id=1 disposes itself", "wait for atomic write-back"],
    expected: ["persisted id=1 is disabled and retains config a=3", "siblings are unchanged"]
}

spec_case! {
    /// TS origin: `packages/loader/tests/index.spec.ts`, test `pending`.
    loader_await_intercept_blocks_entry_behind_loading_dependency,
    origin: "packages/loader/tests/index.spec.ts",
    test: "pending",
    setup: ["foo remains loading", "bar waits forever on missing dependency", "qux injects loader with await intercept"],
    actions: ["create all entries and settle runnable tasks"],
    expected: ["foo is loading", "bar is pending", "qux is pending behind loader await"]
}

spec_case! {
    /// TS origin: `packages/loader/tests/index.spec.ts`, test `resolved`.
    loader_await_intercept_releases_when_tracked_loading_entry_settles,
    origin: "packages/loader/tests/index.spec.ts",
    test: "resolved",
    setup: ["foo is loading", "bar is pending on missing dependency", "qux awaits loader"],
    actions: ["complete foo initialization", "wait for quiescence"],
    expected: ["foo and qux are active", "bar remains cleanly pending"]
}
