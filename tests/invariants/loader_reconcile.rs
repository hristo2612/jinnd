mod loader_cases;
mod loader_fixture;
mod support;

use jinnd_api::Kernel;
use support::{expect_ok, facade_gap_at, spec_case};

const SUBSYSTEM: support::Subsystem = support::Subsystem::Loader;
const FACADE_GAP_REASON: &str =
    "the facade has no loader-as-service dependency or loader await-intercept surface";

spec_case! {
    /// TS origin: `packages/loader/tests/index.spec.ts`, test `loader initiate`.
    initial_profile_activates_only_enabled_entries,
    origin: "packages/loader/tests/index.spec.ts",
    test: "loader initiate",
    setup: ["profile contains enabled foo, enabled nested bar, and disabled nested qux"],
    actions: ["reconcile from an empty runtime"],
    expected: ["foo and bar activate once", "qux remains inactive", "entry ids are retained"],
    body: |_case| { loader_cases::reconcile::initial_profile().await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/index.spec.ts`, `plugin self-update`; C9 extends it to opaque document data.
    raw_entries_and_unknown_fields_survive_runtime_write_back,
    origin: "packages/loader/tests/index.spec.ts",
    test: "plugin self-update preserves raw entries and unknown fields",
    setup: ["persisted document contains one decodable entry with an unknown field and one raw future-version entry"],
    actions: ["reconcile the known entry", "perform a runtime config write-back"],
    expected: ["known config changes atomically", "unknown field and raw entry round-trip byte-for-byte"],
    body: |case| {
        let kernel = jinnd_adapter::kernel();
        let log = loader_fixture::log();
        loader_fixture::register(&kernel, &log);
        loader_fixture::reconcile(
            &kernel,
            vec![loader_fixture::entry("known", loader_fixture::COUNT, 1)],
        )
        .await;
        expect_ok(
            kernel
                .update_entry(&loader_fixture::id("known"), loader_fixture::Config {
                    entry: "known".to_owned(),
                    value: 2,
                })
                .await,
            "the typed write-back lane should update",
        );
        let persisted = kernel
            .persisted_profile::<loader_fixture::Config>()
            .unwrap_or_else(|| panic!("the typed profile should remain observable"));
        assert_eq!(persisted.entries[0].config.value, 2);

        facade_gap_at(
            &case,
            "the facade has no raw Document attach/read surface, so unknown entry fields and opaque entries cannot be supplied or observed",
        );
    }
}

spec_case! {
    /// TS origin: `packages/loader/tests/index.spec.ts`, test `loader update`.
    reconcile_by_id_preserves_unchanged_and_swaps_only_affected_entries,
    origin: "packages/loader/tests/index.spec.ts",
    test: "loader update",
    setup: ["active profile has foo and nested bar while qux is disabled"],
    actions: ["reconcile final profile containing unchanged foo and enabled qux but no bar"],
    expected: ["foo does not restart", "bar disposes", "qux activates exactly once"],
    body: |_case| { loader_cases::reconcile::reconcile_by_id().await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/index.spec.ts`, test `plugin self-update`.
    runtime_config_update_writes_back_to_matching_profile_entry,
    origin: "packages/loader/tests/index.spec.ts",
    test: "plugin self-update",
    setup: ["profile has foo id=1 and qux id=4"],
    actions: ["fiber id=1 updates config to a=3", "wait for atomic write-back"],
    expected: ["persisted entry id=1 has config a=3", "sibling id=4 is unchanged"],
    body: |_case| { loader_cases::reconcile::runtime_update().await; }
}

spec_case! {
    /// TS origin: `packages/loader/tests/index.spec.ts`, test `plugin self-dispose`.
    runtime_disposal_writes_disabled_state_back_to_profile,
    origin: "packages/loader/tests/index.spec.ts",
    test: "plugin self-dispose",
    setup: ["profile entry id=1 is active with config a=3"],
    actions: ["fiber id=1 disposes itself", "wait for atomic write-back"],
    expected: ["persisted id=1 is disabled and retains config a=3", "siblings are unchanged"],
    body: |_case| { loader_cases::reconcile::runtime_disposal().await; }
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
