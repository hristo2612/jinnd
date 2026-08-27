mod loader_cases;
mod loader_fixture;
mod support;

use jinnd_api::{
    Activation, EntryId, Kernel, KernelFuture, PluginContract, PluginRef, Profile, ProfileEntry,
};
use support::{expect_ok, spec_case};

const SUBSYSTEM: support::Subsystem = support::Subsystem::Loader;
const V02_DEFERRED_BOUND: &str = "constitution 04 makes reconciliation kernel-owned and dependency readiness automatic in v0.1; no loader-as-service or await-intercept capability contract exists";

struct Cleanup(std::path::PathBuf);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Debug)]
struct RawDocumentPlugin;

impl PluginContract for RawDocumentPlugin {
    type Config = u32;
    type Dependencies = ();

    const NAME: &'static str = "jinn.test/raw-document";

    fn activate<'a>(
        &'a self,
        _activation: Activation<'a, ()>,
        _config: u32,
    ) -> KernelFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

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
    body: |_case| {
        let kernel = jinnd_adapter::kernel();
        expect_ok(
            kernel.register_package("jinn.test/raw-document", |config: u32| {
                Ok((RawDocumentPlugin, config))
            }),
            "raw-document package should register",
        );
        let baseline = r#"{"entries":[{"id":"known","package":"jinn.test/raw-document","config":1,"note":{"spacing":"keep"}},{"id":"future","package":42,"payload":{"raw":[1,2,3]}}]}"#;
        let directory = std::env::temp_dir().join(format!(
            "jinnd-invariant-raw-document-{}",
            std::process::id(),
        ));
        std::fs::create_dir_all(&directory)
            .unwrap_or_else(|error| panic!("temporary directory should create: {error}"));
        let cleanup = Cleanup(directory.clone());
        let path = directory.join("profile.json");
        expect_ok(
            kernel.attach_document::<u32>(path, baseline),
            "raw document should attach",
        );
        expect_ok(
            kernel
                .reconcile(Profile {
                    entries: vec![ProfileEntry {
                        id: EntryId("known".to_owned()),
                        plugin: PluginRef {
                            package: "jinn.test/raw-document".to_owned(),
                            version: "1".to_owned(),
                            artifact_hash: String::new(),
                        },
                        config: 1u32,
                        disabled: false,
                        parent: None,
                        isolation: Vec::new(),
                    }],
                })
                .await,
            "known entry should reconcile",
        );
        expect_ok(
            kernel.update_entry(&EntryId("known".to_owned()), 2u32).await,
            "runtime write-back should update",
        );
        let persisted = kernel
            .document_text()
            .unwrap_or_else(|| panic!("persisted raw document should be observable"));
        assert!(persisted.contains(r#""config": 2"#));
        assert!(
            persisted.contains(r#""note":{"spacing":"keep"}"#),
            "the unknown known-entry field must round-trip byte-for-byte",
        );
        assert!(
            persisted.contains(r#"{"id":"future","package":42,"payload":{"raw":[1,2,3]}}"#),
            "the opaque future entry must round-trip byte-for-byte",
        );
        drop(cleanup);
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
