//! Raw-document attach/read wiring (M1-P7, the loader_reconcile IOU): opaque
//! entries and unknown fields survive a runtime write-back byte-for-byte.

use jinnd_api::{
    Activation, EntryId, Kernel, KernelFuture, PluginContract, PluginRef, Profile, ProfileEntry,
};

#[derive(Debug)]
struct PlainPlugin;

impl PluginContract for PlainPlugin {
    type Config = u8;
    type Dependencies = ();

    const NAME: &'static str = "jinn.test/doc-plain";

    fn activate<'a>(
        &'a self,
        _activation: Activation<'a, ()>,
        _config: u8,
    ) -> KernelFuture<'a, ()> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn raw_entries_and_unknown_fields_survive_a_runtime_write_back() {
    let kernel = jinnd_adapter::kernel();
    kernel
        .register_package("jinn.test/doc-plain", |config: u8| {
            Ok((PlainPlugin, config))
        })
        .unwrap_or_else(|error| panic!("lane: {error:?}"));
    // An opaque future-version entry (package is not a string) and an unknown
    // per-entry field, both owed byte-for-byte survival (v0.1 bounds).
    let baseline = r#"{"entries":[
        {"id":"known","package":"jinn.test/doc-plain","config":1,"note":"keep-me"},
        {"id":"future","package":42,"payload":"opaque"}
    ]}"#;
    let dir = std::env::temp_dir().join(format!("jinnd-adapter-doc-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap_or_else(|error| panic!("mkdir: {error}"));
    let path = dir.join("profile.json");
    kernel
        .attach_document::<u8>(path, baseline)
        .unwrap_or_else(|error| panic!("attach: {error:?}"));

    kernel
        .reconcile(Profile {
            entries: vec![ProfileEntry {
                id: EntryId("known".to_owned()),
                plugin: PluginRef {
                    package: "jinn.test/doc-plain".to_owned(),
                    version: "1".to_owned(),
                    artifact_hash: String::new(),
                },
                config: 0u8,
                disabled: false,
                parent: None,
                isolation: Vec::new(),
            }],
        })
        .await
        .unwrap_or_else(|error| panic!("reconcile: {error:?}"));
    kernel
        .update_entry(&EntryId("known".to_owned()), 2u8)
        .await
        .unwrap_or_else(|error| panic!("update: {error:?}"));

    let text = kernel
        .document_text()
        .unwrap_or_else(|| panic!("the persisted document must be readable"));
    assert!(text.contains("keep-me"), "unknown fields round-trip");
    assert!(text.contains("42"), "raw entries round-trip");
    std::fs::remove_dir_all(&dir).unwrap_or_else(|error| panic!("cleanup: {error}"));
}
