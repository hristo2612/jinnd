//! Atomic bidirectional persistence: write-temp + rename, no partial states on
//! disk (LAW §3, v0.1 constitution bounds).

mod common;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use common::{Grab, entry, fixture, id, profile};
use jinnd_api::{ErrorCode, Profile};
use jinnd_loader::{Document, DocumentEntry, FileStore};

static SCRATCH: AtomicU64 = AtomicU64::new(0);

/// A unique scratch file inside its own fresh directory, so every observation
/// (leftover scans included) is scoped to this test alone; no extra dependency.
fn scratch_path() -> PathBuf {
    let unique = format!(
        "jinnd-loader-store-{}-{}",
        std::process::id(),
        SCRATCH.fetch_add(1, Ordering::Relaxed)
    );
    let directory = std::env::temp_dir().join(unique);
    std::fs::create_dir_all(&directory).grab();
    directory.join("profile.json")
}

fn document(marker: u64) -> Document {
    Document {
        raw: Vec::new(),
        entries: vec![DocumentEntry {
            id: "foo".to_owned(),
            package: "test/foo".to_owned(),
            version: String::new(),
            hash: String::new(),
            config: serde_json::json!({ "marker": marker }),
            disabled: false,
            parent: None,
            isolate: Default::default(),
        }],
    }
}

#[tokio::test]
async fn load_of_a_missing_document_is_none() {
    let store = FileStore::new(scratch_path());
    assert!(store.load().await.grab().is_none());
}

#[tokio::test]
async fn save_then_load_round_trips_the_document() {
    let path = scratch_path();
    let store = FileStore::new(path.clone());
    store.save(&document(7)).await.grab();
    let loaded = store.load().await.grab().grab();
    assert_eq!(loaded, document(7));
    let _ = std::fs::remove_dir_all(path.parent().grab());
}

#[tokio::test]
async fn save_replaces_whole_documents_and_leaves_no_temporary_behind() {
    let path = scratch_path();
    let store = FileStore::new(path.clone());
    for marker in 0..20 {
        store.save(&document(marker)).await.grab();
        // Every observable on-disk state is a complete, parseable document.
        let text = std::fs::read_to_string(&path).grab();
        let loaded = Document::parse(&text).grab();
        assert_eq!(loaded, document(marker));
    }
    let directory = path.parent().grab();
    let leftovers: Vec<_> = std::fs::read_dir(directory)
        .grab()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".tmp"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "temporaries left behind: {leftovers:?}"
    );
    let _ = std::fs::remove_dir_all(directory);
}

#[tokio::test]
async fn concurrent_reader_never_observes_a_partial_document() {
    let path = scratch_path();
    let store = FileStore::new(path.clone());
    store.save(&document(0)).await.grab();

    let reader_path = path.clone();
    let reader = std::thread::spawn(move || {
        for _ in 0..400 {
            let text = std::fs::read_to_string(&reader_path).grab();
            let loaded = Document::parse(&text).grab();
            assert_eq!(loaded.entries.len(), 1);
        }
    });
    for marker in 1..100 {
        store.save(&document(marker)).await.grab();
    }
    reader.join().grab();
    let _ = std::fs::remove_dir_all(path.parent().grab());
}

/// Encodes the fixture's `u32` configs into a persistable document.
fn encode(profile: &Profile<u32>) -> Document {
    Document {
        entries: profile
            .entries
            .iter()
            .map(|entry| DocumentEntry {
                id: entry.id.0.clone(),
                package: entry.plugin.package.clone(),
                version: entry.plugin.version.clone(),
                hash: entry.plugin.artifact_hash.clone(),
                config: serde_json::json!(entry.config),
                disabled: entry.disabled,
                parent: entry.parent.as_ref().map(|parent| parent.0.clone()),
                isolate: Default::default(),
            })
            .collect(),
        raw: Vec::new(),
    }
}

fn persisted_entry(document: &Document, name: &str) -> DocumentEntry {
    document
        .entries
        .iter()
        .find(|entry| entry.id == name)
        .cloned()
        .grab()
}

#[tokio::test]
async fn runtime_changes_write_back_through_the_attached_store() {
    let (loader, _registry, _log) = fixture();
    let path = scratch_path();
    loader.attach_store(FileStore::new(path.clone()), encode);

    // Committing a document of record persists it.
    loader
        .reconcile(profile(vec![entry("one", "test/count", 1)]))
        .await
        .grab();
    let on_disk = FileStore::new(path.clone()).load().await.grab().grab();
    assert_eq!(persisted_entry(&on_disk, "one").config, 1);

    // A runtime config change lands on disk before the fiber reloads.
    loader.update_entry(&id("one"), 5u32).await.grab();
    let on_disk = FileStore::new(path.clone()).load().await.grab().grab();
    assert_eq!(persisted_entry(&on_disk, "one").config, 5);

    // A runtime disposal persists the entry as disabled, config retained.
    loader.dispose_entry::<u32>(&id("one")).await.grab();
    let on_disk = FileStore::new(path.clone()).load().await.grab().grab();
    let one = persisted_entry(&on_disk, "one");
    assert!(one.disabled);
    assert_eq!(one.config, 5);

    // Atomic all the way: no temporary litter beside the document.
    let directory = path.parent().grab();
    let leftovers: Vec<_> = std::fs::read_dir(directory)
        .grab()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".tmp"))
        .collect();
    assert!(leftovers.is_empty(), "temporaries left: {leftovers:?}");
    let _ = std::fs::remove_dir_all(directory);
}

#[tokio::test]
async fn a_foreign_typed_store_is_an_honest_error_not_a_silent_skip() {
    let (loader, _registry, _log) = fixture();
    let path = scratch_path();
    // The store encodes `String` profiles; the loader runs on `u32` ones.
    loader.attach_store(FileStore::new(path.clone()), |_: &Profile<String>| {
        Document::default()
    });
    let Err(error) = loader
        .reconcile(profile(vec![entry("one", "test/count", 1)]))
        .await
    else {
        panic!("a foreign-typed store must fail the commit, not skip persistence");
    };
    assert_eq!(error.code, ErrorCode::InvalidProfile);
    // Nothing was committed: no document on disk, no runtime for the entry.
    assert!(FileStore::new(path.clone()).load().await.grab().is_none());
    assert!(loader.entry_fiber(&id("one")).is_none());
    let _ = std::fs::remove_dir_all(path.parent().grab());
}
