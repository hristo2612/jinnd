//! Atomic bidirectional persistence: write-temp + rename, no partial states on
//! disk (LAW §3, v0.1 constitution bounds).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

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

/// Repo convention: no `unwrap`/`expect`. `grab` is the tests' one panicking
/// accessor, carrying the caller's location.
pub trait Grab<T> {
    fn grab(self) -> T;
}

impl<T, E: std::fmt::Debug> Grab<T> for Result<T, E> {
    #[track_caller]
    fn grab(self) -> T {
        match self {
            Ok(value) => value,
            Err(error) => panic!("unexpected error: {error:?}"),
        }
    }
}

impl<T> Grab<T> for Option<T> {
    #[track_caller]
    fn grab(self) -> T {
        match self {
            Some(value) => value,
            None => panic!("unexpectedly empty"),
        }
    }
}
