//! Atomic profile persistence: write-temp + fsync + rename, so the document on
//! disk is always whole (LAW §3 bidirectional persistence; v0.1 constitution
//! bounds: local-only profiles, no destructive compaction — saves replace, they
//! never rewrite history).

use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use jinnd_api::{ErrorCode, KernelError, Profile};

use crate::document::Document;
use crate::loader::{LaneConfig, Loader};
use crate::state::{error, lock};

static TEMP_SERIAL: AtomicU64 = AtomicU64::new(0);

/// A document persisted at one local path.
#[derive(Clone, Debug)]
pub struct FileStore {
    path: PathBuf,
}

impl FileStore {
    /// A store over `path`. Nothing is touched until the first save.
    #[must_use]
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Loads the persisted document, `None` when nothing was ever saved.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] when the file exists but cannot be read or
    /// parsed.
    pub async fn load(&self) -> Result<Option<Document>, KernelError> {
        match tokio::fs::read_to_string(&self.path).await {
            Ok(text) => Document::parse(&text).map(Some),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(fault(format!(
                "the profile document is unreadable: {error}"
            ))),
        }
    }

    /// Saves the document atomically: a unique sibling temporary is written and
    /// fsynced, then renamed over the destination, so no reader ever observes a
    /// partial document.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::InvalidProfile`] when writing fails; the temporary is
    /// removed on any failed step.
    pub async fn save(&self, document: &Document) -> Result<(), KernelError> {
        let temp = self.path.with_extension(format!(
            "{}-{}.tmp",
            std::process::id(),
            TEMP_SERIAL.fetch_add(1, Ordering::Relaxed)
        ));
        let outcome = self.replace_via(&temp, document).await;
        if outcome.is_err() {
            let _ = tokio::fs::remove_file(&temp).await;
        }
        outcome
    }

    async fn replace_via(&self, temp: &PathBuf, document: &Document) -> Result<(), KernelError> {
        let text = document.render();
        let file_error = |error: std::io::Error| fault(format!("write-back failed: {error}"));
        {
            let mut file = tokio::fs::File::create(temp).await.map_err(file_error)?;
            tokio::io::AsyncWriteExt::write_all(&mut file, text.as_bytes())
                .await
                .map_err(file_error)?;
            // The rename may only land contents that are durably on disk.
            file.sync_all().await.map_err(file_error)?;
        }
        tokio::fs::rename(temp, &self.path)
            .await
            .map_err(file_error)?;
        // Making the rename itself durable is best-effort: the atomicity
        // guarantee (whole document or previous document) holds regardless.
        if let Some(directory) = self.path.parent() {
            if let Ok(directory) = std::fs::File::open(directory) {
                let _ = directory.sync_all();
            }
        }
        Ok(())
    }
}

fn fault(message: String) -> KernelError {
    KernelError {
        code: ErrorCode::InvalidProfile,
        message,
        fiber: None,
    }
}

/// Renders one type-erased committed profile over the baseline document.
type Encode = Box<
    dyn Fn(&(dyn Any + Send + Sync), &Document) -> Result<Document, KernelError> + Send + Sync,
>;

/// The attached store, the kernel-owned encoder, and the committed document —
/// the persistence unit (M1-P6c): raw entries and unknown fields live here and
/// survive every write-back.
pub(crate) struct Persistence {
    store: FileStore,
    encode: Encode,
    baseline: std::sync::Mutex<Document>,
}

impl Loader {
    /// Attaches the persistence store (LAW §3): every commit of the document
    /// of record — reconcile, update, dispose — writes back atomically
    /// through `store`. A document-led reconcile persists before the runtime
    /// converges on the document; a runtime-led amendment persists after the
    /// runtime accepted the change (see `amend`).
    ///
    /// `baseline` is the loaded document being taken over (or
    /// [`Document::default`] for a fresh store): its raw entries and unknown
    /// fields are carried through every save verbatim (M1-P6c). Encoding is
    /// kernel-owned — [`Document::merge_profile`] under `C`'s mechanical
    /// `Serialize` — so no caller-supplied code ever runs inside the persist
    /// permit's span (R1, PLA-270). Re-attaching replaces the previous store.
    pub fn attach_store<C: LaneConfig + serde::Serialize>(
        &self,
        store: FileStore,
        baseline: Document,
    ) {
        let encode: Encode =
            Box::new(|committed: &(dyn Any + Send + Sync), baseline: &Document| {
                // A committed profile of another config type is an honest
                // failure, never a silent skip: the disk may not drift.
                let Some(profile) = committed.downcast_ref::<Profile<C>>() else {
                    return Err(error(
                        ErrorCode::InvalidProfile,
                        "the attached store encodes a different config type",
                    ));
                };
                Document::merge_profile(profile, baseline)
            });
        *lock(&self.persist) = Some(Arc::new(Persistence {
            store,
            encode,
            baseline: std::sync::Mutex::new(baseline),
        }));
    }

    /// Persists one committed document through the attached store, if any.
    /// Called before the commit lands, under the loader gate alone — never
    /// with the state lock held across the write, and with no caller-supplied
    /// code anywhere in the span (R1): the merge is kernel-owned. A saved
    /// document becomes the next save's baseline, so preserved raw entries
    /// and unknown fields survive every consecutive write-back.
    pub(crate) async fn persist(
        &self,
        committed: &Arc<dyn Any + Send + Sync>,
    ) -> Result<(), KernelError> {
        let Some(persistence) = lock(&self.persist).clone() else {
            return Ok(());
        };
        let document = {
            let baseline = lock(&persistence.baseline);
            (persistence.encode)(committed.as_ref(), &baseline)?
        };
        persistence.store.save(&document).await?;
        *lock(&persistence.baseline) = document;
        Ok(())
    }
}
