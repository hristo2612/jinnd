//! Atomic profile persistence: write-temp + fsync + rename, so the document on
//! disk is always whole (LAW §3 bidirectional persistence; v0.1 constitution
//! bounds: local-only profiles, no destructive compaction — saves replace, they
//! never rewrite history).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use jinnd_api::{ErrorCode, KernelError};

use crate::document::Document;

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
