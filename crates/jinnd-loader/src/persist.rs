//! Write-back through the attached store: every commit of the document of
//! record moves to disk atomically before the runtime does (LAW §3,
//! bidirectional persistence — the two views never drift).

use std::any::Any;
use std::sync::Arc;

use jinnd_api::{ErrorCode, KernelError, Profile};

use crate::document::Document;
use crate::loader::{LaneConfig, Loader};
use crate::state::{error, lock};
use crate::store::FileStore;

/// Renders one type-erased committed profile into a persistable document.
type Encode = Box<dyn Fn(&(dyn Any + Send + Sync)) -> Result<Document, KernelError> + Send + Sync>;

/// The attached store and its typed encoder.
pub(crate) struct Persistence {
    store: FileStore,
    encode: Encode,
}

impl Loader {
    /// Attaches the persistence store (LAW §3): every commit of the document
    /// of record — reconcile, update, dispose — writes back atomically
    /// through `store` before the runtime moves. `encode` renders the
    /// committed profile; re-attaching replaces the previous store.
    pub fn attach_store<C: LaneConfig>(
        &self,
        store: FileStore,
        encode: impl Fn(&Profile<C>) -> Document + Send + Sync + 'static,
    ) {
        let encode: Encode = Box::new(move |committed: &(dyn Any + Send + Sync)| {
            // A committed profile of another config type is an honest failure,
            // never a silent skip: the document on disk may not drift.
            let Some(profile) = committed.downcast_ref::<Profile<C>>() else {
                return Err(error(
                    ErrorCode::InvalidProfile,
                    "the attached store encodes a different config type",
                ));
            };
            Ok(encode(profile))
        });
        *lock(&self.persist) = Some(Arc::new(Persistence { store, encode }));
    }

    /// Persists one committed document through the attached store, if any.
    /// Called before the commit lands, under the loader gate alone — never
    /// with the state lock held across the write (R1).
    pub(crate) async fn persist(
        &self,
        committed: &Arc<dyn Any + Send + Sync>,
    ) -> Result<(), KernelError> {
        let Some(persistence) = lock(&self.persist).clone() else {
            return Ok(());
        };
        let document = (persistence.encode)(committed.as_ref())?;
        persistence.store.save(&document).await
    }
}
