//! What `jinn:profile-admin`'s checks read from the loader before a stage
//! (M2-K23; split from `administer.rs` by responsibility, R10).

use jinnd_api::EntryId;

use crate::loader::{LaneConfig, Loader};
use crate::state::lock;

impl Loader {
    /// Whether a lane is registered for `package` under config type `C`:
    /// an admin write naming a package no reconcile ever admitted refuses
    /// with the Law-5 reason rather than spawning into nothing.
    #[must_use]
    pub fn has_lane<C: LaneConfig>(&self, package: &str) -> bool {
        lock(&self.lanes).contains_key(&(package.to_owned(), std::any::TypeId::of::<C>()))
    }

    /// The document of record as the attached store last rendered it —
    /// byte-for-byte what is on disk — or `None` without a store. A digest
    /// of this is a digest of the file (Law 2: checkable with nothing but
    /// the file).
    #[must_use]
    pub fn rendered_document(&self) -> Option<String> {
        self.persistence().map(|persistence| persistence.rendered())
    }

    /// The ids of entries preserved RAW (undecodable, re-emitted verbatim;
    /// R11): their record cannot be captured as a typed prior.
    #[must_use]
    pub fn raw_entry_ids(&self) -> Vec<EntryId> {
        self.persistence()
            .map(|persistence| persistence.raw_ids())
            .unwrap_or_default()
    }
}
