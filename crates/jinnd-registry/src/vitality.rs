//! One provider's reported liveness (§3: a consumer activates only when every
//! injected provider is Active and passes its check).
//!
//! The registry does not own fiber semantics (R10). The layer supervising a
//! provider owns the predicate "Active and passing its check" and reports the
//! resulting bit through a [`Vitality`] handle; availability consumes it.
//! Resolution and leasing deliberately ignore the bit: a dependent is entitled
//! to call a dying provider during its own teardown (I2), and withdrawal — the
//! slot leaving — is the provision undo's business, never vitality's.

#[cfg(not(feature = "loom"))]
use std::sync::Arc;
#[cfg(not(feature = "loom"))]
use tokio::sync::watch;

use crate::sync::Mutex;

/// The decision cell behind [`Vitality`]: the last reported bit, behind the
/// loom shim like every shared cell in this crate.
#[derive(Debug)]
pub(crate) struct VitalityCell {
    reported: Mutex<bool>,
}

impl VitalityCell {
    pub(crate) fn new(initially: bool) -> Self {
        Self {
            reported: Mutex::new(initially),
        }
    }

    pub(crate) fn report(&self, active: bool) {
        *self
            .reported
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = active;
    }

    pub(crate) fn active(&self) -> bool {
        *self
            .reported
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

/// A provider's vitality as its supervisor reports it, wired to the store's
/// change edge so every report wakes availability (R1: edge-driven, never
/// polled).
///
/// Cloning shares the one cell. Provisions made with this handle become
/// available only while the last report was `true`; the kernel's own
/// pseudo-fiber uses a handle that is never reported away from `true`.
#[cfg(not(feature = "loom"))]
#[derive(Clone, Debug)]
pub struct Vitality {
    cell: Arc<VitalityCell>,
    edge: watch::Sender<u64>,
}

#[cfg(not(feature = "loom"))]
impl Vitality {
    pub(crate) fn new(initially: bool, edge: watch::Sender<u64>) -> Self {
        Self {
            cell: Arc::new(VitalityCell::new(initially)),
            edge,
        }
    }

    /// Reports whether the provider is Active and passing its check.
    pub fn report(&self, active: bool) {
        self.cell.report(active);
        self.edge
            .send_modify(|version| *version = version.wrapping_add(1));
    }

    pub(crate) fn cell(&self) -> Arc<VitalityCell> {
        Arc::clone(&self.cell)
    }
}
