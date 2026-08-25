//! The reactive layer over the slot map: one version channel, edge-driven.
//!
//! Every observable change — a slot published, withdrawn, or a lease returned —
//! bumps one `watch` channel. Availability watchers and draining providers wake on
//! that edge and re-read the cells; nothing in this crate polls (R1).

use std::sync::Arc;

use tokio::sync::watch;

use crate::leases::LeaseCell;
use crate::slots::SlotMap;

/// The slot map plus the change edge everything reactive subscribes to.
#[derive(Debug)]
pub(crate) struct Store {
    pub(crate) slots: SlotMap,
    version: watch::Sender<u64>,
}

/// One held lease (I2). Dropping the guard returns the lease and wakes the store,
/// so a provider draining on this generation re-reads the count.
#[derive(Debug)]
pub struct LeaseGuard {
    cell: Arc<LeaseCell>,
    version: watch::Sender<u64>,
}

impl Store {
    pub(crate) fn new() -> Self {
        Self {
            slots: SlotMap::new(),
            version: watch::Sender::new(0),
        }
    }

    /// Wakes every subscriber to re-read the cells.
    pub(crate) fn bump(&self) {
        self.version
            .send_modify(|edge| *edge = edge.wrapping_add(1));
    }

    /// A subscription to the store's change edge.
    pub(crate) fn watch(&self) -> watch::Receiver<u64> {
        self.version.subscribe()
    }

    /// Wraps an acquired lease so its return wakes the store.
    pub(crate) fn guard(&self, cell: Arc<LeaseCell>) -> LeaseGuard {
        LeaseGuard {
            cell,
            version: self.version.clone(),
        }
    }

    /// Resolves once `cell` is drained: closed, with every lease returned (I2).
    ///
    /// Edge-driven (R1): each wake is caused by an actual store change — a lease
    /// returned, a slot moved — and the cell is re-read on each. Every lease guard
    /// holds a sender clone, so the channel cannot close while a lease that could
    /// still drain this cell is outstanding.
    pub(crate) async fn drained(&self, cell: Arc<LeaseCell>) {
        let mut version = self.watch();
        while !cell.is_drained() {
            if version.changed().await.is_err() {
                // Every sender is gone: no lease guard survives, so no release is
                // owed and waiting longer would wait on nothing.
                return;
            }
        }
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        self.cell.release();
        self.version
            .send_modify(|edge| *edge = edge.wrapping_add(1));
    }
}
