//! The state a fiber's handle and its supervisor share.

use std::sync::Mutex;
use std::sync::atomic::AtomicU64;

use jinnd_api::{EffectDescriptor, FiberId, FiberState, KernelError, Transition};
use jinnd_effects::ReplayReport;
use tokio::sync::{Notify, watch};

use crate::record::FiberRecord;
use crate::steering::SteeringCell;

/// The handle-visible half of a fiber.
///
/// Everything here is either a `watch` channel or a lock held for one field update:
/// no lock in this crate is ever held across an `await` or across a call into plugin
/// code (R1).
#[derive(Debug)]
pub(crate) struct Shared {
    pub(crate) id: FiberId,
    pub(crate) steering: SteeringCell,
    pub(crate) state: watch::Sender<FiberState>,
    /// Woken whenever a handle changes what the fiber should be doing.
    pub(crate) wake: Notify,
    /// Bumped by every caller that wants to know the fiber has settled since.
    pub(crate) probe: AtomicU64,
    /// The highest probe the supervisor has acknowledged at quiescence.
    pub(crate) settled: watch::Sender<u64>,
    record: Mutex<FiberRecord>,
}

impl Shared {
    pub(crate) fn new(id: FiberId, epoch: Option<jinnd_api::Epoch>) -> Self {
        Self {
            id,
            steering: SteeringCell::new(epoch),
            state: watch::Sender::new(FiberState::Pending),
            wake: Notify::new(),
            probe: AtomicU64::new(0),
            settled: watch::Sender::new(0),
            record: Mutex::new(FiberRecord::default()),
        }
    }

    pub(crate) fn record(&self) -> FiberRecord {
        self.with_record(|record| record.clone())
    }

    pub(crate) fn effects(&self) -> Vec<EffectDescriptor> {
        self.with_record(|record| record.effects.clone())
    }

    pub(crate) fn transitioned(&self, transition: Transition) {
        self.with_record(|record| record.transitions.push(transition));
    }

    pub(crate) fn fail(&self, error: KernelError) {
        self.with_record(|record| record.failures.push(error));
    }

    pub(crate) fn replayed(&self, report: ReplayReport) {
        self.with_record(|record| record.replays.push(report));
    }

    pub(crate) fn published(&self, effects: Vec<EffectDescriptor>) {
        self.with_record(|record| record.effects = effects);
    }

    /// Acknowledges every probe up to `asked` as settled.
    pub(crate) fn settle(&self, asked: u64) {
        self.settled.send_if_modified(|acknowledged| {
            if *acknowledged >= asked {
                return false;
            }
            *acknowledged = asked;
            true
        });
    }

    fn with_record<T>(&self, read: impl FnOnce(&mut FiberRecord) -> T) -> T {
        let mut record = self
            .record
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        read(&mut record)
    }
}
