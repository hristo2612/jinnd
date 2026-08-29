//! The live seat as `jinn:introspect` reports it (M2-K7): registrations
//! counted per class, a snapshot under the slot lock. Split from `slot.rs`
//! by responsibility (R10 file hygiene).

use crate::handle::Registration;

use super::SharedSlot;

impl SharedSlot {
    /// The live seat's registrations, counted per class — the
    /// `jinn:introspect` view (M2-K7; a snapshot under the lock, never a
    /// walk into guest code, R1). `None` when no seat is live.
    pub fn summary(&self) -> Option<SeatSummary> {
        let guard = self.lock();
        let seat = guard.as_ref()?;
        let mut summary = SeatSummary::default();
        for registration in &seat.registrations {
            match registration {
                Registration::Provision { contract } => summary.provisions.push(contract.clone()),
                Registration::Listen(record) if record.id.is_some() => summary.listeners += 1,
                Registration::Alarm(record) if record.id.is_some() => summary.alarms += 1,
                Registration::Kernel(record)
                    if record.contract == crate::hostcaps::NET_CONTRACT =>
                {
                    summary.sockets += 1;
                }
                Registration::Kernel(record)
                    if record.contract == crate::hostcaps::PROCESS_CONTRACT =>
                {
                    summary.processes += 1;
                }
                _ => {}
            }
        }
        Some(summary)
    }
}

/// One live seat's kernel registrations by class (M2-K7, `jinn:introspect`).
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SeatSummary {
    pub provisions: Vec<String>,
    pub listeners: u32,
    pub alarms: u32,
    pub sockets: u32,
    pub processes: u32,
}
