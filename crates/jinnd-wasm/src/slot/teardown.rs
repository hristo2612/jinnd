//! A seat's closing sequences — `retire` (withdraw everything, R5/I1) and
//! `suspend` (release kernel registrations, retain world effects; M2-K4).
//! Split from `slot.rs` by responsibility (R10 file hygiene).

use jinnd_api::{FiberId, KernelError, LedgerEventKind};

use crate::alarms::Alarms;
use crate::broker::Broker;
use crate::handle::{HostRecord, Registration};
use crate::peer::{LedgerSink, PeerId};
use crate::topics::LocalTopics;

use super::SeatState;

impl SeatState {
    /// Withdraws exactly this seat's contribution (I1) as ONE LIFO replay of
    /// the registration journal (LAW §3; R5: no parallel per-category
    /// loops): each undo runs against the instance that registered it, and
    /// with a `ledger` every withdrawal — effect, listener, and provision
    /// alike — is appended at the moment it actually runs, so the recorded
    /// trail is strictly reverse of the registration sequence (Law 2). The
    /// instance disposes last (R7 instant dispose). The first failing
    /// inverse is reported after the remaining withdrawal still ran
    /// (R9, R11).
    ///
    /// # Errors
    ///
    /// The first guest inverse failure, with everything else withdrawn.
    pub async fn retire(
        self,
        broker: &Broker,
        topics: &LocalTopics,
        alarms: &Alarms,
        peer: PeerId,
        ledger: Option<(&dyn LedgerSink, FiberId)>,
    ) -> Result<(), KernelError> {
        let mut first = None;
        // Keyed by (contract, effect): ids are per PROVIDER (M2-K8 — the
        // fs and keystore stores each mint from their own epoch), so a
        // bare id is not an identity across contracts.
        let mut withdrawn_hosts: Vec<(&str, u64)> = Vec::new();
        for registration in self.registrations.iter().rev() {
            match registration {
                Registration::Effect { label, token } => {
                    let outcome = self.instance.undo(*token).await;
                    if let Some((sink, fiber)) = ledger {
                        sink.append(
                            LedgerEventKind::EffectWithdrawn {
                                label: label.clone(),
                                clean: outcome.is_ok(),
                            },
                            Some(fiber),
                        );
                    }
                    if let Err(error) = outcome {
                        first.get_or_insert(error);
                    }
                }
                Registration::Listen(record) => {
                    let topic = record.id.and_then(|id| topics.unlisten(id));
                    if let (Some((sink, fiber)), Some(topic)) = (ledger, topic) {
                        sink.append(
                            LedgerEventKind::EffectWithdrawn {
                                label: format!("listen {topic}"),
                                clean: true,
                            },
                            Some(fiber),
                        );
                    }
                }
                // The alarm effect's undo: cancel host-side (M2-K2, R5).
                // After this, no wake of the id is ever ledgered again.
                Registration::Alarm(record) => {
                    let cancelled = record.id.is_some_and(|id| alarms.cancel(id));
                    if let Some((sink, fiber)) = ledger
                        && cancelled
                    {
                        sink.append(
                            LedgerEventKind::EffectWithdrawn {
                                label: record.label.clone(),
                                clean: true,
                            },
                            Some(fiber),
                        );
                    }
                }
                // A host-provider effect withdraws through the contract's
                // current provider (M2-K3; R5): inverse from the spill,
                // storage reclaimed, ledgered under its own label.
                Registration::Host(record) => {
                    // A keyed replay journaled the same id twice (03 §Act):
                    // it withdraws exactly once.
                    if withdrawn_hosts.contains(&(record.contract.as_str(), record.effect)) {
                        continue;
                    }
                    withdrawn_hosts.push((record.contract.as_str(), record.effect));
                    let outcome = broker
                        .withdraw_effect(&record.contract, record.effect)
                        .await;
                    if let Some((sink, fiber)) = ledger {
                        sink.append(
                            LedgerEventKind::EffectWithdrawn {
                                label: record.label.clone(),
                                clean: outcome.is_ok(),
                            },
                            Some(fiber),
                        );
                    }
                    if let Err(error) = outcome {
                        first.get_or_insert(error);
                    }
                }
                // A kernel registration releases through its provider on
                // dispose exactly as on suspend (M2-K6): kill, close.
                Registration::Kernel(record) => {
                    release(broker, record, ledger, &mut first).await;
                }
                // The broker appends the withdrawal itself (R6), so it too
                // lands at the moment it runs.
                Registration::Provision { contract } => broker.withdraw(peer, contract),
            }
        }
        self.instance.dispose().await;
        match first {
            None => Ok(()),
            Some(error) => Err(error),
        }
    }

    /// Suspends exactly this seat (M2-K4; decision log 2026-08-28): ONE
    /// LIFO pass over the same journal that RELEASES kernel registrations
    /// — listeners unlisten, alarms cancel, provisions withdraw, each
    /// ledgered as it runs — and RETAINS world mutations: the host-provider
    /// effects are handed back, in registration order, for the entry's live
    /// journal. Guest-owned inverses are instance-bound by nature (their
    /// undo lives in the store that disposes here) and run no more than the
    /// process's crash would have run them; the seat's suspension is the
    /// ledgered fact. The instance disposes last.
    pub async fn suspend(
        self,
        broker: &Broker,
        topics: &LocalTopics,
        alarms: &Alarms,
        peer: PeerId,
        ledger: Option<(&dyn LedgerSink, FiberId)>,
    ) -> Vec<HostRecord> {
        // The world effects, in registration order, once each (a keyed
        // replay journaled its id again).
        let mut retained: Vec<HostRecord> = Vec::new();
        for registration in &self.registrations {
            if let Registration::Host(record) = registration
                && !retained.iter().any(|held| held.effect == record.effect)
            {
                retained.push(record.clone());
            }
        }
        for registration in self.registrations.iter().rev() {
            match registration {
                Registration::Effect { .. } | Registration::Host(_) => {}
                // A suspended incarnation owns no live child or socket
                // (M2-K6): the registration releases, ledgered, and the
                // next activate re-establishes it.
                Registration::Kernel(record) => {
                    let mut first = None;
                    release(broker, record, ledger, &mut first).await;
                }
                Registration::Listen(record) => {
                    let topic = record.id.and_then(|id| topics.unlisten(id));
                    if let (Some((sink, fiber)), Some(topic)) = (ledger, topic) {
                        sink.append(
                            LedgerEventKind::EffectWithdrawn {
                                label: format!("listen {topic}"),
                                clean: true,
                            },
                            Some(fiber),
                        );
                    }
                }
                Registration::Alarm(record) => {
                    let cancelled = record.id.is_some_and(|id| alarms.cancel(id));
                    if let Some((sink, fiber)) = ledger
                        && cancelled
                    {
                        sink.append(
                            LedgerEventKind::EffectWithdrawn {
                                label: record.label.clone(),
                                clean: true,
                            },
                            Some(fiber),
                        );
                    }
                }
                Registration::Provision { contract } => broker.withdraw(peer, contract),
            }
        }
        self.instance.dispose().await;
        retained
    }
}

/// Releases one kernel registration through its provider (M2-K6): the
/// withdrawal is ledgered under the registration's label as it runs, a
/// failing release is contained and reported first-wins (R9, R11).
async fn release(
    broker: &Broker,
    record: &HostRecord,
    ledger: Option<(&dyn LedgerSink, FiberId)>,
    first: &mut Option<KernelError>,
) {
    let outcome = broker
        .withdraw_effect(&record.contract, record.effect)
        .await;
    if let Some((sink, fiber)) = ledger {
        sink.append(
            LedgerEventKind::EffectWithdrawn {
                label: record.label.clone(),
                clean: outcome.is_ok(),
            },
            Some(fiber),
        );
    }
    if let Err(error) = outcome {
        first.get_or_insert(error);
    }
}
