//! The entry-scoped world-effect journal (M2-K4): what suspended seats hand
//! back, inherited across incarnations and withdrawn LIFO at true dispose.
//! Split from `lane.rs` by responsibility (R10 file hygiene).

use jinnd_api::{EntryId, FiberId, KernelError, LedgerEventKind};

use crate::handle::HostRecord;

use super::{LaneCore, lock};

impl LaneCore {
    /// Hands `entry` retained world effects (M2-K4): a suspended seat's,
    /// or a prior process's, rehydrated from the provider's retention store
    /// by the assembly at open. Appended after what the entry already holds.
    pub fn inherit(&self, entry: &EntryId, records: Vec<HostRecord>) {
        let mut journals = lock(&self.journals);
        let journal = journals.entry(entry.clone()).or_default();
        // A keyed replay answers the recorded effect (03 §Act): the seat
        // journals the same id again, and the entry's journal keeps ONE —
        // the trail is the contribution, never a contribution twice.
        for record in records {
            if !journal.iter().any(|held| held.effect == record.effect) {
                journal.push(record);
            }
        }
    }

    /// Forgets effects the live seat's own trail just withdrew: a keyed
    /// replay's id may sit in both, and it withdraws exactly once.
    pub(super) fn release(&self, entry: &EntryId, withdrawn: &[u64]) {
        if let Some(journal) = lock(&self.journals).get_mut(entry) {
            journal.retain(|record| !withdrawn.contains(&record.effect));
        }
    }

    /// The entries holding a retained journal right now.
    #[must_use]
    pub fn journaled_entries(&self) -> Vec<EntryId> {
        let mut entries: Vec<EntryId> = lock(&self.journals)
            .iter()
            .filter(|(_, records)| !records.is_empty())
            .map(|(entry, _)| entry.clone())
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    /// Withdraws `entry`'s retained journal LIFO through each effect's
    /// current provider (M2-K4; R5, I1) — the entry left the composition,
    /// so its whole contribution goes, every withdrawal ledgered under the
    /// entry's (and, when known, the fiber's) attribution. The first
    /// failing inverse is reported after the rest still ran (R9, R11).
    ///
    /// # Errors
    ///
    /// The first failing inverse.
    pub async fn withdraw_journal(
        &self,
        entry: &EntryId,
        fiber: Option<FiberId>,
    ) -> Result<(), KernelError> {
        let retained = lock(&self.journals).remove(entry).unwrap_or_default();
        let mut first = None;
        for record in retained.iter().rev() {
            let outcome = self
                .broker
                .withdraw_effect(&record.contract, record.effect)
                .await;
            self.sink.append_for(
                LedgerEventKind::EffectWithdrawn {
                    label: record.label.clone(),
                    clean: outcome.is_ok(),
                },
                Some(entry.clone()),
                fiber,
            );
            if let Err(error) = outcome {
                first.get_or_insert(error);
            }
        }
        first.map_or(Ok(()), Err)
    }
}
