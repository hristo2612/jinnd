//! The daemon's observation and revert surface: keyed fs revert, effect,
//! fiber, entry, and ledger reads, and the transition sync. Split from
//! `daemon.rs` by responsibility (R10 file hygiene).

use jinnd_api::{
    EffectId, EntryId, ErrorCode, FiberId, FiberState, KernelError, LedgerEventKind, LedgerQuery,
    LedgerRecord, RevertKey, RevertResolution,
};

use crate::support::{error, lock};

use super::{Daemon, storage};

impl Daemon {
    /// Keyed exactly-once revert of one recorded fs effect (Law 3): the
    /// inverse restores the prior content, absence, or length from the
    /// retention store; the witness reads the file back. Receipts land in
    /// the ledger either way.
    ///
    /// # Errors
    ///
    /// An effect this daemon's provider does not own, a distinct key for an
    /// already-claimed effect, or a storage refusal.
    pub async fn revert(
        &self,
        effect: EffectId,
        key: &str,
    ) -> Result<RevertResolution, KernelError> {
        // The provider that captured the write's inverse builds the action
        // (Law 3, M2-K1 seam); this daemon feeds it to the ledger's keyed
        // exactly-once protocol.
        let (witness, inverse) = self.hostfs.undo_action(effect).ok_or_else(|| {
            error(
                ErrorCode::EffectFailed,
                format!("no revertible effect {}", effect.0),
            )
        })?;
        let resolution = self
            .revert
            .revert(
                effect,
                RevertKey(key.to_owned()),
                witness,
                inverse,
                None,
                None,
            )
            .await?;
        // Consumption reclaims the spilled inverse (M2-K3 retention): only
        // once the ledger holds the branch `Reverted`, so a crash between
        // completion and reclaim resumes from the record, never re-runs.
        if resolution == RevertResolution::Reverted {
            self.hostfs.reclaim(effect).await?;
        }
        Ok(resolution)
    }

    /// Every live (unconsumed) fs effect — write, append, remove — in id
    /// order.
    #[must_use]
    pub fn fs_effects(&self) -> Vec<(EffectId, String)> {
        self.hostfs.effects()
    }

    /// The fiber currently hosting `entry`, if any.
    #[must_use]
    pub fn entry_fiber(&self, entry: &str) -> Option<FiberId> {
        self.loader.entry_fiber(&EntryId(entry.to_owned()))
    }

    /// The last committed state of one loader-owned fiber.
    #[must_use]
    pub fn fiber_state(&self, fiber: FiberId) -> Option<FiberState> {
        self.loader.fiber_state(fiber)
    }

    /// The committed entry ids, for operator status output.
    #[must_use]
    pub fn entries(&self) -> Vec<String> {
        self.loader
            .persisted::<serde_json::Value>()
            .map(|profile| {
                profile
                    .entries
                    .iter()
                    .map(|entry| entry.id.0.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The whole ledger stream, in sequence order.
    ///
    /// # Errors
    ///
    /// Storage refusals.
    pub async fn ledger_events(&self) -> Result<Vec<LedgerRecord>, KernelError> {
        self.ledger
            .events(LedgerQuery::default())
            .await
            .map_err(storage)
    }

    /// Emits every committed fiber transition the ledger has not yet seen
    /// (R6: transitions are ledger events; ordered, unreceipted lane).
    pub fn sync_transitions(&self) {
        let mut fibers = lock(&self.fibers);
        for tracked in fibers.values_mut() {
            let transitions = tracked.fiber.record().transitions;
            for transition in transitions.iter().skip(tracked.recorded) {
                self.ledger.record(
                    LedgerEventKind::FiberTransition(transition.clone()),
                    None,
                    Some(tracked.fiber.id()),
                );
            }
            tracked.recorded = transitions.len();
        }
    }
}
