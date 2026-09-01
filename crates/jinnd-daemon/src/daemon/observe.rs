//! The daemon's observation and revert surface: keyed fs revert, effect,
//! fiber, entry, and ledger reads, and the transition sync. Split from
//! `daemon.rs` by responsibility (R10 file hygiene).

use jinnd_api::{
    EffectId, EntryId, ErrorCode, FiberId, FiberState, KernelError, LedgerEventKind, LedgerQuery,
    LedgerRecord, RevertKey, RevertResolution,
};

use crate::support::error;

use super::{Daemon, storage};

/// One member of a revert unit, named by the provider that owns it
/// (constitution 03 §Units of revert). Effect ids are minted per provider,
/// so the provider is part of the address — a bare number would be
/// ambiguous, and an ambiguous revert is the one thing Law 3 cannot have.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnitMember {
    /// A revertible `jinn:fs` write, append, or removal.
    Fs(EffectId),
    /// A revertible `jinn:keystore` put or delete.
    Keystore(EffectId),
    /// An outbound `jinn:net` call: DECLARED IRREVERSIBLE (M2-K14;
    /// contracts/jinn-net §operations.request).
    Net(EffectId),
}

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

    /// Reverts one UNIT (constitution 03 §Units of revert), LIFO.
    ///
    /// The unit is decided WHOLE before any inverse runs: if it contains
    /// an effect whose contract declares the operation `irreversible`, the
    /// unit is REJECTED and nothing in it is applied (03 §51). A revert
    /// that silently skipped the irreversible member — or that succeeded
    /// while pretending the call had not happened — would be exactly the
    /// falsehood Law 3 exists to prevent, so the refusal is typed
    /// ([`ErrorCode::Irreversible`]) and names WHICH effect it could not
    /// revert and WHY. There is no declared compensator for an outbound
    /// call: the kernel cannot know what would correct an arbitrary one.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::Irreversible`] for a unit containing an irreversible
    /// effect, [`ErrorCode::NotFound`] for a member no provider owns, and
    /// otherwise as [`Daemon::revert`].
    pub async fn revert_unit(
        &self,
        unit: &[UnitMember],
        key: &str,
    ) -> Result<Vec<RevertResolution>, KernelError> {
        for member in unit {
            if let UnitMember::Net(effect) = member {
                return Err(match self.net_effect(*effect).await? {
                    Some(label) => error(
                        ErrorCode::Irreversible,
                        format!(
                            "revert refused: the unit contains {label}, and a sent request cannot be un-sent — `jinn:net` outbound is declared irreversible with no inverse and no compensator (constitution 03 §51). Nothing in the unit was reverted."
                        ),
                    ),
                    None => error(
                        ErrorCode::NotFound,
                        format!("no jinn:net request effect {}", effect.0),
                    ),
                });
            }
        }
        let mut resolved = Vec::with_capacity(unit.len());
        for member in unit.iter().rev() {
            resolved.push(match member {
                UnitMember::Fs(effect) => self.revert(*effect, key).await?,
                UnitMember::Keystore(effect) => self.revert_keystore(*effect, key).await?,
                UnitMember::Net(_) => unreachable!("rejected above"),
            });
        }
        Ok(resolved)
    }

    /// Every outbound `jinn:net` call this kernel has made, in id order:
    /// (id, label), read from the DURABLE record. They are irreversible,
    /// so they are never consumed and never withdrawn — and the register
    /// is the ledger itself, not a live map, so the list is the same after
    /// a reopen as before it (M2-K14 round 2; R5).
    ///
    /// # Errors
    ///
    /// A storage refusal reading the ledger.
    pub async fn net_effects(&self) -> Result<Vec<(EffectId, String)>, KernelError> {
        Ok(net_calls(&self.ledger_events().await?).collect())
    }

    /// The label of one outbound call, if the durable record carries it.
    async fn net_effect(&self, effect: EffectId) -> Result<Option<String>, KernelError> {
        Ok(net_calls(&self.ledger_events().await?)
            .find(|(id, _)| *id == effect)
            .map(|(_, label)| label))
    }

    /// Seeds the net provider's effect counter past the highest id the
    /// durable record has already spent, so a reopened daemon never
    /// re-mints an id that names a call it can never take back (M2-K14).
    ///
    /// # Errors
    ///
    /// A storage refusal reading the ledger.
    pub(super) async fn seed_net_effects(&self) -> Result<(), KernelError> {
        let highest = net_calls(&self.ledger_events().await?)
            .map(|(effect, _)| effect.0)
            .max()
            .unwrap_or(0);
        self.hostnet.seed_effects(highest);
        Ok(())
    }

    /// Keyed exactly-once revert of one recorded keystore effect (M2-K8;
    /// Law 3): the inverse restores the prior value or absence from the
    /// sealed spill; the witness reads the key back.
    ///
    /// # Errors
    ///
    /// As [`Daemon::revert`].
    pub async fn revert_keystore(
        &self,
        effect: EffectId,
        key: &str,
    ) -> Result<RevertResolution, KernelError> {
        let (witness, inverse) = self.keystore.undo_action(effect).ok_or_else(|| {
            error(
                ErrorCode::EffectFailed,
                format!("no revertible keystore effect {}", effect.0),
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
        if resolution == RevertResolution::Reverted {
            self.keystore.reclaim(effect).await?;
        }
        Ok(resolution)
    }

    /// Every live (unconsumed) keystore effect — put, delete — in id
    /// order: (id, key name).
    #[must_use]
    pub fn keystore_effects(&self) -> Vec<(EffectId, String)> {
        self.keystore.effects()
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
        crate::support::sync_transitions(&self.fibers, &self.ledger, Some(&self.lifecycle));
    }

    /// The file watcher is armed (M2-K7 `jinn:introspect` readiness): the
    /// shell says so once `Watch::start` returned — the daemon never
    /// claims a watcher it cannot see.
    pub fn mark_watcher_armed(&self) {
        self.readiness
            .watcher_armed
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Every outbound call in `records`, as (effect, label). The label names
/// WHAT the kernel could not take back — method, authority and path, the
/// same redacted shape the row itself carries (02 §Redaction): a refusal
/// that named a body or a header would leak the credential the row was
/// careful not to hold.
fn net_calls(records: &[LedgerRecord]) -> impl Iterator<Item = (EffectId, String)> + '_ {
    records.iter().filter_map(|record| match &record.kind {
        LedgerEventKind::NetRequested {
            effect,
            method,
            host,
            path,
            ..
        } => Some((
            *effect,
            format!(
                "jinn:net request {method} {host}{path} [effect {}]",
                effect.0
            ),
        )),
        _ => None,
    })
}
