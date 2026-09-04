//! The `jinn:profile-admin` provider (M2-K23, harness #37; constitution 04
//! §Write-back is confined): five writes that reshape the composition —
//! add, remove, `disabled`, grants, plugin identity — each an OPERATOR
//! INTENT applied by reconcile-by-id through the loader's runtime-led
//! amendments, each a `ProfileAdministered` row naming the CALLER ENTRY and
//! the rendered document's digest before and after, each reversible by the
//! inverse write the row's `prior` records. What authorizes a write is a
//! separate `jinn:profile-admin` grant on the calling entry: whoever holds
//! it is the operator's delegate by the operator's own document. It confines
//! PLUGINS; it does not authenticate the operator (`jinn:auth`'s and the
//! transport's), and a same-uid process editing the file is outside every
//! grant. Refusals are typed on the wire and on the record; the restart or
//! activation is scheduled, never awaited in the caller's host call (R1).

use std::sync::Arc;

use jinnd_api::{EntryId, KernelError, KernelFuture, LedgerEventKind, RefusalReason};
use jinnd_ledger::Ledger;
use jinnd_loader::{Administration, Loader};
use jinnd_wasm::{Broker, PROFILE_ADMIN_CONTRACT, Peer, PeerId, hex_digest};

mod decode;
mod writes;

use writes::{Change, Class, Refusal};

use super::profile_cap::{patch::validate, refused_wire};
use super::storage;
use super::wire::{Callers, unknown};
use crate::support::{SharedFibers, sync_transitions};

/// Answer tag: accepted — both views committed; the row's u64-LE sequence
/// follows.
const TAG_ACCEPTED: u8 = 2;

pub(crate) struct HostProfileAdmin {
    loader: Arc<Loader>,
    ledger: Ledger,
    fibers: SharedFibers,
    lifecycle: Arc<crate::daemon::Lifecycle>,
    callers: Callers,
}

impl HostProfileAdmin {
    /// Registers the provider as a broker peer holding and providing the
    /// contract (providing is authority).
    ///
    /// # Errors
    ///
    /// The broker's refusal of the provision.
    pub(crate) fn register(
        broker: &Arc<Broker>,
        loader: Arc<Loader>,
        ledger: Ledger,
        fibers: SharedFibers,
        lifecycle: Arc<crate::daemon::Lifecycle>,
    ) -> Result<(), KernelError> {
        let peer = broker.register_peer(None);
        broker.grant(peer, PROFILE_ADMIN_CONTRACT);
        let provider = Arc::new(Self {
            loader,
            ledger,
            fibers,
            lifecycle,
            callers: Callers::new(broker, PROFILE_ADMIN_CONTRACT),
        });
        broker.provide(peer, PROFILE_ADMIN_CONTRACT, Arc::new(AdminPeer(provider)))
    }

    /// One write: decode, authorize (scope, then self/ancestor), check the
    /// whole result before anything moves, apply through the loader,
    /// record. Every refusal is the typed `refused(class, reason)` on the
    /// wire and a row on the record.
    async fn write(
        &self,
        caller: PeerId,
        operation: &str,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, KernelError> {
        let (fiber, by) = self.callers.attribution(caller);
        let request = match decode::decode(operation, &payload) {
            Ok(request) => request,
            Err(refusal) => return Ok(self.refuse(operation, None, by, fiber, &refusal)),
        };
        let entry = request.entry.clone();
        let scope = self.callers.policy(caller);
        let admits = |id: &str| scope.as_ref().is_some_and(|scope| scope.admits_entry(id));
        let parent = request.parent().map(|parent| parent.0.clone());
        if !admits(&entry.0) || parent.as_deref().is_some_and(|parent| !admits(parent)) {
            let reason = format!(
                "grant refused: {PROFILE_ADMIN_CONTRACT} scope does not admit entry {:?}",
                entry.0
            );
            self.ledger.record(
                LedgerEventKind::GrantRefused {
                    contract: PROFILE_ADMIN_CONTRACT.to_owned(),
                    reason: RefusalReason::ScopeMismatch,
                    detail: Some(reason.clone()),
                },
                by,
                fiber,
            );
            return Ok(refused_wire_class(Class::Unauthorized, &reason));
        }
        let Some(profile) = self.loader.persisted::<serde_json::Value>() else {
            let refusal = Refusal::conflict("no committed document");
            return Ok(self.refuse(operation, Some(&entry), by, fiber, &refusal));
        };
        if let Err(refusal) = writes::authorize_target(&profile, &request, by.as_ref()) {
            return Ok(self.refuse(operation, Some(&entry), by, fiber, &refusal));
        }
        let prior = match writes::check(&profile, &self.loader, &request) {
            Ok(prior) => prior,
            Err(refusal) => return Ok(self.refuse(operation, Some(&entry), by, fiber, &refusal)),
        };
        let before = self.digest();
        if let Err(refused) = self.apply(&entry, request.change, prior.as_ref()).await {
            let refusal = Refusal::conflict(&refused.message);
            return Ok(self.refuse(operation, Some(&entry), by, fiber, &refusal));
        }
        let after = self.digest();
        let by_name = by
            .as_ref()
            .map_or_else(|| format!("peer:{caller}"), |by| by.0.clone());
        let receipt = self
            .ledger
            .append(
                LedgerEventKind::ProfileAdministered {
                    entry: entry.clone(),
                    by: by_name,
                    write: request.write,
                    before,
                    after,
                    prior: prior.map(|prior| prior.to_string()),
                },
                by,
                fiber,
            )
            .await
            .map_err(storage)?;
        // The transitions land on the ledger when they settle — from a
        // task of their own, never inside this host call (R1).
        let (loader, fibers, ledger, publisher) = (
            Arc::clone(&self.loader),
            Arc::clone(&self.fibers),
            self.ledger.clone(),
            Arc::clone(&self.lifecycle),
        );
        tokio::spawn(async move {
            loader.quiesce_entry(&entry).await;
            sync_transitions(&fibers, &ledger, Some(&publisher));
        });
        let mut wire = vec![TAG_ACCEPTED];
        wire.extend(receipt.sequence.to_le_bytes());
        Ok(wire)
    }

    /// Hands the loader the one runtime-led amendment the write is.
    async fn apply(
        &self,
        entry: &EntryId,
        change: Change,
        prior: Option<&serde_json::Value>,
    ) -> Result<(), KernelError> {
        match change {
            Change::Add(record) => self.loader.administer(Administration::Add(record)).await,
            Change::Remove => {
                self.loader
                    .administer::<serde_json::Value>(Administration::Remove(entry.clone()))
                    .await
            }
            Change::SetDisabled(true) => {
                self.loader.dispose_entry::<serde_json::Value>(entry).await
            }
            Change::SetDisabled(false) => {
                self.loader
                    .administer::<serde_json::Value>(Administration::Enable(entry.clone()))
                    .await
            }
            Change::SetGrants(grants) => {
                let mut config = prior
                    .and_then(|prior| prior.get("config").cloned())
                    .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));
                config["grants"] = grants;
                validate(&config, &config).map_err(|reason| {
                    crate::support::error(jinnd_api::ErrorCode::InvalidProfile, reason)
                })?;
                self.loader.update_entry_deferred(entry, config).await
            }
            Change::Swap(plugin) => {
                self.loader
                    .administer::<serde_json::Value>(Administration::Swap(entry.clone(), plugin))
                    .await
            }
        }
    }

    /// The SHA-256 hex digest of the rendered document of record: what the
    /// loader wrote, byte-for-byte, so it equals the file's digest.
    fn digest(&self) -> String {
        hex_digest(
            self.loader
                .rendered_document()
                .unwrap_or_default()
                .as_bytes(),
        )
    }

    /// A refusal on the wire AND on the record (Law 2), attributed to the
    /// caller; the class rides the wire and the detail.
    fn refuse(
        &self,
        operation: &str,
        entry: Option<&EntryId>,
        by: Option<EntryId>,
        fiber: Option<jinnd_api::FiberId>,
        refusal: &Refusal,
    ) -> Vec<u8> {
        let target = entry.map_or_else(String::new, |entry| format!(" {:?}", entry.0));
        self.ledger.record(
            LedgerEventKind::AmendmentRefused {
                detail: format!(
                    "{operation}{target} refused ({}): {}",
                    refusal.class.name(),
                    refusal.reason
                ),
            },
            by,
            fiber,
        );
        refused_wire_class(refusal.class, &refusal.reason)
    }
}

/// The bundle's `refused(class, reason)`: tag 1, one class byte, the
/// reason's UTF-8 — the `jinn:profile` answer with the class in front.
fn refused_wire_class(class: Class, reason: &str) -> Vec<u8> {
    let mut wire = refused_wire(reason);
    wire.insert(1, class as u8);
    wire
}

/// The provider's broker face.
struct AdminPeer(Arc<HostProfileAdmin>);

impl Peer for AdminPeer {
    fn call(
        &self,
        caller: PeerId,
        _contract: &str,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>> {
        let provider = Arc::clone(&self.0);
        let operation = operation.to_owned();
        Box::pin(async move {
            match operation.as_str() {
                "add-entry" | "remove-entry" | "set-disabled" | "set-grants" | "swap-plugin" => {
                    provider.write(caller, &operation, payload).await
                }
                other => Err(unknown(PROFILE_ADMIN_CONTRACT, other)),
            }
        })
    }
}
