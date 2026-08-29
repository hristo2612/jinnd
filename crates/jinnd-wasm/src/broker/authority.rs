//! The broker's authority bookkeeping — grants and their typed scopes,
//! THE grant check, and each peer's delivery face — split from `broker.rs`
//! by responsibility (R10 file hygiene). Same struct, same lock discipline:
//! no guard is ever held across a call into a peer (R1).

use std::sync::Arc;

use jinnd_api::{ErrorCode, FiberId, KernelError, LedgerEventKind, RefusalReason};

use crate::broker_state::{OpsClass, refusal};
use crate::grants::GrantScope;
use crate::peer::{LedgerSink, PeerId};
use crate::topics::EventTarget;

use super::Broker;

impl Broker {
    /// Attaches `peer`'s delivery face (M2-K7): a host provider wakes the
    /// peer through it (`jinn:net/readable`). Re-attaching replaces — the
    /// Mode-1 commit points the peer at its successor instance (R8).
    pub fn attach_target(&self, peer: PeerId, target: Arc<dyn EventTarget>) {
        if let Some(record) = self.lock().peers.get_mut(&peer) {
            record.target = Some(target);
        }
    }

    /// The peer's current delivery face, if attached.
    #[must_use]
    pub fn target_of(&self, peer: PeerId) -> Option<Arc<dyn EventTarget>> {
        self.lock()
            .peers
            .get(&peer)
            .and_then(|record| record.target.clone())
    }

    /// Names the profile entry `peer` acts for (M2-K4): host providers
    /// retain inverses under the entry, whose contribution outlives any one
    /// fiber incarnation.
    pub fn attribute_entry(&self, peer: PeerId, entry: &str) {
        if let Some(record) = self.lock().peers.get_mut(&peer) {
            record.entry = Some(entry.to_owned());
        }
    }

    /// The profile entry `peer` acts for, if named.
    #[must_use]
    pub fn entry_of(&self, peer: PeerId) -> Option<String> {
        self.lock()
            .peers
            .get(&peer)
            .and_then(|record| record.entry.clone())
    }

    /// The one ledger sink every crossing lands on (R6), for a surface that
    /// must record a refusal with the caller's attribution.
    #[must_use]
    pub fn ledger(&self) -> &Arc<dyn LedgerSink> {
        &self.ledger
    }

    /// The fiber `peer` is attributed to, for a native provider ledgering
    /// an effect on a caller's behalf (R4: effects are charged to the
    /// caller by construction).
    #[must_use]
    pub fn attribution(&self, peer: PeerId) -> Option<FiberId> {
        self.lock().fiber_of(peer)
    }

    /// Grants `peer` the named contract (constitution 01: grants arrive from
    /// the profile/policy side; requests are not grants).
    pub fn grant(&self, peer: PeerId, contract: &str) {
        self.grant_with(peer, contract, GrantScope::Root);
    }

    /// Grants `peer` the named contract under one path-prefix `scope`
    /// (M2-K3 round 2; constitution 01 §Grants attenuation). Scopes
    /// accumulate; a root grant already held stays root.
    pub fn grant_scoped(&self, peer: PeerId, contract: &str, scope: &str) {
        self.grant_with(peer, contract, GrantScope::Paths(vec![scope.to_owned()]));
    }

    /// Grants `peer` the named contract under one ADMITTED typed
    /// authority (M2-K6; round-2 ruling 2): grants of one contract compose
    /// by [`GrantScope::union`] in either order — root stays root, list
    /// scopes accumulate.
    pub fn grant_with(&self, peer: PeerId, contract: &str, scope: GrantScope) {
        if let Some(record) = self.lock().peers.get_mut(&peer) {
            record
                .grants
                .entry(contract.to_owned())
                .and_modify(|held| held.union(scope.clone()))
                .or_insert(scope);
        }
    }

    /// Attenuates `peer`'s grant of `contract` to the operation class `ops`
    /// (M2-K8, harness #24). Classes of one contract compose by union in
    /// either order; a class already every operation stays so.
    pub fn grant_ops(&self, peer: PeerId, contract: &str, ops: Vec<String>) {
        self.widen_ops(peer, contract, OpsClass::Only(ops));
    }

    /// Widens `peer`'s grant of `contract` to every operation: an
    /// unattenuated grant of the contract, before or after an attenuated
    /// one, makes the class every operation (absorbing, never undone).
    pub(crate) fn lift_ops(&self, peer: PeerId, contract: &str) {
        self.widen_ops(peer, contract, OpsClass::All);
    }

    fn widen_ops(&self, peer: PeerId, contract: &str, class: OpsClass) {
        if let Some(record) = self.lock().peers.get_mut(&peer) {
            record
                .ops
                .entry(contract.to_owned())
                .and_modify(|held| held.union(class.clone()))
                .or_insert(class);
        }
    }

    /// The operation-class check every dispatch lane runs after the grant
    /// check (M2-K8): an operation outside the caller's attenuation is a
    /// ledgered scope refusal naming it, never a default-accept.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::EffectFailed`] with the refused operation named.
    pub fn check_op(
        &self,
        peer: PeerId,
        contract: &str,
        operation: &str,
    ) -> Result<(), KernelError> {
        let (allowed, fiber) = {
            let state = self.lock();
            let allowed = state.peers.get(&peer).is_none_or(|record| {
                record
                    .ops
                    .get(contract)
                    .is_none_or(|class| class.admits(operation))
            });
            (allowed, state.fiber_of(peer))
        };
        if allowed {
            return Ok(());
        }
        let message =
            format!("grant refused: {contract} {operation} is outside the granted operation class");
        self.ledger.append(
            LedgerEventKind::GrantRefused {
                contract: contract.to_owned(),
                reason: RefusalReason::ScopeMismatch,
                detail: Some(message.clone()),
            },
            fiber,
        );
        Err(refusal(ErrorCode::EffectFailed, message))
    }

    /// The path-prefix scopes `peer` holds `contract` under — `None` when
    /// ungranted, empty when root — for a provider enforcing its declared
    /// scope type per call (R4: the caller's scope travels with the call).
    #[must_use]
    pub fn scopes(&self, peer: PeerId, contract: &str) -> Option<Vec<String>> {
        self.policy(peer, contract).map(|scope| match scope {
            GrantScope::Paths(paths) => paths,
            _ => Vec::new(),
        })
    }

    /// The typed authority `peer` holds `contract` under (M2-K6), `None`
    /// when ungranted.
    #[must_use]
    pub fn policy(&self, peer: PeerId, contract: &str) -> Option<GrantScope> {
        self.lock()
            .peers
            .get(&peer)
            .and_then(|record| record.grants.get(contract).cloned())
    }

    /// THE grant check, shared by every granted surface — resolve, provide,
    /// listen, and the host-provider dispatch. Authority arrives only as a
    /// grant; an ungranted call is refused and the refusal is a ledger
    /// event, never a default-accept (Law 1 mechanical closure;
    /// constitution 01 §Grants).
    ///
    /// # Errors
    ///
    /// [`ErrorCode::EffectFailed`] with a grant-refused message.
    pub fn check_grant(&self, peer: PeerId, name: &str) -> Result<(), KernelError> {
        let (granted, fiber) = {
            let state = self.lock();
            (state.granted(peer, name), state.fiber_of(peer))
        };
        if granted {
            return Ok(());
        }
        self.ledger.append(
            LedgerEventKind::GrantRefused {
                contract: name.to_owned(),
                reason: RefusalReason::NotGranted,
                detail: None,
            },
            fiber,
        );
        Err(refusal(
            ErrorCode::EffectFailed,
            format!("grant refused: {name}"),
        ))
    }
}
