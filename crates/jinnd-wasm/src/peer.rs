//! The broker's transport seam (decision log 2026-08-25): peers, handles,
//! and the ledger sink — the vocabulary every transport shares. The wasm
//! instance implements [`Peer`] over its supervisor channel; the harness
//! implements it natively; a Tier B process would implement it over a
//! socket.

use jinnd_api::{EntryId, FiberId, KernelFuture, LedgerEventKind};

/// A caller/provider identity at the broker boundary.
pub type PeerId = u64;

/// An opaque caller-scoped capability handle (R4): minted by the broker on a
/// granted resolve, valid only for its owner.
pub type HandleId = u64;

/// Where broker crossings land (R6, Law 2). The harness lane appends into the
/// kernel ledger; unit tests capture events in memory. The sink is ordered:
/// the append happens before the dispatch it describes.
pub trait LedgerSink: Send + Sync + 'static {
    fn append(&self, kind: LedgerEventKind, fiber: Option<FiberId>);

    /// An append attributed to a profile ENTRY as well (M2-K4): the
    /// entry's journal outlives its fibers, so a withdrawal from it is
    /// recorded under the entry. Sinks that carry no entry column keep the
    /// fiber attribution alone.
    fn append_for(&self, kind: LedgerEventKind, _entry: Option<EntryId>, fiber: Option<FiberId>) {
        self.append(kind, fiber);
    }
}

/// A contract call answered by a peer — the transport seam. Implementations
/// exist for the Tier A instance (a channel into its supervisor task) and for
/// native harness providers; a future Tier B peer is a socket, same trait.
pub trait Peer: Send + Sync + 'static {
    /// Answers one operation of a contract this peer provides. `caller` is
    /// the calling peer's identity (R4): a resolved handle pairs the
    /// implementation with the caller's scope, so every dispatch tells the
    /// provider who is calling, by construction.
    fn call(
        &self,
        caller: PeerId,
        contract: &str,
        operation: &str,
        payload: Vec<u8>,
    ) -> KernelFuture<'static, Vec<u8>>;

    /// Answers one per-consumer vitality check (decision log 2026-08-25, C3):
    /// evaluated per consumer, per notify — never a cached provider-side
    /// bool. The default is vital: a provider with no per-consumer opinion
    /// answers every consumer alike.
    fn check(&self, consumer: PeerId) -> KernelFuture<'static, bool> {
        let _ = consumer;
        Box::pin(async { Ok(true) })
    }

    /// Withdraws one host-side effect this peer registered on a caller's
    /// behalf (M2-K3 round 2; R5): the caller seat's journal replays it
    /// LIFO through the broker at teardown, exactly like a guest inverse.
    /// The default refuses — a peer that registers no host effects has
    /// nothing to withdraw.
    fn withdraw(&self, effect: u64) -> KernelFuture<'static, ()> {
        Box::pin(async move {
            Err(jinnd_api::KernelError {
                code: jinnd_api::ErrorCode::EffectFailed,
                message: format!("this provider registers no host effects ({effect})"),
                fiber: None,
            })
        })
    }
}
