//! Where an activation LANDS (M2-K26 (b)/(c); R8, Law 2). A replacement's
//! staged contribution commits through the Mode-1 primitive in place of
//! the tombstones its suspension left; a first activation, or a failed
//! one, installs uncommitted. Split from `lane.rs` at this seam per R10's
//! per-file cap (PLA-360 round 2) — the behaviour is `lane.rs`'s, moved.

use jinnd_api::{FiberId, LedgerEventKind};

use super::{LaneCore, WasmBody};
use crate::handle::{ActivationOutcome, InstanceHandle, Registration};
use crate::peer::PeerId;
use crate::slot::{SeatState, commit_staged};

impl WasmBody {
    /// With the trail on, each landed guest registration is a ledger event
    /// (Law 2); kernel-side crossings ledgered themselves already (R6).
    pub(super) fn trail(&self, core: &LaneCore, contributed: &ActivationOutcome, fiber: FiberId) {
        if !self.guest_trail {
            return;
        }
        for registration in &contributed.registrations {
            let label = match registration {
                Registration::Effect { label, .. } => label.clone(),
                Registration::Listen(listen) => format!("listen {}", listen.topic),
                // An alarm request IS an effect (M2-K2, R5); its
                // registration is a ledger event like any other.
                Registration::Alarm(alarm) => alarm.label.clone(),
                // The broker ledgered the provide crossing itself
                // (R6); the host provider ledgered its own effect
                // registration with this fiber's attribution (M2-K3;
                // a kernel registration's spawn/listen line, M2-K6).
                Registration::Provision { .. }
                | Registration::Host(_)
                | Registration::Kernel(_) => continue,
            };
            core.sink
                .append(LedgerEventKind::EffectRegistered { label }, Some(fiber));
        }
    }

    /// Lands the activation's contribution in the seat. `committing` is a
    /// replacement whose activation succeeded: Mode 0 gets Mode 1's commit
    /// (R8) — under the topic table's one lock the tombstones go and the
    /// staged listens land; the old subscription's withdrawal row lands
    /// HERE, when it actually ended (Law 2) — replaced, never absent.
    /// Otherwise the seat installs uncommitted: a failed staged activation
    /// lands exactly as a failed first one — its recorded listens and
    /// provisions were never routed (ids absent, so the replay skips them)
    /// and its effects still owe their inverses (I1); the tombstones leave
    /// with the fiber's `Failed` rest (M2-K26 (c)).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn land(
        &self,
        core: &LaneCore,
        handle: InstanceHandle,
        contributed: ActivationOutcome,
        committing: bool,
        fiber: FiberId,
        peer: PeerId,
        context: u64,
    ) {
        let displaced = if committing {
            let entombed = core.topics.entombed(fiber);
            let displaced = commit_staged(
                &self.slot,
                handle,
                contributed,
                &core.broker,
                &core.topics,
                &core.alarms,
                peer,
                Some(fiber),
                context,
                core.sink.as_ref(),
            );
            if self.guest_trail {
                for (_, topic) in entombed {
                    core.sink.append(
                        LedgerEventKind::EffectWithdrawn {
                            label: format!("listen {topic}"),
                            clean: true,
                        },
                        Some(fiber),
                    );
                }
            }
            displaced
        } else {
            self.slot.install(SeatState::live(handle, contributed))
        };
        if let Some(previous) = displaced {
            previous.instance.dispose().await;
        }
    }
}
