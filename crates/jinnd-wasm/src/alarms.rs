//! Host-owned alarm machinery for `jinn:clock` (M2-K2): tokio timers in the
//! production host (R1 — no lock is ever held across guest delivery), every
//! wake a ledger event with requesting-fiber attribution (Law 2), and a
//! per-entry resolution floor scoped by the entry's grant so no entry holds
//! a free high-frequency wake hazard (R9; contracts/jinn-clock scope type
//! `rate`). An alarm request is an effect (R5): its undo — the seat's
//! retire, a swap's rebind, a staged unwind — cancels here. Alarms do not
//! survive a kernel restart; plugins re-request on activate (the contract
//! bundle says so, honestly).

use std::sync::Arc;

use jinnd_api::{ErrorCode, FiberId, KernelError, LedgerEventKind};
use tokio::task::AbortHandle;
use tokio::time::{Duration, sleep};

use crate::broker_state::refusal;
use crate::peer::LedgerSink;
use crate::topics::EventTarget;

mod table;

#[cfg(all(test, feature = "loom"))]
mod alarm_model;
#[cfg(all(test, not(feature = "loom")))]
mod tests;

use table::AlarmTable;

/// The clock capability's contract name.
pub const CLOCK_CONTRACT: &str = "jinn:clock";

/// The topic every wake is delivered under (`lifecycle.handle-event`).
pub const WAKE_TOPIC: &str = "jinn:clock/alarm";

/// The default resolution floor for periodic alarms, in milliseconds (R9;
/// documented in `contracts/jinn-clock`): a finer period is refused. An
/// entry's grant may scope its own floor ([`clock_floor`]); this value
/// holds for a `jinn:clock` grant carrying no scope.
pub const DEFAULT_MIN_PERIOD_MS: u64 = 250;

/// The effective `jinn:clock` resolution floor an entry's ADMITTED grants
/// hold (contracts/jinn-clock `[scope]`, type `rate`): the grant's scoped
/// minimum period, or [`DEFAULT_MIN_PERIOD_MS`] for an unscoped grant.
/// Admission already refused every non-`rate` scope on the clock (round-3
/// ruling), so no other shape reaches here. A scope of 0 would nullify the
/// floor into a busy-wake hazard (R9), so the kernel's absolute floor is 1ms.
#[must_use]
pub fn clock_floor(grants: &[crate::grants::Grant]) -> u64 {
    grants
        .iter()
        .find(|grant| grant.contract == CLOCK_CONTRACT)
        .and_then(|grant| match grant.scope {
            Some(crate::grants::ScopeValue::Rate(floor)) => Some(floor),
            _ => None,
        })
        .map_or(DEFAULT_MIN_PERIOD_MS, |floor| floor.max(1))
}

/// The resolution-floor check (R9), run where failing is still allowed: at
/// the live request, and at staging so a swap's commit cannot refuse. The
/// floor is the requesting entry's own ([`clock_floor`]) — grants scope
/// alarm resolution per entry, never assembly-wide.
///
/// # Errors
///
/// A typed refusal for a period finer than the granted floor.
pub fn validate(spec: &AlarmSpec, floor_ms: u64) -> Result<(), KernelError> {
    match spec {
        AlarmSpec::Every(period) if *period < floor_ms => Err(refusal(
            ErrorCode::EffectFailed,
            format!("alarm period {period}ms is finer than the granted floor {floor_ms}ms"),
        )),
        _ => Ok(()),
    }
}

/// One alarm request, as the contract declares it: a single wake at an
/// instant (unix milliseconds, the `now` domain) or a wake every period.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AlarmSpec {
    At(u64),
    Every(u64),
}

impl AlarmSpec {
    /// The request's Law-2 label, shared by registration and withdrawal.
    #[must_use]
    pub fn label(&self) -> String {
        match self {
            Self::At(instant) => format!("alarm at {instant}"),
            Self::Every(period) => format!("alarm every {period}ms"),
        }
    }
}

/// Everything one arm needs — the swap commit's rebind shape.
pub struct ArmRequest {
    pub spec: AlarmSpec,
    pub token: u64,
    pub fiber: Option<FiberId>,
    pub target: Arc<dyn EventTarget>,
}

/// Milliseconds since the Unix epoch — the `jinn:clock` time domain. A
/// clock before the epoch reads 0; ordering authority stays with the
/// ledger's sequence, never with this reading.
#[must_use]
pub fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// The host's alarm registry: one per lane assembly, shared by every seat.
/// Resolution floors are per-entry, carried by each seat's grant scope
/// ([`clock_floor`]) and checked by [`validate`] before an arm.
pub struct Alarms {
    table: AlarmTable<AbortHandle>,
    sink: Arc<dyn LedgerSink>,
}

impl Alarms {
    /// A registry appending its Law-2 wake events to `sink`.
    #[must_use]
    pub fn new(sink: Arc<dyn LedgerSink>) -> Self {
        Self {
            table: AlarmTable::default(),
            sink,
        }
    }

    /// Arms one alarm and returns its id. Infallible by design — the swap
    /// commit's critical section arms staged alarms and may not fail
    /// (round-3 ruling); `validate` ran where failing was allowed. Without
    /// a timer runtime the row is withdrawn and the failure is a ledger
    /// event, never a silently dead alarm (R6).
    pub fn arm(self: &Arc<Self>, request: ArmRequest) -> u64 {
        let id = self.table.arm();
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                let task = runtime.spawn(run(Arc::clone(self), id, request));
                if !self.table.install(id, task.abort_handle()) {
                    // Taken between arm and spawn: the row owner won.
                    task.abort();
                }
            }
            Err(_) => {
                self.table.take(id);
                self.sink.append(
                    LedgerEventKind::ErrorRecorded {
                        error: refusal(
                            ErrorCode::EffectFailed,
                            "no timer runtime to host the alarm".to_owned(),
                        ),
                    },
                    request.fiber,
                );
            }
        }
        id
    }

    /// Cancels one alarm — the effect's undo (R5). Idempotent: an unknown
    /// or already-completed id is `false` and nothing else happens. After
    /// this returns, no wake of `id` is ever appended again (loom-pinned).
    pub fn cancel(&self, id: u64) -> bool {
        match self.table.take(id) {
            Some(handle) => {
                if let Some(abort) = handle {
                    abort.abort();
                }
                true
            }
            None => false,
        }
    }

    /// The swap commit's atomic-enough rebind (R8): cancels the displaced
    /// seat's alarms, then arms the staged seat's requests against the NEW
    /// instance's delivery face, answering the minted ids in request order.
    /// Pure sync bookkeeping — validation already ran at staging.
    pub fn rebind(self: &Arc<Self>, old: &[u64], requests: Vec<ArmRequest>) -> Vec<u64> {
        for id in old {
            self.cancel(*id);
        }
        requests
            .into_iter()
            .map(|request| self.arm(request))
            .collect()
    }
}

/// One alarm's timer task: sleep, claim the wake (the ledger append rides
/// the claim — once cancelled, no append ever lands), then deliver outside
/// every lock (R1). A wake whose delivery fails while the alarm is still
/// live is a recorded, contained plugin failure (R6, R11).
async fn run(alarms: Arc<Alarms>, id: u64, request: ArmRequest) {
    let ArmRequest {
        spec,
        token,
        fiber,
        target,
    } = request;
    match spec {
        AlarmSpec::At(instant) => {
            sleep(Duration::from_millis(instant.saturating_sub(now_unix_ms()))).await;
            wake(&alarms, id, token, fiber, target.as_ref()).await;
            // One-shot completion takes its own row; a racing cancel and
            // this take agree on ownership (loom-pinned).
            alarms.table.take(id);
        }
        AlarmSpec::Every(period) => loop {
            sleep(Duration::from_millis(period)).await;
            if !wake(&alarms, id, token, fiber, target.as_ref()).await {
                break;
            }
        },
    }
}

/// Claims and delivers one wake; `false` when the alarm is gone.
async fn wake(
    alarms: &Alarms,
    id: u64,
    token: u64,
    fiber: Option<FiberId>,
    target: &dyn EventTarget,
) -> bool {
    let claimed = alarms.table.claim_wake(id, || {
        alarms
            .sink
            .append(LedgerEventKind::AlarmWake { alarm: id }, fiber);
    });
    if !claimed {
        return false;
    }
    if let Err(error) = target
        .deliver(token, WAKE_TOPIC, now_unix_ms().to_le_bytes().to_vec())
        .await
        && alarms.table.alive(id)
    {
        // Still live, so this is a real contained wake-handler failure —
        // not the benign race with a teardown that just cancelled us.
        alarms
            .sink
            .append(LedgerEventKind::ErrorRecorded { error }, fiber);
    }
    true
}
