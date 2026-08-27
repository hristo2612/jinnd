//! The demo `clock` provider: one monotonic tick behind the `demo:clock`
//! contract. `now` answers the next tick; `version` answers the artifact
//! generation (1, or 2 with the `v2` feature) — a byte-distinct healthy
//! Mode-1 swap target. The counter is the handoff state: `snapshot` offers
//! it, `restore` adopts it, so a healthy swap continues the tick where the
//! old instance left it. The `broken-restore` feature refuses every handoff,
//! failing a swap's health gate on demand (the rollback demonstrator).

use std::sync::atomic::{AtomicU64, Ordering};

wit_bindgen::generate!({
    path: "../../../wit",
    world: "plugin",
});

use exports::jinn::plugin::lifecycle::{Guest, GuestFault};
use jinn::plugin::{effects, services};

const CLOCK_CONTRACT: &str = "demo:clock";
const VERSION: u64 = if cfg!(feature = "v2") { 2 } else { 1 };

static TICK: AtomicU64 = AtomicU64::new(0);

fn fault(error: jinn::plugin::types::KernelError) -> GuestFault {
    GuestFault::Failed(format!("{error:?}"))
}

struct Clock;

impl Guest for Clock {
    fn activate(_config: Vec<u8>) -> Result<(), GuestFault> {
        effects::register("clock ticking", 1).map_err(fault)?;
        services::provide(CLOCK_CONTRACT).map_err(fault)?;
        Ok(())
    }

    fn check(_consumer: u64) -> bool {
        true
    }

    fn undo(_token: u64) -> Result<(), GuestFault> {
        Ok(())
    }

    fn handle_event(_token: u64, _topic: String, payload: Vec<u8>) -> Result<Vec<u8>, GuestFault> {
        Ok(payload)
    }

    fn handle_call(
        _caller: u64,
        _contract: String,
        operation: String,
        _payload: Vec<u8>,
    ) -> Result<Vec<u8>, GuestFault> {
        match operation.as_str() {
            "now" => Ok((TICK.fetch_add(1, Ordering::SeqCst) + 1).to_le_bytes().to_vec()),
            "version" => Ok(VERSION.to_le_bytes().to_vec()),
            other => Err(GuestFault::Failed(format!("unknown operation {other}"))),
        }
    }

    fn snapshot() -> Vec<u8> {
        TICK.load(Ordering::SeqCst).to_le_bytes().to_vec()
    }

    fn restore(blob: Vec<u8>) -> Result<(), GuestFault> {
        if cfg!(feature = "broken-restore") {
            return Err(GuestFault::Failed("broken clock refused the handoff".into()));
        }
        if blob.len() != 8 {
            return Err(GuestFault::Failed("unusable handoff blob".into()));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&blob);
        TICK.store(u64::from_le_bytes(bytes), Ordering::SeqCst);
        Ok(())
    }
}

export!(Clock);
