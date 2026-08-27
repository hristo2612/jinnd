//! The swap-target fixture guest: whatever its config, its activation
//! registers one effect and PROVIDES the counter contract — a different
//! contribution than `counter-plugin`'s `plain` mode, so a Mode-1 swap to
//! this artifact proves the STAGED activation's outcome is committed
//! (provision live after commit, withdrawn exactly on dispose). State is
//! the same one counter, handed across swaps via snapshot/restore.

use std::sync::atomic::{AtomicU64, Ordering};

wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
});

use exports::jinn::plugin::lifecycle::{Guest, GuestFault};
use jinn::plugin::{effects, services};

/// The counter contract this fixture provides.
const COUNTER_CONTRACT: &str = "jinn:test/counter";

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn fault(error: jinn::plugin::types::KernelError) -> GuestFault {
    GuestFault::Failed(format!("{error:?}"))
}

struct Fixture;

impl Guest for Fixture {
    fn activate(_config: Vec<u8>) -> Result<(), GuestFault> {
        effects::register("provider-v2 effect", 11).map_err(fault)?;
        services::provide(COUNTER_CONTRACT).map_err(fault)?;
        Ok(())
    }

    fn check(_consumer: u64) -> bool {
        true
    }

    fn undo(token: u64) -> Result<(), GuestFault> {
        // Only the token THIS activation registered is undoable: an inverse
        // charged to another instance's token is a pairing defect.
        if token == 11 {
            Ok(())
        } else {
            Err(GuestFault::Failed(format!(
                "token {token} was not registered by this instance"
            )))
        }
    }

    fn handle_event(_token: u64, _topic: String, payload: Vec<u8>) -> Result<Vec<u8>, GuestFault> {
        Ok(payload)
    }

    fn handle_call(
        caller: u64,
        _contract: String,
        operation: String,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, GuestFault> {
        match operation.as_str() {
            "whoami" => Ok(caller.to_le_bytes().to_vec()),
            "add" => {
                let mut delta = [0u8; 8];
                let len = payload.len().min(8);
                delta[..len].copy_from_slice(&payload[..len]);
                let value = COUNTER
                    .fetch_add(u64::from_le_bytes(delta), Ordering::SeqCst)
                    + u64::from_le_bytes(delta);
                Ok(value.to_le_bytes().to_vec())
            }
            "get" => Ok(COUNTER.load(Ordering::SeqCst).to_le_bytes().to_vec()),
            _ => Err(GuestFault::Failed(format!("unknown operation {operation}"))),
        }
    }

    fn snapshot() -> Vec<u8> {
        COUNTER.load(Ordering::SeqCst).to_le_bytes().to_vec()
    }

    fn restore(blob: Vec<u8>) -> Result<(), GuestFault> {
        if blob.len() != 8 {
            return Err(GuestFault::Failed("unusable handoff blob".into()));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&blob);
        COUNTER.store(u64::from_le_bytes(bytes), Ordering::SeqCst);
        Ok(())
    }
}

export!(Fixture);
