//! The demo `scribe`: listens on the `demo:announce` topic and appends every
//! announcement to a journal file through its granted `jinn:fs` host
//! contract. Its config bytes name the journal path (relative to the
//! daemon's data root). The journal write is the revertible effect the
//! keyed-revert demo targets: the daemon's fs provider registers the inverse
//! (restore the prior content) at the point of action (Law 3, R5).

use std::sync::Mutex;

wit_bindgen::generate!({
    path: "../../../wit",
    world: "plugin",
});

use exports::jinn::plugin::lifecycle::{Guest, GuestFault};
use jinn::plugin::{effects, events, fs};

const ANNOUNCE_TOPIC: &str = "demo:announce";
const LISTEN_TOKEN: u64 = 7;

static JOURNAL: Mutex<String> = Mutex::new(String::new());

fn fault(error: jinn::plugin::types::KernelError) -> GuestFault {
    GuestFault::Failed(format!("{error:?}"))
}

struct Scribe;

impl Guest for Scribe {
    fn activate(config: Vec<u8>) -> Result<(), GuestFault> {
        let journal = String::from_utf8_lossy(&config).into_owned();
        *JOURNAL.lock().unwrap() = journal;
        effects::register("scribe on duty", 1).map_err(fault)?;
        events::listen(ANNOUNCE_TOPIC, LISTEN_TOKEN).map_err(fault)?;
        Ok(())
    }

    fn check(_consumer: u64) -> bool {
        true
    }

    fn undo(_token: u64) -> Result<(), GuestFault> {
        Ok(())
    }

    fn handle_event(_token: u64, _topic: String, payload: Vec<u8>) -> Result<Vec<u8>, GuestFault> {
        let journal = JOURNAL.lock().unwrap().clone();
        let prior = fs::read(&journal).unwrap_or_default();
        let mut next = prior;
        next.extend_from_slice(&payload);
        next.push(b'\n');
        fs::write(&journal, &next).map_err(fault)?;
        Ok(payload)
    }

    fn handle_call(
        _caller: u64,
        _contract: String,
        operation: String,
        _payload: Vec<u8>,
    ) -> Result<Vec<u8>, GuestFault> {
        Err(GuestFault::Failed(format!("unknown operation {operation}")))
    }

    fn snapshot() -> Vec<u8> {
        Vec::new()
    }

    fn restore(_blob: Vec<u8>) -> Result<(), GuestFault> {
        Ok(())
    }
}

export!(Scribe);
