//! The demo `greeter`: consumes `demo:clock` over the broker, provides
//! `demo:greeting`, and announces every greeting on the `demo:announce`
//! topic. Its config bytes are the name it greets — editing that one entry
//! in the profile restarts exactly this fiber (the reconcile-by-id demo) and
//! the fresh activation greets again, tick advanced.

use std::sync::Mutex;

wit_bindgen::generate!({
    path: "../../../wit",
    world: "plugin",
});

use exports::jinn::plugin::lifecycle::{Guest, GuestFault};
use jinn::plugin::types::{DispatchMode, Selector};
use jinn::plugin::{effects, events, services};

const GREETING_CONTRACT: &str = "demo:greeting";
const CLOCK_CONTRACT: &str = "demo:clock";
const ANNOUNCE_TOPIC: &str = "demo:announce";

static NAME: Mutex<String> = Mutex::new(String::new());

fn fault(error: jinn::plugin::types::KernelError) -> GuestFault {
    GuestFault::Failed(format!("{error:?}"))
}

/// One greeting: resolve the clock, take the next tick, announce the result.
fn greet(name: &str) -> Result<Vec<u8>, GuestFault> {
    let clock = services::resolve(CLOCK_CONTRACT).map_err(fault)?;
    let answer = services::call(clock, "now", &[]).map_err(fault)?;
    let mut tick_bytes = [0u8; 8];
    let len = answer.len().min(8);
    tick_bytes[..len].copy_from_slice(&answer[..len]);
    let tick = u64::from_le_bytes(tick_bytes);
    let greeting = format!("hello, {name} (tick {tick})");
    events::emit(
        ANNOUNCE_TOPIC,
        DispatchMode::Emit,
        &Selector::All,
        greeting.as_bytes(),
    )
    .map_err(fault)?;
    Ok(greeting.into_bytes())
}

struct Greeter;

impl Guest for Greeter {
    fn activate(config: Vec<u8>) -> Result<(), GuestFault> {
        let name = String::from_utf8_lossy(&config).into_owned();
        *NAME.lock().unwrap() = name.clone();
        effects::register("greeter ready", 1).map_err(fault)?;
        services::provide(GREETING_CONTRACT).map_err(fault)?;
        // Siblings activate concurrently; at first boot the clock may not
        // have provided yet (guest-to-guest readiness over the dynamic
        // string lane is post-M1 kernel surface). Greet opportunistically:
        // every later activation — and every `greet` call — has the clock.
        let _ = greet(&name);
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
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, GuestFault> {
        match operation.as_str() {
            "greet" => {
                let name = if payload.is_empty() {
                    NAME.lock().unwrap().clone()
                } else {
                    String::from_utf8_lossy(&payload).into_owned()
                };
                greet(&name)
            }
            other => Err(GuestFault::Failed(format!("unknown operation {other}"))),
        }
    }

    fn snapshot() -> Vec<u8> {
        Vec::new()
    }

    fn restore(_blob: Vec<u8>) -> Result<(), GuestFault> {
        Ok(())
    }
}

export!(Greeter);
