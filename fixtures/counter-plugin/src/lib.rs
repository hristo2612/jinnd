//! The fixture guest. Its `activate` mode comes from the entry config bytes
//! (UTF-8): `plain` registers one effect; `provider` also provides the
//! counter contract; `picky` provides it with a per-consumer vitality
//! opinion; `caller` resolves and calls a granted contract; `ungranted`
//! asserts the broker refuses an ungranted resolve; `trap` panics; `spin`
//! never returns; `grumpy-undo` registers an effect whose inverse fails
//! loudly (it proves an undo replay RAN); `flaky-restore` refuses every
//! handoff (it fails a swap's health gate on demand); `interleave` registers
//! effects, a provision, and a listener deliberately interleaved (the LIFO
//! teardown probe). State is one counter, handed across Mode-1 swaps via
//! snapshot/restore.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
});

use exports::jinn::plugin::lifecycle::{Guest, GuestFault};
use jinn::plugin::{effects, fs, services};

/// The counter contract this fixture provides.
const COUNTER_CONTRACT: &str = "jinn:test/counter";
/// A contract the fixture calls when activated in `caller` mode.
const GREETER_CONTRACT: &str = "jinn:test/greeter";
/// A contract nobody grants; `ungranted` mode asserts its refusal.
const SECRET_CONTRACT: &str = "jinn:test/secret";
/// The topic `listener` mode subscribes to (granted) and `eavesdrop` mode is
/// refused (ungranted).
const TOPIC: &str = "jinn:test/topic";

static COUNTER: AtomicU64 = AtomicU64::new(0);
static PICKY: AtomicBool = AtomicBool::new(false);
static STASH: Mutex<Vec<u8>> = Mutex::new(Vec::new());
static MODE: Mutex<String> = Mutex::new(String::new());

fn fault(error: jinn::plugin::types::KernelError) -> GuestFault {
    GuestFault::Failed(format!("{error:?}"))
}

struct Fixture;

impl Guest for Fixture {
    fn activate(config: Vec<u8>) -> Result<(), GuestFault> {
        let mode = String::from_utf8_lossy(&config).into_owned();
        *MODE.lock().unwrap() = mode.clone();
        match mode.as_str() {
            "trap" => panic!("fixture trap mode"),
            "spin" => loop {
                std::hint::black_box(());
            },
            "caller" => {
                let handle = services::resolve(GREETER_CONTRACT).map_err(fault)?;
                let answer =
                    services::call(handle, "greet", b"from-guest").map_err(fault)?;
                *STASH.lock().unwrap() = answer;
                effects::register("caller effect", 1).map_err(fault)?;
                Ok(())
            }
            "ungranted" => match services::resolve(SECRET_CONTRACT) {
                Err(_) => Ok(()),
                Ok(_) => Err(GuestFault::Failed(
                    "an ungranted resolve was not refused".into(),
                )),
            },
            "listener" => {
                jinn::plugin::events::listen(TOPIC, 7).map_err(fault)?;
                Ok(())
            }
            "eavesdrop" => match jinn::plugin::events::listen(TOPIC, 7) {
                Err(_) => Ok(()),
                Ok(_) => Err(GuestFault::Failed(
                    "an ungranted listen was not refused".into(),
                )),
            },
            "fs" => {
                let answer = fs::read("/probe").map_err(fault)?;
                *STASH.lock().unwrap() = answer;
                Ok(())
            }
            // Registrations of every category, deliberately interleaved: the
            // seat's teardown must replay this exact journal in reverse.
            "interleave" => {
                effects::register("first effect", 1).map_err(fault)?;
                services::provide(COUNTER_CONTRACT).map_err(fault)?;
                jinn::plugin::events::listen(TOPIC, 7).map_err(fault)?;
                effects::register("second effect", 2).map_err(fault)?;
                Ok(())
            }
            "fs-denied" => match fs::read("/probe") {
                Err(_) => Ok(()),
                Ok(_) => Err(GuestFault::Failed(
                    "an ungranted host-fs read was not refused".into(),
                )),
            },
            other => {
                effects::register("fixture effect", 1).map_err(fault)?;
                if other == "provider" || other == "picky" {
                    services::provide(COUNTER_CONTRACT).map_err(fault)?;
                }
                if other == "picky" {
                    PICKY.store(true, Ordering::SeqCst);
                }
                Ok(())
            }
        }
    }

    fn check(consumer: u64) -> bool {
        if PICKY.load(Ordering::SeqCst) {
            consumer % 2 == 0
        } else {
            true
        }
    }

    fn undo(token: u64) -> Result<(), GuestFault> {
        // The grumpy inverse fails loudly: a host that replays staged
        // effects on discard observes this contained failure, a host that
        // raw-disposes never runs it.
        if *MODE.lock().unwrap() == "grumpy-undo" {
            return Err(GuestFault::Failed(format!("grumpy undo ran (token {token})")));
        }
        Ok(())
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
            // The caller scope the broker delivered with this call (R4).
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
            "stash" => Ok(STASH.lock().unwrap().clone()),
            _ => Err(GuestFault::Failed(format!("unknown operation {operation}"))),
        }
    }

    fn snapshot() -> Vec<u8> {
        COUNTER.load(Ordering::SeqCst).to_le_bytes().to_vec()
    }

    fn restore(blob: Vec<u8>) -> Result<(), GuestFault> {
        if *MODE.lock().unwrap() == "flaky-restore" {
            return Err(GuestFault::Failed("flaky restore refused the handoff".into()));
        }
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
