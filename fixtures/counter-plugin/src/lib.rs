//! The fixture guest. Its `activate` mode comes from the entry config bytes
//! (UTF-8): `plain` registers one effect; `provider` also provides the
//! counter contract; `picky` provides it with a per-consumer vitality
//! opinion; `caller` resolves and calls a granted contract; `ungranted`
//! asserts the broker refuses an ungranted resolve; `trap` panics; `spin`
//! never returns; `fs-bundle` / `fs-bundle-denied` / `fs-scope-probe` drive
//! the `jinn:fs` bundle under a root, no, and a path-prefix grant, and
//! `fs-on-wake` appends from a wake handler (M2-K3);
//! `grumpy-undo` registers an effect whose inverse fails
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
use jinn::plugin::{clock, effects, fs, services};

/// The counter contract this fixture provides.
const COUNTER_CONTRACT: &str = "jinn:test/counter";
/// A contract the fixture calls when activated in `caller` mode.
const GREETER_CONTRACT: &str = "jinn:test/greeter";
/// A contract nobody grants; `ungranted` mode asserts its refusal.
const SECRET_CONTRACT: &str = "jinn:test/secret";
/// The topic `listener` mode subscribes to (granted) and `eavesdrop` mode is
/// refused (ungranted).
const TOPIC: &str = "jinn:test/topic";
/// Where `jinn:clock` wakes arrive (wit/plugin.wit `interface clock`).
const WAKE_TOPIC: &str = "jinn:clock/alarm";
/// The token the clock modes request their alarms under.
const WAKE_TOKEN: u64 = 11;

static COUNTER: AtomicU64 = AtomicU64::new(0);
static PICKY: AtomicBool = AtomicBool::new(false);
static STASH: Mutex<Vec<u8>> = Mutex::new(Vec::new());
static MODE: Mutex<String> = Mutex::new(String::new());

fn fault(error: jinn::plugin::types::KernelError) -> GuestFault {
    GuestFault::Failed(format!("{error:?}"))
}

fn fs_fault(error: fs::FsError) -> GuestFault {
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
                let answer = fs::read("/probe").map_err(fs_fault)?;
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
            // The full jinn:fs bundle (M2-K3): keyed write, append, meta,
            // list, remove, and the typed not-found; stashes the listing.
            "fs-bundle" => {
                fs::write("/log/a.txt", b"one\n", "k-a1").map_err(fs_fault)?;
                fs::append("/log/a.txt", b"two\n", "k-a2").map_err(fs_fault)?;
                fs::write("/log/b.txt", b"bee", "k-b1").map_err(fs_fault)?;
                let meta = fs::meta("/log/a.txt").map_err(fs_fault)?;
                if meta.size != 8 || meta.is_dir || meta.modified_ms == 0 {
                    return Err(GuestFault::Failed(format!("meta misdescribes: {meta:?}")));
                }
                let listed = fs::list("/log").map_err(fs_fault)?;
                let names: Vec<String> = listed.iter().map(|m| m.path.clone()).collect();
                fs::remove("/log/b.txt", "k-b2").map_err(fs_fault)?;
                for absent in [fs::read("/log/b.txt").map(|_| ()), fs::meta("/missing").map(|_| ())] {
                    match absent {
                        Err(fs::FsError::NotFound) => {}
                        other => {
                            return Err(GuestFault::Failed(format!(
                                "absence was not the typed not-found: {other:?}"
                            )))
                        }
                    }
                }
                *STASH.lock().unwrap() = names.join(",").into_bytes();
                Ok(())
            }
            // Every new op refuses without a grant (M2-K3, red-first).
            "fs-bundle-denied" => {
                let refused = fs::list("/").is_err()
                    && fs::meta("/x").is_err()
                    && fs::append("/x", b"y", "").is_err()
                    && fs::remove("/x", "").is_err();
                if refused {
                    Ok(())
                } else {
                    Err(GuestFault::Failed(
                        "an ungranted fs bundle op was not refused".into(),
                    ))
                }
            }
            // A path-prefix grant of `/log` (M2-K3 round 2): the scoped
            // write admits, the write beside the scope is the typed denial.
            "fs-scope-probe" => {
                fs::write("/log/in.txt", b"in", "").map_err(fs_fault)?;
                match fs::write("/other/out.txt", b"out", "") {
                    Err(fs::FsError::Denied) => Ok(()),
                    other => Err(GuestFault::Failed(format!(
                        "a write beside the scope was not the typed denial: {other:?}"
                    ))),
                }
            }
            // Reads the granted clock and stashes the 8-byte LE instant.
            "clock-now" => {
                let reading = clock::now().map_err(fault)?;
                *STASH.lock().unwrap() = reading.to_le_bytes().to_vec();
                Ok(())
            }
            // Provides the counter, then asks for a periodic wake at the
            // default floor; each typed wake bumps the counter (see
            // `handle_event`), so the host can watch time arriving.
            "clock-alarm" => {
                services::provide(COUNTER_CONTRACT).map_err(fault)?;
                clock::alarm_every(250, WAKE_TOKEN).map_err(fault)?;
                Ok(())
            }
            // A one-shot wake that appends to a guest-kept log from
            // `handle_event` (M2-K3 round 2): an effect registered AFTER
            // activation must still join the fiber's journal.
            // The FINDINGS #15 shape (M2-K4): the handler appends, dawdles
            // mid-tick, then appends again — a dispose landing in between
            // must seal the journal against the second append.
            "fs-on-wake" | "fs-on-wake-busy" => {
                clock::alarm_at(0, WAKE_TOKEN).map_err(fault)?;
                Ok(())
            }
            // A one-shot at instant 0 — already past, wakes once immediately.
            "clock-at" => {
                services::provide(COUNTER_CONTRACT).map_err(fault)?;
                clock::alarm_at(0, WAKE_TOKEN).map_err(fault)?;
                Ok(())
            }
            "clock-denied" => match (clock::now(), clock::alarm_every(300, WAKE_TOKEN)) {
                (Err(_), Err(_)) => Ok(()),
                _ => Err(GuestFault::Failed(
                    "an ungranted clock call was not refused".into(),
                )),
            },
            "clock-fast" => match clock::alarm_every(1, WAKE_TOKEN) {
                Err(_) => Ok(()),
                Ok(_) => Err(GuestFault::Failed(
                    "a period finer than the floor was not refused".into(),
                )),
            },
            // One bus emit through the daemon path — the DispatchTrace probe.
            "emitter" => {
                jinn::plugin::events::emit(
                    TOPIC,
                    jinn::plugin::types::DispatchMode::Emit,
                    &jinn::plugin::types::Selector::All,
                    b"ping",
                )
                .map_err(fault)?;
                Ok(())
            }
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

    fn handle_event(token: u64, topic: String, payload: Vec<u8>) -> Result<Vec<u8>, GuestFault> {
        // A typed clock wake: right token, right topic, 8-byte LE instant —
        // anything else on the wake topic is a contract violation.
        if topic == WAKE_TOPIC {
            if token != WAKE_TOKEN || payload.len() != 8 {
                return Err(GuestFault::Failed("a malformed wake arrived".into()));
            }
            COUNTER.fetch_add(1, Ordering::SeqCst);
            let mode = MODE.lock().unwrap().clone();
            if mode == "fs-on-wake" {
                fs::append("/wakes.log", b"tick\n", "").map_err(fs_fault)?;
            }
            if mode == "fs-on-wake-busy" {
                fs::append("/wakes.log", b"tick\n", "").map_err(fs_fault)?;
                let started = clock::now().map_err(fault)?;
                while clock::now().map_err(fault)? < started + 600 {
                    std::hint::black_box(());
                }
                fs::append("/wakes.log", b"tock\n", "").map_err(fs_fault)?;
            }
            return Ok(Vec::new());
        }
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
