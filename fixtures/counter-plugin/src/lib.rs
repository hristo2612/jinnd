//! The fixture guest. Its `activate` mode comes from the entry config bytes
//! (UTF-8): `plain` registers one effect; `provider` also provides the
//! counter contract; `picky` provides it with a per-consumer vitality
//! opinion; `caller` resolves and calls a granted contract; `ungranted`
//! asserts the broker refuses an ungranted resolve; `trap` panics; `spin`
//! never returns; `fs-bundle` / `fs-bundle-denied` / `fs-scope-probe` drive
//! the `jinn:fs` bundle under a root, no, and a path-prefix grant, and
//! `fs-on-wake` appends from a wake handler (M2-K3); the `proc-*` and
//! `net-*` modes drive the `jinn:process` / `jinn:net` long-lived editions
//! (M2-K6; `mode:arg` carries a path, a port, or an address);
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
use jinn::plugin::{clock, effects, fs, net, process, services};

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
/// The `net-echo` listener and its live connections (M2-K6).
static LISTENER: AtomicU64 = AtomicU64::new(0);
static CONNS: Mutex<Vec<u64>> = Mutex::new(Vec::new());

fn fault(error: jinn::plugin::types::KernelError) -> GuestFault {
    GuestFault::Failed(format!("{error:?}"))
}

fn fs_fault(error: fs::FsError) -> GuestFault {
    GuestFault::Failed(format!("{error:?}"))
}

/// Spins until `condition` answers `Some`, or the clock says `budget_ms`
/// passed (a guest has no sleep; polling is the v0.1 wake shape, M2-K6).
fn poll<T>(budget_ms: u64, mut condition: impl FnMut() -> Result<Option<T>, GuestFault>) -> Result<T, GuestFault> {
    let started = clock::now().map_err(fault)?;
    loop {
        if let Some(found) = condition()? {
            return Ok(found);
        }
        if clock::now().map_err(fault)? > started + budget_ms {
            return Err(GuestFault::Failed("polling budget exhausted".into()));
        }
    }
}

/// Drains one child stream until EOF (M2-K6), bounded by the clock.
fn drain(handle: u64, which: process::ChildStream, budget_ms: u64) -> Result<Vec<u8>, GuestFault> {
    let mut collected = Vec::new();
    poll(budget_ms, || match process::read(handle, which, 4096).map_err(fault)? {
        process::ReadResult::Data(bytes) => {
            collected.extend(bytes);
            Ok(None)
        }
        process::ReadResult::WouldBlock => Ok(None),
        process::ReadResult::Eof => Ok(Some(())),
    })?;
    Ok(collected)
}

/// The M2-K6 process modes: `proc-echo` round-trips stdin→stdout through
/// `/bin/cat`; `proc-sleeper` holds a long-lived child; `proc-kill`
/// terminates one and records its status; `proc-env` records what the
/// child's environment holds; `proc-run` is the one-shot; `proc-denied`
/// and `proc-escape:<path>` assert refusals.
fn process_mode(mode: &str, arg: &str) -> Result<(), GuestFault> {
    match mode {
        "proc-echo" => {
            let child = process::spawn("/bin/cat", &[], None, &[]).map_err(fault)?;
            let accepted = process::write_stdin(child, b"hello\n").map_err(fault)?;
            if accepted != 6 {
                return Err(GuestFault::Failed(format!("stdin accepted {accepted}")));
            }
            process::close_stdin(child).map_err(fault)?;
            let echoed = drain(child, process::ChildStream::Stdout, 3000)?;
            let status = poll(3000, || match process::wait(child, 1000).map_err(fault)? {
                process::WaitResult::Exited(code) => Ok(Some(code)),
                process::WaitResult::Running => Ok(None),
            })?;
            if status != 0 {
                return Err(GuestFault::Failed(format!("cat exited {status}")));
            }
            fs::write("/proc-echo.out", &echoed, "").map_err(fs_fault)
        }
        "proc-sleeper" => {
            process::spawn("/bin/sleep", &["30".to_owned()], None, &[]).map_err(fault)?;
            Ok(())
        }
        "proc-kill" => {
            let child = process::spawn("/bin/sleep", &["30".to_owned()], None, &[]).map_err(fault)?;
            process::kill(child, process::Signal::Terminate).map_err(fault)?;
            let status = poll(3000, || match process::wait(child, 1000).map_err(fault)? {
                process::WaitResult::Exited(code) => Ok(Some(code)),
                process::WaitResult::Running => Ok(None),
            })?;
            fs::write("/proc-kill.out", status.to_string().as_bytes(), "").map_err(fs_fault)
        }
        "proc-env" => {
            let env = vec![("JINND_GUEST_VAR".to_owned(), "from-guest".to_owned())];
            let child = process::spawn("/usr/bin/env", &[], None, &env).map_err(fault)?;
            let listing = drain(child, process::ChildStream::Stdout, 3000)?;
            fs::write("/proc-env.out", &listing, "").map_err(fs_fault)
        }
        "proc-run" => {
            let out = process::run("/bin/echo", &["hi".to_owned()]).map_err(fault)?;
            fs::write("/proc-run.out", &out, "").map_err(fs_fault)
        }
        "proc-denied" => match process::spawn("/bin/cat", &[], None, &[]) {
            Err(_) => Ok(()),
            Ok(_) => Err(GuestFault::Failed("an ungranted spawn was not refused".into())),
        },
        "proc-escape" => match process::spawn(arg, &[], None, &[]) {
            Err(jinn::plugin::types::KernelError::GrantRefused(_)) => Ok(()),
            other => Err(GuestFault::Failed(format!(
                "a link out of the allowlist was not the typed refusal: {other:?}"
            ))),
        },
        other => Err(GuestFault::Failed(format!("unknown process mode {other}"))),
    }
}

/// The M2-K6 net modes: `net-echo:<port>` listens on loopback and echoes
/// from its wake handler; `net-refused:<addr>` asserts the bind refuses.
fn net_mode(mode: &str, arg: &str) -> Result<(), GuestFault> {
    match mode {
        "net-echo" => {
            let listener = net::listen(&format!("127.0.0.1:{arg}")).map_err(fault)?;
            LISTENER.store(listener, Ordering::SeqCst);
            clock::alarm_every(250, WAKE_TOKEN).map_err(fault)?;
            Ok(())
        }
        "net-refused" => match net::listen(arg) {
            Err(jinn::plugin::types::KernelError::GrantRefused(_)) => Ok(()),
            other => Err(GuestFault::Failed(format!(
                "the bind was not the typed refusal: {other:?}"
            ))),
        },
        other => Err(GuestFault::Failed(format!("unknown net mode {other}"))),
    }
}

/// One echo tick: accept what is pending, echo what each connection sent,
/// close what the peer closed (M2-K6 polling shape).
fn echo_tick() -> Result<(), GuestFault> {
    let listener = LISTENER.load(Ordering::SeqCst);
    let mut conns = CONNS.lock().unwrap();
    while let net::AcceptResult::Connection(conn) = net::accept(listener).map_err(fault)? {
        conns.push(conn);
    }
    let mut closed = Vec::new();
    for &conn in conns.iter() {
        match net::read(conn, 4096).map_err(fault)? {
            net::ReadResult::Data(bytes) => {
                let mut offered = 0;
                while offered < bytes.len() {
                    offered += net::write(conn, &bytes[offered..]).map_err(fault)? as usize;
                }
            }
            net::ReadResult::Eof => {
                net::close(conn).map_err(fault)?;
                closed.push(conn);
            }
            net::ReadResult::WouldBlock => {}
        }
    }
    conns.retain(|conn| !closed.contains(conn));
    Ok(())
}

struct Fixture;

impl Guest for Fixture {
    fn activate(config: Vec<u8>) -> Result<(), GuestFault> {
        let mode = String::from_utf8_lossy(&config).into_owned();
        *MODE.lock().unwrap() = mode.clone();
        let (mode, arg) = mode.split_once(':').unwrap_or((mode.as_str(), ""));
        if mode.starts_with("proc-") {
            return process_mode(mode, arg);
        }
        if mode.starts_with("net-") {
            return net_mode(mode, arg);
        }
        match mode {
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
            if mode.starts_with("net-echo") {
                echo_tick()?;
            }
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
