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
//! the `notify-*` modes drive the M2-K9 restart-window shape (#31);
//! the `cycle-*` modes drive the M2-K10 wait-cycle shape (#32);
//! `grumpy-undo` registers an effect whose inverse fails
//! loudly (it proves an undo replay RAN); `flaky-restore` refuses every
//! handoff (it fails a swap's health gate on demand); `interleave` registers
//! effects, a provision, and a listener deliberately interleaved (the LIFO
//! teardown probe). State is one counter, handed across Mode-1 swaps via
//! snapshot/restore.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

wit_bindgen::generate!({
    path: "../../wit",
    world: "plugin",
});

use exports::jinn::plugin::lifecycle::{Guest, GuestFault};
use jinn::plugin::{clock, effects, fs, keystore, net, process, services};

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
/// Where `jinn:net` readiness wakes arrive (M2-K7; the token is the handle).
const READABLE_TOPIC: &str = "jinn:net/readable";
/// The token the clock modes request their alarms under.
const WAKE_TOKEN: u64 = 11;
/// The M2-K9 notice topic: a settings provider's `changed` notice, which
/// its consumers must answer (a reply-expecting dispatch).
const CHANGED_TOPIC: &str = "jinn:test/settings-changed";
/// The token `notify-consumer` listens for the notice under.
const NOTICE_TOKEN: u64 = 21;
/// The token `notify-consumer`'s effect answers to: its seat carries a
/// real journal, so its restart is a real seat replacement.
const CONSUMER_UNDO_TOKEN: u64 = 22;
/// The M2-K10 notice topic: kept apart from the K9 one so the two shapes
/// can never be mistaken for each other in a ledger or a log.
const CYCLE_TOPIC: &str = "jinn:test/cycle-notice";
/// The token the `cycle-*` listeners subscribe under.
const CYCLE_TOKEN: u64 = 31;
/// The kernel's RESERVED lifecycle topic (M2-K13, harness #40/#41): the
/// kernel publishes every committed fiber transition here. Listening needs
/// the `jinn:introspect` grant; no guest may emit on it.
const TRANSITIONS_TOPIC: &str = "jinn:introspect/transitions";
/// The token the `lifecycle-*` modes subscribe to the reserved topic under.
const LIFECYCLE_TOKEN: u64 = 41;
/// The ledger contract the lifecycle listener consults from inside a
/// delivery (the ordering probe).
const LEDGER_CONTRACT: &str = "jinn:ledger";

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

fn process_fault(error: process::ProcessError) -> GuestFault {
    GuestFault::Failed(format!("{error:?}"))
}

fn net_fault(error: net::NetError) -> GuestFault {
    GuestFault::Failed(format!("{error:?}"))
}

fn keystore_fault(error: keystore::KeystoreError) -> GuestFault {
    GuestFault::Failed(format!("{error:?}"))
}

/// The secret the keystore modes store: the daemon test greps the ledger
/// file, the sealed store, and the inverses for these bytes (zero hits).
const SECRET: &[u8] = b"sk-live-0xDEADBEEF-fixture-secret";
/// The same secret as text, for the M2-K14 credential-in-a-call probe.
const SECRET_TEXT: &str = "sk-live-0xDEADBEEF-fixture-secret";

/// The M2-K8 keystore modes: `keystore` drives the whole bundle under an
/// `engines/` prefix grant and leaves one key in place for the dispose
/// probe; `keystore-readonly` asserts the `ops: [get, list]` attenuation.
fn keystore_mode(mode: &str) -> Result<(), GuestFault> {
    match mode {
        "keystore" => {
            keystore::put("engines/openai", SECRET).map_err(keystore_fault)?;
            if keystore::get("engines/openai").map_err(keystore_fault)? != SECRET {
                return Err(GuestFault::Failed("the value did not round-trip".into()));
            }
            let names = keystore::list().map_err(keystore_fault)?;
            match keystore::put("smtp/password", b"beside-the-prefix") {
                Err(keystore::KeystoreError::Denied(_)) => {}
                other => {
                    return Err(GuestFault::Failed(format!(
                        "a key beside the granted prefix was not denied: {other:?}"
                    )))
                }
            }
            keystore::put("engines/kept", b"kept-0xCAFEBABE-value").map_err(keystore_fault)?;
            keystore::delete("engines/openai").map_err(keystore_fault)?;
            match keystore::get("engines/openai") {
                Err(keystore::KeystoreError::NotFound) => {}
                other => {
                    return Err(GuestFault::Failed(format!(
                        "absence was not the typed not-found: {other:?}"
                    )))
                }
            }
            fs::write("/keystore.out", names.join(",").as_bytes(), "").map_err(fs_fault)
        }
        "keystore-readonly" => {
            match keystore::get("engines/none") {
                Err(keystore::KeystoreError::NotFound) => {}
                other => return Err(GuestFault::Failed(format!("get: {other:?}"))),
            }
            let denied = matches!(
                keystore::put("engines/x", b"v"),
                Err(keystore::KeystoreError::Denied(_))
            ) && matches!(
                keystore::delete("engines/x"),
                Err(keystore::KeystoreError::Denied(_))
            );
            if denied {
                Ok(())
            } else {
                Err(GuestFault::Failed(
                    "a mutation under a read-only keystore grant was not denied".into(),
                ))
            }
        }
        other => Err(GuestFault::Failed(format!("unknown keystore mode {other}"))),
    }
}

/// Spins until `condition` answers `Some`, or the clock says `budget_ms`
/// passed (a guest has no sleep; polling is the v0.1 wake shape, M2-K6).
fn poll<T>(
    budget_ms: u64,
    mut condition: impl FnMut() -> Result<Option<T>, GuestFault>,
) -> Result<T, GuestFault> {
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

/// Burns roughly `ms` of wall clock WITHOUT flooding the ledger: every
/// kernel crossing is an event (Law 2), so the clock is consulted once per
/// coarse spin rather than once per iteration.
fn dawdle(ms: u64) -> Result<(), GuestFault> {
    let started = clock::now().map_err(fault)?;
    loop {
        for step in 0..200_000u64 {
            std::hint::black_box(step);
        }
        if clock::now().map_err(fault)? >= started + ms {
            return Ok(());
        }
    }
}

/// Drains one child stream until EOF (M2-K6), bounded by the clock.
fn drain(handle: u64, which: process::ChildStream, budget_ms: u64) -> Result<Vec<u8>, GuestFault> {
    let mut collected = Vec::new();
    poll(budget_ms, || {
        match process::read(handle, which, 4096).map_err(process_fault)? {
            process::ReadResult::Data(bytes) => {
                collected.extend(bytes);
                Ok(None)
            }
            process::ReadResult::WouldBlock => Ok(None),
            process::ReadResult::Eof => Ok(Some(())),
        }
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
            let child = process::spawn("/bin/cat", &[], None, &[]).map_err(process_fault)?;
            let accepted = process::write_stdin(child, b"hello\n").map_err(process_fault)?;
            if accepted != 6 {
                return Err(GuestFault::Failed(format!("stdin accepted {accepted}")));
            }
            process::close_stdin(child).map_err(process_fault)?;
            let echoed = drain(child, process::ChildStream::Stdout, 3000)?;
            let status = poll(3000, || {
                match process::wait(child, 1000).map_err(process_fault)? {
                    process::WaitResult::Exited(code) => Ok(Some(code)),
                    process::WaitResult::Running => Ok(None),
                }
            })?;
            if status != 0 {
                return Err(GuestFault::Failed(format!("cat exited {status}")));
            }
            fs::write("/proc-echo.out", &echoed, "").map_err(fs_fault)
        }
        "proc-sleeper" => {
            process::spawn("/bin/sleep", &["30".to_owned()], None, &[]).map_err(process_fault)?;
            Ok(())
        }
        "proc-kill" => {
            let child = process::spawn("/bin/sleep", &["30".to_owned()], None, &[])
                .map_err(process_fault)?;
            process::kill(child, process::Signal::Terminate).map_err(process_fault)?;
            let status = poll(3000, || {
                match process::wait(child, 1000).map_err(process_fault)? {
                    process::WaitResult::Exited(code) => Ok(Some(code)),
                    process::WaitResult::Running => Ok(None),
                }
            })?;
            fs::write("/proc-kill.out", status.to_string().as_bytes(), "").map_err(fs_fault)
        }
        "proc-env" => {
            let env = vec![("JINND_GUEST_VAR".to_owned(), "from-guest".to_owned())];
            let child = process::spawn("/usr/bin/env", &[], None, &env).map_err(process_fault)?;
            let listing = drain(child, process::ChildStream::Stdout, 3000)?;
            fs::write("/proc-env.out", &listing, "").map_err(fs_fault)
        }
        "proc-run" => {
            let out = process::run("/bin/echo", &["hi".to_owned()]).map_err(process_fault)?;
            fs::write("/proc-run.out", &out, "").map_err(fs_fault)
        }
        "proc-denied" => match process::spawn("/bin/cat", &[], None, &[]) {
            Err(_) => Ok(()),
            Ok(_) => Err(GuestFault::Failed(
                "an ungranted spawn was not refused".into(),
            )),
        },
        // Output past the bundle's cap is the TYPED truncation ON THE WIRE
        // (M2-K6 round 4): a guest matches the bundle variant, never a string.
        "proc-truncated" => match process::run("/bin/cat", &["/dev/zero".to_owned()]) {
            Err(process::ProcessError::OutputTruncated) => Ok(()),
            other => Err(GuestFault::Failed(format!(
                "a runaway output was not the typed truncation: {other:?}"
            ))),
        },
        "proc-escape" => match process::spawn(arg, &[], None, &[]) {
            Err(process::ProcessError::Denied(_)) => Ok(()),
            other => Err(GuestFault::Failed(format!(
                "a link out of the allowlist was not the typed refusal: {other:?}"
            ))),
        },
        other => Err(GuestFault::Failed(format!("unknown process mode {other}"))),
    }
}

/// The M2-K6 net modes: `net-echo:<port>` listens on loopback and echoes
/// from its alarm handler; `net-refused:<addr>` asserts the bind refuses;
/// `net-wake:<port>` (M2-K7) listens and echoes from the READINESS wake
/// alone — no alarm is ever requested.
fn net_mode(mode: &str, arg: &str) -> Result<(), GuestFault> {
    match mode {
        "net-echo" => {
            let listener = net::listen(&format!("127.0.0.1:{arg}")).map_err(net_fault)?;
            LISTENER.store(listener, Ordering::SeqCst);
            clock::alarm_every(250, WAKE_TOKEN).map_err(fault)?;
            Ok(())
        }
        "net-wake" => {
            let listener = net::listen(&format!("127.0.0.1:{arg}")).map_err(net_fault)?;
            LISTENER.store(listener, Ordering::SeqCst);
            Ok(())
        }
        "net-refused" => match net::listen(arg) {
            Err(net::NetError::Denied(_)) => Ok(()),
            other => Err(GuestFault::Failed(format!(
                "the bind was not the typed refusal: {other:?}"
            ))),
        },
        "net-out" => outbound_mode(arg),
        "net-tls" => tls_mode(arg),
        "net-widen" => widen_mode(arg),
        other => Err(GuestFault::Failed(format!("unknown net mode {other}"))),
    }
}

fn get(url: &str, headers: &[(String, String)]) -> Result<net::OutboundResponse, net::NetError> {
    net::send_request(&net::OutboundRequest {
        method: "GET".to_owned(),
        url: url.to_owned(),
        headers: headers.to_vec(),
        body: Vec::new(),
    })
}

/// The M2-K14 outbound matrix, driven from a real guest through the real
/// daemon: `net-out:<allowed>,<denied>`. One revertible fs write joins it,
/// so the daemon can build a revert UNIT that contains both a revertible
/// effect and the irreversible call.
fn outbound_mode(arg: &str) -> Result<(), GuestFault> {
    let (allowed, denied) = arg
        .split_once(',')
        .ok_or_else(|| GuestFault::Failed("net-out wants <allowed>,<denied>".into()))?;
    fs::write("/kept", b"written before the call", "").map_err(fs_fault)?;
    // 1. An allowed authority answers, credential and query carried.
    let answer = get(
        &format!("http://127.0.0.1:{allowed}/probe?access_token={SECRET_TEXT}"),
        &[("authorization".to_owned(), format!("Bearer {SECRET_TEXT}"))],
    )
    .map_err(net_fault)?;
    if (answer.status, answer.body.as_slice()) != (200, b"pong".as_slice()) {
        return Err(GuestFault::Failed(format!(
            "the allowed call did not answer: {} {:?}",
            answer.status, answer.body
        )));
    }
    // 2. An off-allowlist authority is denied.
    match get(&format!("http://127.0.0.1:{denied}/probe"), &[]) {
        Err(net::NetError::Denied(_)) => {}
        other => {
            return Err(GuestFault::Failed(format!(
                "an off-allowlist call was not denied: {other:?}"
            )));
        }
    }
    // 3. A malformed URL is a THIRD answer, never the same as a refusal.
    match get("not a url", &[]) {
        Err(net::NetError::Invalid(_)) => {}
        other => {
            return Err(GuestFault::Failed(format!(
                "a malformed url was not invalid: {other:?}"
            )));
        }
    }
    // 4. A redirect off the allowlist is DENIED inside the call: the
    //    kernel never follows one, and never hands back one it cannot
    //    prove the allowlist admits.
    match get(&format!("http://127.0.0.1:{allowed}/redirect"), &[]) {
        Err(net::NetError::Denied(_)) => {}
        other => {
            return Err(GuestFault::Failed(format!(
                "the off-allowlist redirect was not denied: {other:?}"
            )));
        }
    }
    // 5. The 0.1.0 declaration is provided at the same door: same
    //    authority, body in and body out (R12).
    let body = net::request(
        "GET",
        &format!("http://127.0.0.1:{allowed}/probe"),
        &[],
    )
    .map_err(net_fault)?;
    if body != b"pong" {
        return Err(GuestFault::Failed(format!(
            "the declared shape did not answer: {body:?}"
        )));
    }
    match net::request("GET", &format!("http://127.0.0.1:{denied}/probe"), &[]) {
        Err(net::NetError::Denied(_)) => Ok(()),
        other => Err(GuestFault::Failed(format!(
            "the declared shape bypassed the allowlist: {other:?}"
        ))),
    }
}

/// The M2-K15 TLS matrix through a real guest (`net-tls:<allowed>,<denied>`).
///
/// `allowed` speaks TLS behind a certificate the kernel does not anchor, so
/// the guest sees `untrusted` — a THIRD refusal case beside `denied` and
/// `failed`, which is the whole point of adding it. `denied` is off the
/// allowlist and never dialled. The authorized-but-unbelieved call still
/// LANDS its irreversible ledger row: the host test reverts it.
fn tls_mode(arg: &str) -> Result<(), GuestFault> {
    let (allowed, denied) = arg
        .split_once(',')
        .ok_or_else(|| GuestFault::Failed("net-tls wants <allowed>,<denied>".into()))?;
    fs::write("/kept", b"written before the call", "").map_err(fs_fault)?;
    // 1. Admitted, reached, and REFUSED on its certificate. The credential
    //    rides along so the host test can grep the ledger for it.
    match get(
        &format!("https://127.0.0.1:{allowed}/probe?access_token={SECRET_TEXT}"),
        &[("authorization".to_owned(), format!("Bearer {SECRET_TEXT}"))],
    ) {
        Err(net::NetError::Untrusted(_)) => {}
        other => {
            return Err(GuestFault::Failed(format!(
                "an unanchored certificate was not untrusted: {other:?}"
            )));
        }
    }
    // 2. Off the allowlist over https is still DENIED, and denied first:
    //    a caller learns nothing about a target it may not reach.
    match get(&format!("https://127.0.0.1:{denied}/probe"), &[]) {
        Err(net::NetError::Denied(_)) => {}
        other => {
            return Err(GuestFault::Failed(format!(
                "an off-allowlist https call was not denied: {other:?}"
            )));
        }
    }
    // 3. The 0.1.0 declaration reaches the same door with the same answer.
    match net::request("GET", &format!("https://127.0.0.1:{allowed}/probe"), &[]) {
        Err(net::NetError::Untrusted(_)) => Ok(()),
        other => Err(GuestFault::Failed(format!(
            "the declared door answered differently: {other:?}"
        ))),
    }
}

/// The widening demonstration (`net-widen:<id>@<port>`): the entry tries to
/// patch its OWN grants to add the target to its outbound allowlist, and
/// the call is still refused afterwards.
fn widen_mode(arg: &str) -> Result<(), GuestFault> {
    let (id, port) = arg
        .split_once('@')
        .ok_or_else(|| GuestFault::Failed("net-widen wants <id>@<port>".into()))?;
    let patch = format!(
        r#"{{"grants":[{{"contract":"jinn:net","scope":{{"outbound":["127.0.0.1:{port}"]}}}}]}}"#
    );
    let answer = operator_call("jinn:profile", "patch-entry", &patch_payload(id, &patch))?;
    let reason = String::from_utf8_lossy(&answer[1.min(answer.len())..]).into_owned();
    if answer.first() != Some(&1) {
        return Err(GuestFault::Failed(format!(
            "an entry widened its own grants: {answer:?}"
        )));
    }
    if !reason.contains("itself") {
        return Err(GuestFault::Failed(format!(
            "the refusal did not name the reason: {reason}"
        )));
    }
    match get(&format!("http://127.0.0.1:{port}/probe"), &[]) {
        Err(net::NetError::Denied(_)) => Ok(()),
        other => Err(GuestFault::Failed(format!(
            "the call was admitted after a refused widening: {other:?}"
        ))),
    }
}

/// One echo tick: accept what is pending, echo what each connection sent,
/// close what the peer closed (M2-K6 polling shape).
fn echo_tick() -> Result<(), GuestFault> {
    let listener = LISTENER.load(Ordering::SeqCst);
    let mut conns = CONNS.lock().unwrap();
    while let net::AcceptResult::Connection(conn) = net::accept(listener).map_err(net_fault)? {
        conns.push(conn);
    }
    let mut closed = Vec::new();
    for &conn in conns.iter() {
        match net::read(conn, 4096).map_err(net_fault)? {
            net::ReadResult::Data(bytes) => {
                let mut offered = 0;
                while offered < bytes.len() {
                    offered += net::write(conn, &bytes[offered..]).map_err(net_fault)? as usize;
                }
            }
            net::ReadResult::Eof => {
                net::close(conn).map_err(net_fault)?;
                closed.push(conn);
            }
            net::ReadResult::WouldBlock => {}
        }
    }
    conns.retain(|conn| !closed.contains(conn));
    Ok(())
}

/// One operator-contract call over the handle lane (M2-K7).
/// The ledger's committed high-water mark, read from inside a delivery.
fn ledger_high_water() -> Result<u64, GuestFault> {
    let answer = operator_call(LEDGER_CONTRACT, "last-seq", &[])?;
    let bytes: [u8; 8] = answer
        .as_slice()
        .try_into()
        .map_err(|_| GuestFault::Failed("last-seq is not a u64".into()))?;
    Ok(u64::from_le_bytes(bytes))
}

fn operator_call(contract: &str, operation: &str, payload: &[u8]) -> Result<Vec<u8>, GuestFault> {
    let handle = services::resolve(contract).map_err(fault)?;
    services::call(handle, operation, payload).map_err(fault)
}

/// The `jinn:profile` request wire: u32-LE id length, the id, the patch.
fn patch_payload(id: &str, patch: &str) -> Vec<u8> {
    let mut wire = (id.len() as u32).to_le_bytes().to_vec();
    wire.extend(id.as_bytes());
    wire.extend(patch.as_bytes());
    wire
}

/// The M2-K7 operator modes. `introspect` and `ledger-read` answer at
/// activation and stash the answers on the granted fs; `profile-patch:<id>`
/// and `profile-patch-bad:<id>` patch from an alarm tick (the boot
/// reconcile still engages the document during activation, so a loader
/// conflict is retried next tick — a retryable refusal names "retry" or
/// "in flight"); `profile-patch-denied:<id>` asserts the scope refusal.
fn operator_mode(mode: &str, arg: &str) -> Result<(), GuestFault> {
    match mode {
        // Reads from a tick, once the boot reconcile has landed: the
        // composition it reports is then settled, not mid-activation.
        "introspect" => {
            clock::alarm_every(250, WAKE_TOKEN).map_err(fault)?;
            Ok(())
        }
        "ledger-read" => {
            let mut request = 1u64.to_le_bytes().to_vec();
            request.extend(500u32.to_le_bytes());
            let first = operator_call("jinn:ledger", "read-range", &request)?;
            fs::write("/ledger-page1.json", &first, "").map_err(fs_fault)?;
            let second = operator_call("jinn:ledger", "read-range", &request)?;
            fs::write("/ledger-page2.json", &second, "").map_err(fs_fault)?;
            let last = operator_call("jinn:ledger", "last-seq", &[])?;
            fs::write("/ledger-last", &last, "").map_err(fs_fault)
        }
        "profile-patch" | "profile-patch-bad" => {
            clock::alarm_every(250, WAKE_TOKEN).map_err(fault)?;
            Ok(())
        }
        "profile-patch-denied" => {
            // The contract: EVERY refusal, the scope's included, is the
            // typed `refused` outcome on the wire (tag 1 + reason), never
            // an outer kernel error.
            let answer = operator_call(
                "jinn:profile",
                "patch-entry",
                &patch_payload(arg, r#"{"data":"noop"}"#),
            )?;
            let reason = String::from_utf8_lossy(&answer[1.min(answer.len())..]).into_owned();
            if answer.first() == Some(&1) && reason.contains("scope") {
                Ok(())
            } else {
                Err(GuestFault::Failed(format!(
                    "a patch outside the scope was not `refused` on the wire: {answer:?}"
                )))
            }
        }
        other => Err(GuestFault::Failed(format!("unknown operator mode {other}"))),
    }
}

/// The M2-K21 `jinn:auth` mode, `auth:<name>:<presented>`: presents the
/// credential through the handle lane and writes the RAW wire answer
/// (tag + UTF-8) to `/auth-answer-<name>`, so the daemon test reads
/// exactly what the guest saw.
fn auth_mode(arg: &str) -> Result<(), GuestFault> {
    let (name, presented) = arg.split_once(':').unwrap_or((arg, ""));
    let answer = operator_call("jinn:auth", "verify", presented.as_bytes())?;
    fs::write(&format!("/auth-answer-{name}"), &answer, "").map_err(fs_fault)
}

/// The settings contract the two-hop modes provide and consume (M2-K8 #26).
const SETTINGS_CONTRACT: &str = "jinn:test/settings";

/// The M2-K8 profile modes. `settings-provider` provides the settings
/// contract; its `patch` handler patches the OWNER entry through
/// `jinn:profile` — the two-hop shape. `settings-owner` resolves the
/// provider from `activate` (polling until it is live) and logs the
/// answer. `settings-trigger:<owner>` calls the provider's `patch` from a
/// tick. `profile-read:<id>` reads `entry` and `document` under a
/// read-only grant and asserts a patch is refused.
fn profile_mode(mode: &str, arg: &str) -> Result<(), GuestFault> {
    match mode {
        "settings-provider" => {
            services::provide(SETTINGS_CONTRACT).map_err(fault)?;
            Ok(())
        }
        "settings-owner" => {
            let answer = poll(4000, || {
                let Ok(handle) = services::resolve(SETTINGS_CONTRACT) else {
                    return Ok(None);
                };
                Ok(services::call(handle, "get", b"").ok())
            })?;
            let mut line = answer;
            line.push(b'\n');
            fs::append("/owner.log", &line, "").map_err(fs_fault)
        }
        "settings-trigger" => {
            clock::alarm_every(250, WAKE_TOKEN).map_err(fault)?;
            Ok(())
        }
        "profile-read" => {
            let entry = operator_call("jinn:profile", "entry", &patch_payload(arg, ""))?;
            fs::write("/profile-entry.json", &entry, "").map_err(fs_fault)?;
            let document = operator_call("jinn:profile", "document", &[])?;
            fs::write("/profile-document.json", &document, "").map_err(fs_fault)?;
            match operator_call(
                "jinn:profile",
                "patch-entry",
                &patch_payload(arg, r#"{"data":"noop"}"#),
            ) {
                Err(_) => fs::write("/profile-read-denied", b"denied", "").map_err(fs_fault),
                Ok(answer) => Err(GuestFault::Failed(format!(
                    "a patch under a read-only profile grant was not refused: {answer:?}"
                ))),
            }
        }
        other => Err(GuestFault::Failed(format!("unknown profile mode {other}"))),
    }
}

/// The M2-K9 modes (harness #31) — the two-hop shape with a NOTICE.
/// `notify-provider` provides the settings contract and, from its
/// `patch-notify` handler, patches its CONSUMER and immediately dispatches
/// the `changed` notice serially: straight into the window K8 opened, where
/// the consumer holds a pending restart and is still addressable.
/// `notify-consumer` listens for the notice and registers an effect whose
/// inverse dawdles, so its restart holds that window open for the whole
/// withdrawal replay — the seat still installed, the listener still routed;
/// its handler calls BACK into the provider, the peer that cannot answer
/// because it is inside the very call that emitted. `notify-trigger:<id>`
/// starts the shape from a tick.
fn notify_mode(mode: &str) -> Result<(), GuestFault> {
    match mode {
        "notify-provider" => services::provide(SETTINGS_CONTRACT).map(|_| ()).map_err(fault),
        "notify-consumer" => {
            effects::register("consumer effect", CONSUMER_UNDO_TOKEN).map_err(fault)?;
            jinn::plugin::events::listen(CHANGED_TOPIC, NOTICE_TOKEN).map_err(fault)?;
            fs::append("/consumer.log", b"act\n", "").map_err(fs_fault)?;
            // A deliberately unhurried activation: the listener is live and
            // the fiber's restart is still in flight for the whole of it,
            // which is precisely the state a reply-expecting dispatch must
            // refuse — and long enough to observe instead of race for.
            dawdle(600)
        }
        "notify-trigger" => {
            clock::alarm_every(250, WAKE_TOKEN).map_err(fault)?;
            Ok(())
        }
        other => Err(GuestFault::Failed(format!("unknown notify mode {other}"))),
    }
}

/// The M2-K13 modes (harness #40/#41) — the kernel's own lifecycle
/// publish. `lifecycle-listener` subscribes to the reserved topic and
/// writes every delivery down; `lifecycle-slow` is the same listener that
/// dawdles inside each delivery (the back-pressure probe);
/// `lifecycle-eavesdrop` asserts an UNGRANTED subscribe is refused; and
/// `lifecycle-forge` asserts a granted guest still cannot EMIT on the
/// reserved topic — only the kernel publishes there.
fn lifecycle_mode(mode: &str) -> Result<(), GuestFault> {
    match mode {
        "lifecycle-listener" | "lifecycle-slow" => {
            effects::register("lifecycle effect", 1).map_err(fault)?;
            jinn::plugin::events::listen(TRANSITIONS_TOPIC, LIFECYCLE_TOKEN)
                .map(|_| ())
                .map_err(fault)
        }
        "lifecycle-eavesdrop" => {
            match jinn::plugin::events::listen(TRANSITIONS_TOPIC, LIFECYCLE_TOKEN) {
                // The refusal is WRITTEN DOWN, not merely survived: a test
                // that only checks the activation did not fault would pass
                // just as well with no gate at all.
                Err(refusal) => fs::write("/eavesdrop.out", format!("{refusal:?}").as_bytes(), "")
                    .map_err(fs_fault),
                Ok(_) => Err(GuestFault::Failed(
                    "an ungranted lifecycle subscribe was not refused".into(),
                )),
            }
        }
        "lifecycle-forge" => {
            jinn::plugin::events::listen(TRANSITIONS_TOPIC, LIFECYCLE_TOKEN).map_err(fault)?;
            let forged = jinn::plugin::events::emit(
                TRANSITIONS_TOPIC,
                jinn::plugin::types::DispatchMode::Emit,
                &jinn::plugin::types::Selector::All,
                b"forged",
            );
            match forged {
                Err(refusal) => {
                    fs::write("/forge.out", format!("{refusal:?}").as_bytes(), "")
                        .map_err(fs_fault)
                }
                Ok(_) => Err(GuestFault::Failed(
                    "a guest emit on the reserved lifecycle topic was not refused".into(),
                )),
            }
        }
        other => Err(GuestFault::Failed(format!(
            "unknown lifecycle mode {other}"
        ))),
    }
}

/// The M2-K10 modes (harness #32) — two HONEST plugins parking on each
/// other, no restart anywhere in the shape. `cycle-provider` provides the
/// settings contract and, from inside a call it is serving, dispatches its
/// notice to its listeners. `cycle-owner` listens for that notice and,
/// from the handler, calls BACK into the provider — the peer that cannot
/// answer, because it is inside the very call that emitted. `cycle-caller`
/// is the same pair with the edges taken in the OTHER order: it calls the
/// provider first, and the provider's dispatch is the edge that would
/// close. `cycle-trigger` starts the first ordering from a tick.
fn cycle_mode(mode: &str) -> Result<(), GuestFault> {
    match mode {
        "cycle-provider" => services::provide(SETTINGS_CONTRACT).map(|_| ()).map_err(fault),
        "cycle-owner" => jinn::plugin::events::listen(CYCLE_TOPIC, CYCLE_TOKEN)
            .map(|_| ())
            .map_err(fault),
        "cycle-caller" => {
            jinn::plugin::events::listen(CYCLE_TOPIC, CYCLE_TOKEN).map_err(fault)?;
            clock::alarm_every(250, WAKE_TOKEN).map_err(fault)?;
            Ok(())
        }
        "cycle-trigger" => {
            clock::alarm_every(250, WAKE_TOKEN).map_err(fault)?;
            Ok(())
        }
        other => Err(GuestFault::Failed(format!("unknown cycle mode {other}"))),
    }
}

/// Records a typed wait-cycle refusal for the test to read back: the tag
/// byte, then the record's own fields as JSON. Nothing here parses the
/// kernel's prose into meaning — there is none to parse (M2-K10, R3).
fn cycle_record(cycle: &jinn::plugin::types::WaitCycle) -> Vec<u8> {
    let through: Vec<String> = cycle
        .through
        .iter()
        .map(|hop| format!("\"{hop}\""))
        .collect();
    let mut wire = vec![7];
    wire.extend(
        format!(
            r#"{{"on":"{}","waiter":"{}","target":"{}","through":[{}]}}"#,
            cycle.on,
            cycle.waiter,
            cycle.target,
            through.join(",")
        )
        .into_bytes(),
    );
    wire
}

/// What the kernel answered one `cycle-*` crossing: 7 = the typed wait
/// cycle (the record follows), 0 = it went through (the byte count
/// follows), 2 = some other kernel error.
fn cycle_answer(answer: Result<Vec<u8>, jinn::plugin::types::KernelError>) -> Vec<u8> {
    match answer {
        Ok(bytes) => vec![0, bytes.len() as u8],
        Err(jinn::plugin::types::KernelError::Cycle(cycle)) => cycle_record(&cycle),
        Err(other) => {
            let mut wire = vec![2];
            wire.extend(format!("{other:?}").into_bytes());
            wire
        }
    }
}

/// The provider's half: dispatch the notice from inside the call it is
/// currently serving. In the first ordering this walk is delivered and the
/// listener's call back is what closes; in the second the caller is
/// already parked on this very fiber, so THIS walk is the closing edge and
/// is refused whole.
fn cycle_notify() -> Result<Vec<u8>, GuestFault> {
    Ok(match jinn::plugin::events::emit(
        CYCLE_TOPIC,
        jinn::plugin::types::DispatchMode::Serial,
        &jinn::plugin::types::Selector::All,
        b"notice",
    ) {
        Ok(outputs) => vec![0, outputs.len() as u8],
        Err(jinn::plugin::types::KernelError::Cycle(cycle)) => cycle_record(&cycle),
        Err(other) => {
            let mut wire = vec![2];
            wire.extend(format!("{other:?}").into_bytes());
            wire
        }
    })
}

/// One tick of either ordering: ask the provider to run the shape once and
/// record what the kernel answered, exactly once.
fn cycle_tick(out: &str) -> Result<(), GuestFault> {
    if fs::meta(out).is_ok() {
        return Ok(());
    }
    let Ok(handle) = services::resolve(SETTINGS_CONTRACT) else {
        return Ok(());
    };
    // The provider's own verdict travels back VERBATIM: in one ordering
    // its walk was delivered, in the other its walk was the refused edge.
    // A refusal of THIS call is recorded in the same vocabulary.
    let answer = match services::call(handle, "cycle-notify", b"") {
        Ok(bytes) => bytes,
        Err(refused) => cycle_answer(Err(refused)),
    };
    fs::write(out, &answer, "").map_err(fs_fault)
}

/// Records a typed dispatch refusal for the test to read back: the tag
/// byte, then the record's own fields as JSON. The guest never formats the
/// kernel's prose into meaning — there is none to format (M2-K9, R3).
fn refusal(tag: u8, case: &str, target: &jinn::plugin::types::RefusedTarget) -> Vec<u8> {
    let mut wire = vec![tag];
    wire.extend(
        format!(
            r#"{{"case":"{case}","entry":"{}","incarnation":{},"topic":"{}"}}"#,
            target.entry, target.incarnation, target.topic
        )
        .into_bytes(),
    );
    wire
}

/// Patch the consumer, then dispatch the notice into the window that
/// opened. The outcome's first byte is what the KERNEL answered: 1 = the
/// typed `restarting` refusal, 2 = some other kernel error, 0 = the walk
/// was delivered (the listener-output count follows), 9 = the patch itself
/// was refused (retryable; nothing was dispatched).
fn notify(consumer: &str) -> Result<Vec<u8>, GuestFault> {
    let accepted = operator_call(
        "jinn:profile",
        "patch-entry",
        &patch_payload(consumer, r#"{"data":"notify-consumer:v2"}"#),
    )?;
    if accepted.first() != Some(&2) {
        let mut wire = vec![9];
        wire.extend(accepted);
        return Ok(wire);
    }
    // The notice, dispatched across the replacement the patch scheduled.
    // A walk that selects nobody (the old listener withdrawn, the new one
    // not yet registered) is not an answer about the target at all, so it
    // is retried until the kernel says something — bounded by the clock.
    let mut outcome = Vec::new();
    for _ in 0..40u32 {
        match jinn::plugin::events::emit(
            CHANGED_TOPIC,
            jinn::plugin::types::DispatchMode::Serial,
            &jinn::plugin::types::Selector::All,
            b"changed",
        ) {
            // A walk that selected nobody says nothing about the target
            // (the old listener withdrawn, the new one not yet
            // registered): dispatch again a moment later.
            Ok(outputs) if outputs.is_empty() => {}
            Ok(outputs) => {
                outcome = vec![0, outputs.len() as u8];
                break;
            }
            // The typed refusal: the case IS the next move, and the
            // record names who refused. Nothing here parses a sentence.
            Err(jinn::plugin::types::KernelError::Restarting(target)) => {
                outcome = refusal(1, "restarting", &target);
                break;
            }
            Err(jinn::plugin::types::KernelError::Gone(target)) => {
                outcome = refusal(4, "gone", &target);
                break;
            }
            Err(jinn::plugin::types::KernelError::Suspended(target)) => {
                outcome = refusal(5, "suspended", &target);
                break;
            }
            Err(jinn::plugin::types::KernelError::Stalled(target)) => {
                outcome = refusal(6, "stalled", &target);
                break;
            }
            Err(other) => {
                outcome = vec![2];
                outcome.extend(format!("{other:?}").into_bytes());
                break;
            }
        }
        dawdle(20)?;
    }
    if outcome.is_empty() {
        // Every dispatch selected nobody: the notice had no audience at
        // all, which is not an answer about the target.
        outcome = vec![3];
    }
    // The pending restart is ASKABLE, not only discoverable by stalling —
    // snapshotted from inside the window that produced the refusal.
    if outcome.first() == Some(&1) {
        let entries = operator_call("jinn:introspect", "entries", &[])?;
        fs::write("/notify-introspect.json", &entries, "").map_err(fs_fault)?;
    }
    Ok(outcome)
}

/// One notice tick (M2-K9): asks the provider to run the shape once and
/// records what the kernel answered. The window between "the restart is
/// SCHEDULED" and "the swap commits" is scheduling-narrow by nature — that
/// is exactly why a dispatch landing in it must be refused rather than
/// left to luck — so the tick RETRIES the whole shape until it lands in
/// the window (tag 1) or the attempt budget is spent. Every attempt's tag
/// is on the record in `/notify.log`, so a run that never reaches the
/// window is legible rather than silent.
fn notify_tick(consumer: &str) -> Result<(), GuestFault> {
    if fs::meta("/notify.out").is_ok() {
        return Ok(());
    }
    let Ok(handle) = services::resolve(SETTINGS_CONTRACT) else {
        return Ok(());
    };
    let answer = match services::call(handle, "patch-notify", consumer.as_bytes()) {
        Ok(answer) => answer,
        Err(refused) => {
            fs::append("/notify.err", format!("{refused:?}\n").as_bytes(), "").map_err(fs_fault)?;
            return Ok(());
        }
    };
    let Some(&tag) = answer.first() else {
        return Ok(());
    };
    // 9: the patch itself was refused (the boot reconcile still engaged);
    // nothing was dispatched, so it is not an attempt.
    if tag == 9 {
        return Ok(());
    }
    fs::append("/notify.log", &[tag], "").map_err(fs_fault)?;
    let attempts = fs::meta("/notify.log").map_err(fs_fault)?.size;
    if tag == 1 || attempts >= 20 {
        fs::write("/notify.out", &answer, "").map_err(fs_fault)?;
    }
    Ok(())
}

/// One trigger tick (M2-K8 #26): asks the provider to patch the owner;
/// a loader conflict (the boot reconcile still engaged) retries next
/// tick; the final answer is recorded once.
fn trigger_tick(owner: &str) -> Result<(), GuestFault> {
    if fs::meta("/trigger.out").is_ok() {
        return Ok(());
    }
    let Ok(handle) = services::resolve(SETTINGS_CONTRACT) else {
        return Ok(());
    };
    let Ok(answer) = services::call(handle, "patch", owner.as_bytes()) else {
        return Ok(());
    };
    let reason = String::from_utf8_lossy(&answer[1.min(answer.len())..]).into_owned();
    if answer.first() == Some(&1) && (reason.contains("retry") || reason.contains("in flight")) {
        return Ok(());
    }
    fs::write("/trigger.out", &answer, "").map_err(fs_fault)
}

/// One introspection from the tick: stashes the composition and readiness
/// once `boot-reconciled` is true (every entry settled), else waits.
fn introspect_tick() -> Result<(), GuestFault> {
    if fs::meta("/introspect-entries.json").is_ok() {
        return Ok(());
    }
    let readiness = operator_call("jinn:introspect", "readiness", &[])?;
    if !String::from_utf8_lossy(&readiness).contains("\"boot-reconciled\":true") {
        return Ok(());
    }
    let entries = operator_call("jinn:introspect", "entries", &[])?;
    fs::write("/introspect-readiness.json", &readiness, "").map_err(fs_fault)?;
    fs::write("/introspect-entries.json", &entries, "").map_err(fs_fault)
}

/// One patch attempt from the tick: records the final answer, retries a
/// loader conflict silently.
fn patch_tick(mode: &str, id: &str) -> Result<(), GuestFault> {
    if fs::meta("/patch.out").is_ok() {
        return Ok(());
    }
    let patch = if mode == "profile-patch-bad" {
        r#"{"grants":[7]}"#
    } else {
        r#"{"data":"noop"}"#
    };
    let answer = operator_call("jinn:profile", "patch-entry", &patch_payload(id, patch))?;
    let reason = String::from_utf8_lossy(&answer[1.min(answer.len())..]).into_owned();
    if answer.first() == Some(&1) && (reason.contains("retry") || reason.contains("in flight")) {
        return Ok(());
    }
    fs::write("/patch.out", &answer, "").map_err(fs_fault)
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
        if mode == "introspect" || mode == "ledger-read" || mode.starts_with("profile-patch") {
            return operator_mode(mode, arg);
        }
        if mode == "auth" {
            return auth_mode(arg);
        }
        if mode.starts_with("keystore") {
            return keystore_mode(mode);
        }
        if mode.starts_with("settings-") || mode == "profile-read" {
            return profile_mode(mode, arg);
        }
        if mode.starts_with("notify-") {
            return notify_mode(mode);
        }
        if mode.starts_with("cycle-") {
            return cycle_mode(mode);
        }
        if mode.starts_with("lifecycle-") {
            return lifecycle_mode(mode);
        }
        match mode {
            "trap" => panic!("fixture trap mode"),
            "spin" => loop {
                std::hint::black_box(());
            },
            "caller" => {
                let handle = services::resolve(GREETER_CONTRACT).map_err(fault)?;
                let answer = services::call(handle, "greet", b"from-guest").map_err(fault)?;
                *STASH.lock().unwrap() = answer;
                effects::register("caller effect", 1).map_err(fault)?;
                Ok(())
            }
            // M2-K24 (harness #45): a sibling's contract injected AT
            // ACTIVATION over the string lane. `inject-counter` reads the
            // counter and fails if no provider answers — the coin-toss
            // shape; `inject-counter-bad` fails on its OWN account against
            // a live provider (an operation it does not have), the (c)
            // shape a repaired provider re-arms.
            // M2-K24: a provider whose provision lands long before its
            // activation returns — the window in which a gate that read
            // provision as readiness would let a consumer through.
            "provider-slow" => {
                effects::register("fixture effect", 1).map_err(fault)?;
                services::provide(COUNTER_CONTRACT).map_err(fault)?;
                dawdle(700)
            }
            "inject-counter" | "inject-counter-bad" => {
                let handle = services::resolve(COUNTER_CONTRACT).map_err(fault)?;
                let operation = if mode == "inject-counter" { "get" } else { "nope" };
                let answer = services::call(handle, operation, b"").map_err(fault)?;
                *STASH.lock().unwrap() = answer;
                effects::register("inject effect", 1).map_err(fault)?;
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
            "listener-slow" | "listener-spin" => {
                jinn::plugin::events::listen(TOPIC, 7).map_err(fault)?;
                Ok(())
            }
            "listener-budget" => {
                jinn::plugin::events::listen_within(
                    TOPIC,
                    7,
                    jinn::plugin::types::DeliveryBudget { fuel: 10_000 },
                )
                .map_err(fault)?;
                Ok(())
            }
            "listener-zero" => match jinn::plugin::events::listen_within(
                TOPIC,
                7,
                jinn::plugin::types::DeliveryBudget { fuel: 0 },
            ) {
                Err(jinn::plugin::types::KernelError::Invalid(_)) => Ok(()),
                Err(error) => Err(GuestFault::Failed(format!(
                    "zero budget returned the wrong refusal: {error:?}"
                ))),
                Ok(_) => Err(GuestFault::Failed(
                    "a zero delivery budget was accepted".into(),
                )),
            },
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
                for absent in [
                    fs::read("/log/b.txt").map(|_| ()),
                    fs::meta("/missing").map(|_| ()),
                ] {
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
            // A read-only fs grant (M2-K8 #24): reads answer, every
            // mutation is the typed denial.
            "fs-readonly" => {
                if let Err(fs::FsError::Denied) = fs::read("/doc.txt") {
                    return Err(GuestFault::Failed("a read under a read-only grant was denied".into()));
                }
                let denied = matches!(fs::write("/doc.txt", b"x", ""), Err(fs::FsError::Denied))
                    && matches!(fs::append("/doc.txt", b"x", ""), Err(fs::FsError::Denied))
                    && matches!(fs::remove("/doc.txt", ""), Err(fs::FsError::Denied));
                if denied {
                    Ok(())
                } else {
                    Err(GuestFault::Failed(
                        "a mutation under a read-only fs grant was not denied".into(),
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
            return Err(GuestFault::Failed(format!(
                "grumpy undo ran (token {token})"
            )));
        }
        Ok(())
    }

    fn handle_event(token: u64, topic: String, payload: Vec<u8>) -> Result<Vec<u8>, GuestFault> {
        // The M2-K9 notice (harness #31). Reaching this handler at all is
        // the defect: the delivery landed in an incarnation the loader is
        // already replacing, and the call below can never be served — the
        // provider is inside the very call that emitted the notice.
        // The M2-K10 notice (harness #32): from this handler the owner
        // calls BACK into the provider, which is parked on this very
        // delivery. The kernel refuses that call rather than letting both
        // ends run to the guest deadline; what it answered is recorded and
        // the handler returns normally, so nothing here is a stall.
        // The kernel's own lifecycle publish (M2-K13): every delivery is
        // written down verbatim, followed by what the ledger had ALREADY
        // committed when the delivery landed — the in-handler half of the
        // ordering proof (a delivery may never precede its ledger row).
        if topic == TOPIC {
            match MODE.lock().unwrap().as_str() {
                "listener-slow" => dawdle(120)?,
                "listener-spin" | "listener-budget" => loop {
                    std::hint::black_box(());
                },
                _ => {}
            }
            return Ok(payload);
        }
        if topic == TRANSITIONS_TOPIC {
            if token != LIFECYCLE_TOKEN {
                return Err(GuestFault::Failed(
                    "a malformed lifecycle delivery arrived".into(),
                ));
            }
            let mut line = payload;
            line.push(b'\t');
            line.extend(ledger_high_water()?.to_string().into_bytes());
            line.push(b'\n');
            fs::append("/transitions.log", &line, "").map_err(fs_fault)?;
            if MODE.lock().unwrap().as_str() == "lifecycle-slow" {
                dawdle(400)?;
            }
            return Ok(Vec::new());
        }
        if topic == CYCLE_TOPIC {
            if token != CYCLE_TOKEN {
                return Err(GuestFault::Failed("a malformed notice arrived".into()));
            }
            // The live wait is ASKABLE from inside the window it opened,
            // not only discoverable by stalling in it (M2-K10).
            let waits = operator_call("jinn:introspect", "waits", &[])?;
            fs::write("/cycle-waits.json", &waits, "").map_err(fs_fault)?;
            let handle = services::resolve(SETTINGS_CONTRACT).map_err(fault)?;
            let answer = cycle_answer(services::call(handle, "get", b""));
            fs::write("/owner.out", &answer, "").map_err(fs_fault)?;
            return Ok(b"answered".to_vec());
        }
        if topic == CHANGED_TOPIC {
            if token != NOTICE_TOKEN {
                return Err(GuestFault::Failed("a malformed notice arrived".into()));
            }
            fs::append("/consumer.log", b"notice\n", "").map_err(fs_fault)?;
            let handle = services::resolve(SETTINGS_CONTRACT).map_err(fault)?;
            return services::call(handle, "get", b"").map_err(fault);
        }
        // A typed readiness wake (M2-K7): the token is the handle and the
        // payload repeats it; the echo tick serves whatever is ready.
        if topic == READABLE_TOPIC {
            if payload != token.to_le_bytes() {
                return Err(GuestFault::Failed(
                    "a malformed readiness wake arrived".into(),
                ));
            }
            COUNTER.fetch_add(1, Ordering::SeqCst);
            echo_tick()?;
            return Ok(Vec::new());
        }
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
            if let Some((patch_mode, id)) = mode.split_once(':') {
                if patch_mode.starts_with("profile-patch") {
                    patch_tick(patch_mode, id)?;
                }
                if patch_mode == "settings-trigger" {
                    trigger_tick(id)?;
                }
                if patch_mode == "notify-trigger" {
                    notify_tick(id)?;
                }
            }
            // The M2-K10 orderings (harness #32): the trigger opens with
            // the provider's dispatch, the caller opens with its own call.
            if mode == "cycle-trigger" {
                cycle_tick("/cycle.out")?;
            }
            if mode == "cycle-caller" {
                cycle_tick("/caller.out")?;
            }
            if mode == "introspect" {
                introspect_tick()?;
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
                let value = COUNTER.fetch_add(u64::from_le_bytes(delta), Ordering::SeqCst)
                    + u64::from_le_bytes(delta);
                Ok(value.to_le_bytes().to_vec())
            }
            "get" if *MODE.lock().unwrap() == "settings-provider" => Ok(b"v1".to_vec()),
            "get" => Ok(COUNTER.load(Ordering::SeqCst).to_le_bytes().to_vec()),
            "stall" => {
                dawdle(170)?;
                Ok(Vec::new())
            }
            // The two-hop shape (M2-K8 #26): from inside this handler the
            // provider patches its OWNER, whose restarted `activate` will
            // call `get` on this very instance.
            "patch" => {
                let owner = String::from_utf8_lossy(&payload).into_owned();
                operator_call(
                    "jinn:profile",
                    "patch-entry",
                    &patch_payload(&owner, r#"{"data":"settings-owner:v2"}"#),
                )
            }
            // The M2-K9 shape (harness #31): patch the consumer, then
            // dispatch the notice into the window that just opened.
            "patch-notify" => notify(&String::from_utf8_lossy(&payload)),
            // The M2-K10 shape (harness #32): dispatch the notice from
            // inside the call this fiber is currently serving.
            "cycle-notify" => cycle_notify(),
            "stash" => Ok(STASH.lock().unwrap().clone()),
            _ => Err(GuestFault::Failed(format!("unknown operation {operation}"))),
        }
    }

    fn snapshot() -> Vec<u8> {
        COUNTER.load(Ordering::SeqCst).to_le_bytes().to_vec()
    }

    fn restore(blob: Vec<u8>) -> Result<(), GuestFault> {
        if *MODE.lock().unwrap() == "flaky-restore" {
            return Err(GuestFault::Failed(
                "flaky restore refused the handoff".into(),
            ));
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
