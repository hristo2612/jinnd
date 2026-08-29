//! Provider-seam pins for M2-K6 `jinn:process`: default deny under a bare
//! grant, the resolved-executable allowlist, env policy, caller-scoped
//! handles, non-blocking streams under a hard memory bound (R9), the
//! capped wait (R1), typed signals, and the release that kills AND reaps.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use jinnd_api::{ErrorCode, FiberId, LedgerEventKind};

use super::HostProcess;
use super::ring::STREAM_CAP;
use crate::broker::Broker;
use crate::grants::{EnvPolicy, GrantScope, ProcessScope};
use crate::hostcaps::PROCESS_CONTRACT;
use crate::hostwire::{TAG_DATA, TAG_EOF, TAG_WOULD_BLOCK, encode_spawn, put_segment};
use crate::peer::LedgerSink;

pub(super) struct Recording(Mutex<Vec<(LedgerEventKind, Option<FiberId>)>>);

impl LedgerSink for Recording {
    fn append(&self, kind: LedgerEventKind, fiber: Option<FiberId>) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((kind, fiber));
    }
}

impl Recording {
    pub(super) fn kinds(&self) -> Vec<(LedgerEventKind, Option<FiberId>)> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn refusals(&self) -> usize {
        self.kinds()
            .iter()
            .filter(|(kind, _)| {
                matches!(kind, LedgerEventKind::GrantRefused { contract, .. } if contract == PROCESS_CONTRACT)
            })
            .count()
    }
}

pub(super) struct Rig {
    pub(super) ledger: Arc<Recording>,
    pub(super) broker: Arc<Broker>,
    pub(super) provider: Arc<HostProcess>,
    /// Fiber 7, allowed `/bin` and `/usr/bin`, inherit-none.
    pub(super) guest: u64,
    /// Fiber 8, a bare grant: the empty policy.
    bare: u64,
}

pub(super) fn rig() -> Rig {
    let ledger = Arc::new(Recording(Mutex::new(Vec::new())));
    let broker = Arc::new(Broker::new(Arc::clone(&ledger) as Arc<dyn LedgerSink>));
    let provider = HostProcess::new(Arc::clone(&ledger) as Arc<dyn LedgerSink>);
    provider
        .register(&broker)
        .unwrap_or_else(|error| panic!("register: {error:?}"));
    let guest = broker.register_peer(Some(FiberId(7)));
    broker.grant_with(
        guest,
        PROCESS_CONTRACT,
        GrantScope::Process(ProcessScope {
            exec: vec!["/bin".into(), "/usr/bin".into()],
            env: EnvPolicy::InheritNone,
        }),
    );
    let bare = broker.register_peer(Some(FiberId(8)));
    broker.grant_with(
        bare,
        PROCESS_CONTRACT,
        GrantScope::Process(ProcessScope::default()),
    );
    Rig {
        ledger,
        broker,
        provider,
        guest,
        bare,
    }
}

fn spawn_wire(command: &str, args: &[&str]) -> Vec<u8> {
    let args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
    encode_spawn(command, &args, None, &[])
}

fn with_handle(handle: u64, tail: &[u8]) -> Vec<u8> {
    let mut wire = handle.to_le_bytes().to_vec();
    wire.extend(tail);
    wire
}

impl Rig {
    async fn call(&self, peer: u64, op: &str, payload: Vec<u8>) -> Result<Vec<u8>, ErrorCode> {
        self.broker
            .dispatch(peer, PROCESS_CONTRACT, op, payload)
            .await
            .map_err(|error| error.code)
    }

    async fn spawn(&self, peer: u64, command: &str, args: &[&str]) -> u64 {
        let answer = self
            .call(peer, "spawn", spawn_wire(command, args))
            .await
            .unwrap_or_else(|code| panic!("spawn {command}: {code:?}"));
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&answer);
        u64::from_le_bytes(bytes)
    }

    async fn read(&self, peer: u64, handle: u64, which: u8) -> (u8, Vec<u8>) {
        let mut tail = vec![which];
        tail.extend(4096u32.to_le_bytes());
        let answer = self
            .call(peer, "read", with_handle(handle, &tail))
            .await
            .unwrap_or_else(|code| panic!("read: {code:?}"));
        (answer[0], answer[1..].to_vec())
    }

    async fn drain(&self, peer: u64, handle: u64, which: u8) -> Vec<u8> {
        let mut collected = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match self.read(peer, handle, which).await {
                (TAG_DATA, data) => collected.extend(data),
                (TAG_EOF, _) => return collected,
                _ => {
                    assert!(Instant::now() < deadline, "the stream ends");
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }
    }

    async fn wait(&self, peer: u64, handle: u64, timeout_ms: u64) -> Option<i32> {
        let answer = self
            .call(peer, "wait", with_handle(handle, &timeout_ms.to_le_bytes()))
            .await
            .unwrap_or_else(|code| panic!("wait: {code:?}"));
        (answer[0] == TAG_DATA).then(|| {
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&answer[1..5]);
            i32::from_le_bytes(bytes)
        })
    }
}

/// A bare grant is the EMPTY policy: nothing may be executed (default deny,
/// M2-K6); a relative command and an unlisted executable refuse likewise,
/// each on the record with the caller's attribution.
#[tokio::test]
async fn a_bare_grant_a_relative_command_and_an_unlisted_executable_refuse() {
    let rig = rig();
    assert_eq!(
        rig.call(rig.bare, "spawn", spawn_wire("/bin/cat", &[]))
            .await,
        Err(ErrorCode::EffectFailed),
        "the empty policy allows nothing"
    );
    assert_eq!(
        rig.call(rig.guest, "spawn", spawn_wire("bin/cat", &[]))
            .await,
        Err(ErrorCode::EffectFailed)
    );
    assert_eq!(
        rig.call(rig.guest, "spawn", spawn_wire("/sbin/nologin", &[]))
            .await,
        Err(ErrorCode::EffectFailed)
    );
    assert_eq!(rig.ledger.refusals(), 3, "every refusal is ledgered");
    assert_eq!(rig.provider.live(), 0);
}

/// The hard bound (R9): a child writing far more than the ring holds is
/// backpressured — the provider never buffers past the cap, the child stays
/// alive and blocked — and a guest that then drains reads every byte.
#[tokio::test]
async fn an_unread_stream_backpressures_the_child_under_the_bound() {
    let rig = rig();
    let total = STREAM_CAP * 4;
    let handle = rig
        .spawn(
            rig.guest,
            "/bin/sh",
            &["-c", &format!("head -c {total} /dev/zero")],
        )
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let buffered = rig.provider.buffered(rig.guest, handle);
    assert!(buffered <= STREAM_CAP, "never above the cap: {buffered}");
    assert!(buffered > 0, "the pump filled what it could");
    assert_eq!(
        rig.wait(rig.guest, handle, 100).await,
        None,
        "the child is blocked on its pipe, not gone"
    );
    let drained = rig.drain(rig.guest, handle, 0).await;
    assert_eq!(
        drained.len(),
        total,
        "every byte arrives once the guest reads"
    );
    assert_eq!(rig.wait(rig.guest, handle, 1000).await, Some(0));
}

/// A handle is the caller's alone (R4): another peer's use is refused on
/// the record; `wait` is capped at 1000ms whatever the guest asks (R1).
#[tokio::test]
async fn handles_are_caller_scoped_and_wait_is_capped() {
    let rig = rig();
    let handle = rig.spawn(rig.guest, "/bin/sleep", &["30"]).await;
    assert_eq!(
        rig.call(rig.bare, "wait", with_handle(handle, &0u64.to_le_bytes()))
            .await,
        Err(ErrorCode::EffectFailed)
    );
    assert_eq!(rig.ledger.refusals(), 1);
    let started = Instant::now();
    assert_eq!(rig.wait(rig.guest, handle, 60_000).await, None);
    assert!(
        started.elapsed() < Duration::from_millis(2500),
        "capped: {:?}",
        started.elapsed()
    );
    rig.provider
        .withdraw(handle)
        .await
        .unwrap_or_else(|error| panic!("release: {error:?}"));
}

/// The release kills AND reaps: after `withdraw` returns the child is
/// gone, the kill and the exit are on the record, the table is empty, and
/// a second release is a clean no-op.
#[tokio::test]
async fn the_release_kills_reaps_and_ledgers() {
    let rig = rig();
    let handle = rig.spawn(rig.guest, "/bin/sleep", &["30"]).await;
    assert_eq!(rig.provider.live(), 1);
    rig.provider
        .withdraw(handle)
        .await
        .unwrap_or_else(|error| panic!("release: {error:?}"));
    assert_eq!(rig.provider.live(), 0);
    let kinds = rig.ledger.kinds();
    assert!(kinds.iter().any(|(kind, fiber)| matches!(
        kind,
        LedgerEventKind::ProcessKilled { handle: killed, signal } if *killed == handle && signal == "kill"
    ) && *fiber == Some(FiberId(7))));
    assert!(kinds.iter().any(|(kind, _)| matches!(
        kind,
        LedgerEventKind::ProcessExited { handle: exited, code } if *exited == handle && *code < 0
    )));
    rig.provider
        .withdraw(handle)
        .await
        .unwrap_or_else(|error| panic!("a second release is clean: {error:?}"));
    assert_eq!(
        rig.call(rig.guest, "wait", with_handle(handle, &[0; 8]))
            .await,
        Err(ErrorCode::NotFound),
        "a released handle is gone"
    );
}

/// An env allowlist passes exactly the named daemon variables plus the
/// guest's explicit pairs; the rest of the daemon's environment never
/// reaches the child.
#[tokio::test]
async fn an_env_allowlist_passes_exactly_the_named_variables() {
    let rig = rig();
    let allow = rig.broker.register_peer(Some(FiberId(9)));
    rig.broker.grant_with(
        allow,
        PROCESS_CONTRACT,
        GrantScope::Process(ProcessScope {
            exec: vec!["/usr/bin/env".into()],
            env: EnvPolicy::Allow(vec!["HOME".into()]),
        }),
    );
    assert!(std::env::var_os("HOME").is_some() && std::env::var_os("PATH").is_some());
    let payload = encode_spawn(
        "/usr/bin/env",
        &[],
        None,
        &[("JINND_GUEST_VAR".to_owned(), "yes".to_owned())],
    );
    let answer = rig
        .call(allow, "spawn", payload)
        .await
        .unwrap_or_else(|code| panic!("spawn: {code:?}"));
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&answer);
    let handle = u64::from_le_bytes(bytes);
    let listing = String::from_utf8(rig.drain(allow, handle, 0).await).unwrap_or_default();
    let names: Vec<&str> = listing
        .lines()
        .filter_map(|line| line.split('=').next())
        .collect();
    assert!(
        names.contains(&"HOME") && names.contains(&"JINND_GUEST_VAR"),
        "{listing}"
    );
    assert!(
        !names.contains(&"PATH"),
        "PATH was not allowlisted: {listing}"
    );
}

/// The one-shot `run` answers stdout under the same admission; a run past
/// its bound is killed on the record and refused — and the kill is never
/// half a story: the exit follows it on the ledger (M2-K6 round 2; Law 2),
/// attributed to the same handle.
#[tokio::test]
async fn run_answers_stdout_and_a_runaway_is_killed_and_reaped_on_the_record() {
    let rig = rig();
    let mut wire = Vec::new();
    put_segment(&mut wire, b"/bin/echo");
    put_segment(&mut wire, b"hi");
    assert_eq!(
        rig.call(rig.guest, "run", wire).await,
        Ok(b"\0hi\n".to_vec()),
        "a tagged answer: data then the bytes"
    );
    let mut wire = Vec::new();
    put_segment(&mut wire, b"/bin/sleep");
    put_segment(&mut wire, b"30");
    assert_eq!(
        rig.call(rig.guest, "run", wire).await,
        Err(ErrorCode::PluginFailed)
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let kinds = rig.ledger.kinds();
        let killed = kinds.iter().position(|(kind, _)| {
            matches!(kind, LedgerEventKind::ProcessKilled { signal, .. } if signal == "kill")
        });
        let Some(killed) = killed else {
            panic!("the runaway was not killed on the record: {kinds:?}")
        };
        let LedgerEventKind::ProcessKilled { handle, .. } = &kinds[killed].0 else {
            unreachable!()
        };
        let exited = kinds.iter().position(|(kind, _)| {
            matches!(kind, LedgerEventKind::ProcessExited { handle: exited, code }
                if exited == handle && *code < 0)
        });
        if let Some(exited) = exited {
            assert!(killed < exited, "kill precedes exit: {kinds:?}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the killed runaway's exit never landed on the ledger: {kinds:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Typed signals: `terminate` delivers SIGTERM, the exit reports the
/// negated signal, and stdin round-trips with EOF via `close-stdin`.
#[cfg(unix)]
#[tokio::test]
async fn signals_are_typed_and_stdin_round_trips() {
    let rig = rig();
    let sleeper = rig.spawn(rig.guest, "/bin/sleep", &["30"]).await;
    rig.call(rig.guest, "kill", with_handle(sleeper, &[1]))
        .await
        .unwrap_or_else(|code| panic!("kill: {code:?}"));
    assert_eq!(rig.wait(rig.guest, sleeper, 1000).await, Some(-15));

    let cat = rig.spawn(rig.guest, "/bin/cat", &[]).await;
    let accepted = rig
        .call(rig.guest, "write-stdin", with_handle(cat, b"ping\n"))
        .await
        .unwrap_or_else(|code| panic!("write: {code:?}"));
    assert_eq!(accepted, 5u32.to_le_bytes().to_vec());
    rig.call(rig.guest, "close-stdin", with_handle(cat, &[]))
        .await
        .unwrap_or_else(|code| panic!("close: {code:?}"));
    assert_eq!(rig.drain(rig.guest, cat, 0).await, b"ping\n".to_vec());
    assert_eq!(rig.wait(rig.guest, cat, 1000).await, Some(0));
    assert_eq!(
        rig.read(rig.guest, cat, 1).await.0,
        TAG_EOF,
        "stderr ended too"
    );
    assert_eq!(
        rig.call(rig.guest, "write-stdin", with_handle(cat, b"x"))
            .await,
        Err(ErrorCode::PluginFailed),
        "a closed stdin refuses"
    );
    let _ = TAG_WOULD_BLOCK;
}
