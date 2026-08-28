//! Provider-seam pins for M2-K3: the full bundle's semantics, typed
//! not-found, fail-closed grants per op, post-symlink containment,
//! path-prefix scope enforcement, keyed exactly-once, the
//! durable-before-commit inverse rule, withdrawal through the peer, and the
//! finding-8 memory bound (inverses live in the spill, never in provider
//! memory).

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use jinnd_api::{EffectId, ErrorCode, FiberId, LedgerEventKind};

use super::retention::{Header, Prior, Record};
use super::scope::resolve;
use super::wire::{decode_metas, split_keyed};
use super::{FS_CONTRACT, HostFs};
use crate::broker::Broker;
use crate::peer::LedgerSink;

struct Recording(Mutex<Vec<(LedgerEventKind, Option<FiberId>)>>);

impl LedgerSink for Recording {
    fn append(&self, kind: LedgerEventKind, fiber: Option<FiberId>) {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .push((kind, fiber));
    }
}

impl Recording {
    fn kinds(&self) -> Vec<(LedgerEventKind, Option<FiberId>)> {
        self.0
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .clone()
    }

    fn grant_refusals(&self, fiber: FiberId) -> usize {
        self.kinds()
            .iter()
            .filter(|(kind, attributed)| {
                matches!(kind, LedgerEventKind::GrantRefused { contract } if contract == FS_CONTRACT)
                    && *attributed == Some(fiber)
            })
            .count()
    }
}

struct Home(PathBuf);

impl Drop for Home {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn home(name: &str) -> Home {
    let root = std::env::temp_dir().join(format!("jinnd-hostfs-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap_or_else(|error| panic!("{error}"));
    Home(root)
}

struct Rig {
    home: Home,
    ledger: Arc<Recording>,
    broker: Arc<Broker>,
    fs: Arc<HostFs>,
    /// A root-granted guest peer attributed to fiber 7.
    guest: u64,
    /// An ungranted peer, fiber 8.
    stranger: u64,
    /// A peer granted only the `/log` subtree, fiber 9.
    scoped: u64,
}

fn open(home: &Home, ledger: &Arc<Recording>) -> Arc<HostFs> {
    Arc::new(
        HostFs::open(
            home.0.join("data"),
            home.0.join("inverses"),
            Arc::clone(ledger) as Arc<dyn LedgerSink>,
        )
        .unwrap_or_else(|error| panic!("open: {error:?}")),
    )
}

fn rig(name: &str) -> Rig {
    let home = home(name);
    let ledger = Arc::new(Recording(Mutex::new(Vec::new())));
    let broker = Arc::new(Broker::new(Arc::clone(&ledger) as Arc<dyn LedgerSink>));
    let fs = open(&home, &ledger);
    fs.register(&broker)
        .unwrap_or_else(|error| panic!("register: {error:?}"));
    let guest = broker.register_peer(Some(FiberId(7)));
    broker.grant(guest, FS_CONTRACT);
    let stranger = broker.register_peer(Some(FiberId(8)));
    let scoped = broker.register_peer(Some(FiberId(9)));
    broker.grant_scoped(scoped, FS_CONTRACT, "/log");
    Rig {
        home,
        ledger,
        broker,
        fs,
        guest,
        stranger,
        scoped,
    }
}

/// The keyed effect wire (`write`/`append`/`remove`): prefixed path,
/// prefixed idempotency key, then the data.
fn keyed(path: &str, key: &str, data: &[u8]) -> Vec<u8> {
    let mut wire = Vec::new();
    for segment in [path, key] {
        wire.extend(
            u32::try_from(segment.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        wire.extend(segment.as_bytes());
    }
    wire.extend(data);
    wire
}

fn write_wire(path: &str, data: &[u8]) -> Vec<u8> {
    keyed(path, "", data)
}

fn effect_of(answer: &[u8]) -> EffectId {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(answer);
    EffectId(u64::from_le_bytes(bytes))
}

impl Rig {
    async fn call(&self, peer: u64, op: &str, payload: Vec<u8>) -> Result<Vec<u8>, ErrorCode> {
        self.broker
            .dispatch(peer, FS_CONTRACT, op, payload)
            .await
            .map_err(|error| error.code)
    }

    async fn ok(&self, op: &str, payload: Vec<u8>) -> Vec<u8> {
        self.call(self.guest, op, payload)
            .await
            .unwrap_or_else(|code| panic!("{op} answers: {code:?}"))
    }

    fn data(&self, path: &str) -> PathBuf {
        self.home.0.join("data").join(path)
    }

    async fn revert(&self, effect: EffectId) {
        let (witness, inverse) = self
            .fs
            .undo_action(effect)
            .unwrap_or_else(|| panic!("effect {} is revertible", effect.0));
        inverse()
            .await
            .unwrap_or_else(|error| panic!("inverse runs: {error:?}"));
        assert!(witness(), "the witness passes after the inverse");
        self.fs
            .reclaim(effect)
            .await
            .unwrap_or_else(|error| panic!("reclaim: {error:?}"));
    }
}

#[test]
fn resolve_scopes_paths_under_the_root() {
    let home = home("resolve");
    let root = home.0.join("data");
    std::fs::create_dir_all(root.join("nested")).unwrap_or_else(|error| panic!("{error}"));
    let root = std::fs::canonicalize(&root).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        resolve(&root, "journal.txt").unwrap_or_else(|error| panic!("scoped: {error:?}")),
        root.join("journal.txt")
    );
    assert_eq!(
        resolve(&root, "/nested/file").unwrap_or_else(|error| panic!("scoped: {error:?}")),
        root.join("nested/file"),
        "an absent file under an existing directory resolves"
    );
    assert_eq!(
        resolve(&root, "/").unwrap_or_else(|error| panic!("scoped: {error:?}")),
        root
    );
}

#[test]
fn resolve_refuses_parent_traversal() {
    let home = home("traversal");
    let root = home.0.join("data");
    std::fs::create_dir_all(&root).unwrap_or_else(|error| panic!("{error}"));
    assert!(resolve(&root, "../escape").is_err());
    assert!(resolve(&root, "a/../../escape").is_err());
    assert!(resolve(&root, "/abs/../../escape").is_err());
}

/// The verifier's round-1 probe: `data/log -> outside`. Containment is
/// decided on the fully resolved path, never lexically; an unresolvable
/// path refuses (no lexical fallback).
#[tokio::test]
async fn containment_is_decided_after_symlink_resolution() {
    let rig = rig("symlink");
    let outside = rig.home.0.join("outside");
    std::fs::create_dir_all(&outside).unwrap_or_else(|error| panic!("{error}"));
    std::os::unix::fs::symlink(&outside, rig.data("log")).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        rig.call(rig.guest, "write", write_wire("/log/a", b"escaped"))
            .await,
        Err(ErrorCode::EffectFailed),
        "a write through the link is refused"
    );
    assert!(
        !outside.join("a").exists(),
        "nothing landed outside the root"
    );
    assert_eq!(
        rig.call(rig.guest, "list", b"/log".to_vec()).await,
        Err(ErrorCode::EffectFailed),
        "reads through the link refuse too"
    );
    assert_eq!(
        rig.ledger.grant_refusals(FiberId(7)),
        2,
        "each refusal is on the record"
    );
    assert!(rig.fs.effects().is_empty());
}

/// A path-prefix grant authorizes exactly its subtree (M2-K3 retires the
/// K2-era refusal): the scoped peer reaches `/log`, nothing beside it, and
/// every scope refusal is a ledgered grant refusal with attribution.
#[tokio::test]
async fn a_path_prefix_scope_authorizes_only_its_subtree() {
    let rig = rig("scoped");
    rig.call(rig.scoped, "write", write_wire("/log/a", b"in"))
        .await
        .unwrap_or_else(|code| panic!("the scoped write admits: {code:?}"));
    assert_eq!(std::fs::read(rig.data("log/a")).ok(), Some(b"in".to_vec()));
    for (op, payload) in [
        ("write", write_wire("/other/b", b"out")),
        ("append", write_wire("/other/b", b"out")),
        ("remove", keyed("/other/b", "", b"")),
        ("read", b"/other/b".to_vec()),
        ("meta", b"/".to_vec()),
        ("list", b"/".to_vec()),
    ] {
        assert_eq!(
            rig.call(rig.scoped, op, payload).await,
            Err(ErrorCode::EffectFailed),
            "{op} outside the scope refuses"
        );
    }
    assert!(
        !rig.data("other").exists(),
        "nothing mutated outside the scope"
    );
    assert_eq!(rig.ledger.grant_refusals(FiberId(9)), 6);
    let listed = decode_metas(
        &rig.call(rig.scoped, "list", b"/log".to_vec())
            .await
            .unwrap_or_else(|code| panic!("{code:?}")),
    )
    .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(listed.len(), 1);
}

/// Constitution 03 §Act: a repeated delivery of the same idempotency key
/// returns the recorded outcome — the same effect — without applying the
/// mutation again.
#[tokio::test]
async fn a_repeated_idempotency_key_returns_the_recorded_effect() {
    let rig = rig("keyed");
    let first = effect_of(&rig.ok("write", keyed("/a", "k1", b"first")).await);
    let again = effect_of(&rig.ok("write", keyed("/a", "k1", b"second")).await);
    assert_eq!(first, again, "the recorded outcome answers the replay");
    assert_eq!(
        std::fs::read(rig.data("a")).ok(),
        Some(b"first".to_vec()),
        "the mutation did not apply again"
    );
    assert_eq!(rig.fs.effects().len(), 1);
    // Keys are the caller's: another fiber's same key is a distinct effect.
    let other = rig.broker.register_peer(Some(FiberId(10)));
    rig.broker.grant(other, FS_CONTRACT);
    let theirs = effect_of(
        &rig.call(other, "write", keyed("/a", "k1", b"theirs"))
            .await
            .unwrap_or_else(|code| panic!("{code:?}")),
    );
    assert_ne!(theirs, first);
    // An empty key claims no idempotency: every call is a fresh effect.
    let fresh = effect_of(&rig.ok("write", keyed("/a", "", b"x")).await);
    assert_ne!(fresh, first);
    assert_eq!(rig.fs.effects().len(), 3);
    // Reopened, the keys are still known (they live in the spilled header).
    let reopened = open(&rig.home, &rig.ledger);
    assert_eq!(reopened.recorded(Some(FiberId(7)), "k1"), Some(first));
}

/// The fiber's teardown path (R5, M1-P9b): withdrawing an effect through
/// the peer runs its inverse from the spill and reclaims the storage.
#[tokio::test]
async fn withdrawal_through_the_peer_runs_the_inverse_and_reclaims() {
    let rig = rig("withdraw");
    rig.ok("write", write_wire("/a", b"before")).await;
    let effect = effect_of(&rig.ok("write", write_wire("/a", b"after")).await);
    rig.broker
        .withdraw_effect(FS_CONTRACT, effect.0)
        .await
        .unwrap_or_else(|error| panic!("withdraw: {error:?}"));
    assert_eq!(std::fs::read(rig.data("a")).ok(), Some(b"before".to_vec()));
    assert_eq!(rig.fs.spilled(), 1, "exactly that inverse was reclaimed");
    assert_eq!(rig.fs.effects().len(), 1);
    // Withdrawing a consumed effect is a clean no-op; an unknown one is not.
    rig.broker
        .withdraw_effect(FS_CONTRACT, effect.0)
        .await
        .unwrap_or_else(|error| panic!("consumed withdraws clean: {error:?}"));
    assert!(rig.broker.withdraw_effect(FS_CONTRACT, 99).await.is_err());
}

#[test]
fn split_keyed_decodes_the_effect_wire() {
    let (path, key, data) =
        split_keyed(&keyed("log", "k", b"x")).unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        (path.as_str(), key.as_str(), data.as_slice()),
        ("log", "k", &b"x"[..])
    );
    assert!(split_keyed(&[1, 0]).is_err(), "truncation refuses");
}

#[test]
fn records_round_trip_their_header_and_prior() {
    let record = Record {
        header: Header {
            label: "log".into(),
            key: "k1".into(),
            owner: 7,
        },
        prior: Prior::Length(42),
    };
    assert_eq!(
        Record::decode(1, &record.encode()).unwrap_or_else(|error| panic!("{error:?}")),
        record
    );
    assert!(Record::decode(1, &[1, 0]).is_err());
}

#[tokio::test]
async fn reads_of_a_missing_path_are_the_typed_not_found() {
    let rig = rig("not-found");
    for op in ["read", "meta", "list"] {
        assert_eq!(
            rig.call(rig.guest, op, b"/missing".to_vec()).await,
            Err(ErrorCode::NotFound),
            "{op} classifies absence by code, never by message"
        );
    }
    assert_eq!(
        rig.call(rig.guest, "remove", keyed("/missing", "", b""))
            .await,
        Err(ErrorCode::NotFound)
    );
    assert_eq!(
        rig.fs.effects().len(),
        0,
        "a refused remove registers nothing"
    );
}

#[tokio::test]
async fn list_and_meta_describe_the_scoped_tree() {
    let rig = rig("list");
    rig.ok("write", write_wire("/dir/a.txt", b"12345678")).await;
    rig.ok("write", write_wire("/dir/b.txt", b"")).await;
    let listed = decode_metas(&rig.ok("list", b"/dir".to_vec()).await)
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(
        listed
            .iter()
            .map(|meta| (meta.path.as_str(), meta.size, meta.is_dir))
            .collect::<Vec<_>>(),
        vec![("a.txt", 8, false), ("b.txt", 0, false)]
    );
    let meta = decode_metas(&rig.ok("meta", b"/dir/a.txt".to_vec()).await)
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert_eq!(meta.len(), 1);
    assert_eq!((meta[0].path.as_str(), meta[0].size), ("dir/a.txt", 8));
    assert!(meta[0].modified_ms > 1_577_836_800_000, "a real mtime");
    let dir = decode_metas(&rig.ok("meta", b"/dir".to_vec()).await)
        .unwrap_or_else(|error| panic!("{error:?}"));
    assert!(dir[0].is_dir);
}

#[tokio::test]
async fn append_is_an_effect_whose_inverse_truncates_to_the_prior_length() {
    let rig = rig("append");
    rig.ok("write", write_wire("/log", b"one\n")).await;
    rig.ok("append", write_wire("/log", b"two\n")).await;
    assert_eq!(
        std::fs::read(rig.data("log")).ok(),
        Some(b"one\ntwo\n".to_vec())
    );
    let (effect, label) = rig.fs.effects()[1].clone();
    assert_eq!(label, "log");
    rig.revert(effect).await;
    assert_eq!(std::fs::read(rig.data("log")).ok(), Some(b"one\n".to_vec()));
    // Appending to an absent file: the inverse restores absence.
    rig.ok("append", write_wire("/fresh", b"x")).await;
    let (effect, _) = rig
        .fs
        .effects()
        .last()
        .cloned()
        .unwrap_or_else(|| panic!("registered"));
    rig.revert(effect).await;
    assert!(!rig.data("fresh").exists());
}

#[tokio::test]
async fn remove_is_an_effect_whose_inverse_restores_the_prior_content() {
    let rig = rig("remove");
    rig.ok("write", write_wire("/gone.txt", b"keep me")).await;
    rig.ok("remove", keyed("/gone.txt", "", b"")).await;
    assert!(!rig.data("gone.txt").exists());
    let (effect, _) = rig
        .fs
        .effects()
        .last()
        .cloned()
        .unwrap_or_else(|| panic!("registered"));
    rig.revert(effect).await;
    assert_eq!(
        std::fs::read(rig.data("gone.txt")).ok(),
        Some(b"keep me".to_vec())
    );
}

#[tokio::test]
async fn every_effect_is_ledgered_with_the_callers_attribution() {
    let rig = rig("attribution");
    rig.ok("write", write_wire("/a", b"1")).await;
    rig.ok("append", write_wire("/a", b"2")).await;
    rig.ok("remove", keyed("/a", "", b"")).await;
    let registered: Vec<(String, Option<FiberId>)> = rig
        .ledger
        .kinds()
        .into_iter()
        .filter_map(|(kind, fiber)| match kind {
            LedgerEventKind::EffectRegistered { label } => Some((label, fiber)),
            _ => None,
        })
        .collect();
    assert_eq!(registered.len(), 3);
    for (label, fiber) in &registered {
        assert_eq!(
            *fiber,
            Some(FiberId(7)),
            "attributed to the caller: {label}"
        );
    }
    assert!(registered[0].0.starts_with("fs write a [effect "));
    assert!(registered[1].0.starts_with("fs append a [effect "));
    assert!(registered[2].0.starts_with("fs remove a [effect "));
}

#[tokio::test]
async fn each_new_op_refuses_without_a_grant_on_the_record() {
    let rig = rig("ungranted");
    rig.ok("write", write_wire("/a", b"1")).await;
    for (op, payload) in [
        ("list", b"/".to_vec()),
        ("meta", b"/a".to_vec()),
        ("append", write_wire("/a", b"2")),
        ("remove", keyed("/a", "", b"")),
    ] {
        assert_eq!(
            rig.call(rig.stranger, op, payload).await,
            Err(ErrorCode::EffectFailed),
            "{op} refuses without a grant"
        );
    }
    assert_eq!(
        rig.ledger.grant_refusals(FiberId(8)),
        4,
        "every refusal is a ledger event with attribution"
    );
    assert_eq!(
        std::fs::read(rig.data("a")).ok(),
        Some(b"1".to_vec()),
        "nothing mutated"
    );
}

#[tokio::test]
async fn an_inverse_that_cannot_be_made_durable_refuses_the_effect() {
    let rig = rig("not-durable");
    rig.ok("write", write_wire("/a", b"before")).await;
    // Sabotage the store: a file where the directory was.
    let store = rig.home.0.join("inverses");
    std::fs::remove_dir_all(&store).unwrap_or_else(|error| panic!("{error}"));
    std::fs::write(&store, b"not a directory").unwrap_or_else(|error| panic!("{error}"));
    for (op, payload) in [
        ("write", write_wire("/a", b"after")),
        ("append", write_wire("/a", b"after")),
        ("remove", keyed("/a", "", b"")),
    ] {
        assert_eq!(
            rig.call(rig.guest, op, payload).await,
            Err(ErrorCode::PluginFailed),
            "{op} refuses when its inverse is not durable (a provider failure, not a denial)"
        );
    }
    assert_eq!(
        std::fs::read(rig.data("a")).ok(),
        Some(b"before".to_vec()),
        "a refused effect mutates nothing"
    );
    let recorded = rig
        .ledger
        .kinds()
        .iter()
        .filter(|(kind, fiber)| {
            matches!(kind, LedgerEventKind::ErrorRecorded { error }
                if error.message.contains("inverse not durable"))
                && *fiber == Some(FiberId(7))
        })
        .count();
    assert_eq!(recorded, 3, "each refusal is a ledgered per-entry error");
    assert_eq!(rig.fs.effects().len(), 1, "only the durable effect is live");
}

/// Finding 8: N effects do not retain N prior-contents in provider memory.
/// Proof by tamper — the inverse is read from the spill at undo time, so a
/// rewritten spill record is what the undo applies: memory held no copy.
#[tokio::test]
async fn retention_is_bounded_in_memory_and_undo_reads_the_spill() {
    let rig = rig("bounded");
    const N: usize = 48;
    const SIZE: usize = 64 * 1024;
    for i in 0..N {
        rig.ok(
            "write",
            write_wire("/big", &vec![u8::try_from(i).unwrap_or(0); SIZE]),
        )
        .await;
    }
    assert_eq!(rig.fs.effects().len(), N);
    assert_eq!(rig.fs.spilled(), N);
    assert!(
        rig.fs.index_bytes() < 8 * 1024,
        "the index holds labels, not {N} × {SIZE} bytes: {}",
        rig.fs.index_bytes()
    );
    let (effect, _) = rig
        .fs
        .effects()
        .last()
        .cloned()
        .unwrap_or_else(|| panic!("registered"));
    let spilled = rig
        .home
        .0
        .join("inverses")
        .join(format!("{}.inverse", effect.0));
    let record = Record {
        header: Header {
            label: "big".into(),
            key: String::new(),
            owner: 7,
        },
        prior: Prior::Content(b"from the spill".to_vec()),
    };
    std::fs::write(&spilled, record.encode()).unwrap_or_else(|error| panic!("{error}"));
    rig.revert(effect).await;
    assert_eq!(
        std::fs::read(rig.data("big")).ok(),
        Some(b"from the spill".to_vec()),
        "the undo came from the spill store, not from memory"
    );
    assert_eq!(
        rig.fs.spilled(),
        N - 1,
        "reclaim released exactly that inverse"
    );
    assert_eq!(rig.fs.effects().len(), N - 1);
}

#[tokio::test]
async fn reverting_every_effect_leaves_no_orphaned_inverse() {
    let rig = rig("orphans");
    rig.ok("write", write_wire("/a", b"1")).await;
    rig.ok("append", write_wire("/a", b"2")).await;
    rig.ok("remove", keyed("/a", "", b"")).await;
    for (effect, _) in rig.fs.effects().into_iter().rev() {
        rig.revert(effect).await;
    }
    assert_eq!(rig.fs.spilled(), 0);
    assert!(rig.fs.effects().is_empty());
    assert!(
        !rig.data("a").exists(),
        "LIFO replay restored prior absence"
    );
    // A consumed effect still answers — its inverse refuses to run twice.
    let consumed = EffectId(1 << 32);
    let (witness, inverse) = rig
        .fs
        .undo_action(consumed)
        .unwrap_or_else(|| panic!("consumed effects stay addressable"));
    assert!(!witness());
    assert!(inverse().await.is_err());
    assert!(
        rig.fs.undo_action(EffectId(99)).is_none(),
        "unknown stays unknown"
    );
}

#[tokio::test]
async fn a_reopened_store_rehydrates_live_inverses_and_never_reuses_an_id() {
    let rig = rig("reopen");
    rig.ok("write", write_wire("/a", b"first")).await;
    rig.ok("write", write_wire("/a", b"second")).await;
    let before = rig.fs.effects();
    assert_eq!(before[0].0, EffectId(1 << 32), "boot 0 keeps the v0.1 base");
    let reopened = open(&rig.home, &rig.ledger);
    assert_eq!(reopened.effects(), before, "the index survives a restart");
    let (witness, inverse) = reopened
        .undo_action(before[1].0)
        .unwrap_or_else(|| panic!("revertible after restart"));
    inverse().await.unwrap_or_else(|error| panic!("{error:?}"));
    assert!(witness());
    assert_eq!(std::fs::read(rig.data("a")).ok(), Some(b"first".to_vec()));
    // Fresh effects after the reopen land above every id of every prior boot.
    let broker = Arc::new(Broker::new(Arc::clone(&rig.ledger) as Arc<dyn LedgerSink>));
    reopened
        .register(&broker)
        .unwrap_or_else(|error| panic!("{error:?}"));
    let peer = broker.register_peer(None);
    broker.grant(peer, FS_CONTRACT);
    broker
        .dispatch(peer, FS_CONTRACT, "write", write_wire("/b", b"x"))
        .await
        .unwrap_or_else(|error| panic!("{error:?}"));
    let fresh = reopened
        .effects()
        .last()
        .cloned()
        .unwrap_or_else(|| panic!("registered"))
        .0;
    assert!(fresh.0 >= 2 << 32, "epoch 1 ids: {}", fresh.0);
}
