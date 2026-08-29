//! Provider-seam pins for M2-K8 (`jinn:keystore`): the bundle's four
//! operations under a prefix grant, typed not-found, the bare grant that
//! admits nothing, read-only attenuation, the value-never-on-record law
//! (ledger, labels, errors, disk), LIFO withdrawal from sealed inverses,
//! the keyed-revert witness, reopen from disk, and malformed key names.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use jinnd_api::{EffectId, ErrorCode, FiberId, LedgerEventKind, RefusalReason};

use super::{HostKeystore, KEYSTORE_CONTRACT};
use crate::broker::Broker;
use crate::grants::GrantScope;
use crate::hostwire::Reader;
use crate::peer::LedgerSink;

const SECRET: &[u8] = b"sk-live-0xDEADBEEF-never-on-the-record";
const OTHER: &[u8] = b"rotated-0xCAFEBABE-value";

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

    fn scope_refusals(&self, fiber: FiberId) -> usize {
        self.kinds()
            .iter()
            .filter(|(kind, by)| {
                matches!(kind, LedgerEventKind::GrantRefused { contract, reason: RefusalReason::ScopeMismatch, .. } if contract == KEYSTORE_CONTRACT)
                    && *by == Some(fiber)
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
    let root = std::env::temp_dir().join(format!("jinnd-keystore-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap_or_else(|error| panic!("{error}"));
    Home(root)
}

struct Rig {
    home: Home,
    ledger: Arc<Recording>,
    broker: Arc<Broker>,
    keystore: Arc<HostKeystore>,
    /// Granted the `engines/` prefix, fiber 7.
    guest: u64,
    /// A bare grant (no prefix), fiber 8.
    bare: u64,
    /// The `engines/` prefix, `ops: [get, list]`, fiber 9.
    reader: u64,
}

fn open(home: &Home, ledger: &Arc<Recording>) -> Arc<HostKeystore> {
    Arc::new(
        HostKeystore::open(
            home.0.join("keystore"),
            Arc::clone(ledger) as Arc<dyn LedgerSink>,
        )
        .unwrap_or_else(|error| panic!("open: {error:?}")),
    )
}

fn rig(name: &str) -> Rig {
    let home = home(name);
    let ledger = Arc::new(Recording(Mutex::new(Vec::new())));
    let broker = Arc::new(Broker::new(Arc::clone(&ledger) as Arc<dyn LedgerSink>));
    let keystore = open(&home, &ledger);
    keystore
        .register(&broker)
        .unwrap_or_else(|error| panic!("register: {error:?}"));
    let engines = || GrantScope::Keys(vec!["engines/".to_owned()]);
    let guest = broker.register_peer(Some(FiberId(7)));
    broker.grant_with(guest, KEYSTORE_CONTRACT, engines());
    let bare = broker.register_peer(Some(FiberId(8)));
    broker.grant_with(bare, KEYSTORE_CONTRACT, GrantScope::Keys(Vec::new()));
    let reader = broker.register_peer(Some(FiberId(9)));
    broker.grant_with(reader, KEYSTORE_CONTRACT, engines());
    broker.grant_ops(
        reader,
        KEYSTORE_CONTRACT,
        ["get", "list"].map(str::to_owned).to_vec(),
    );
    Rig {
        home,
        ledger,
        broker,
        keystore,
        guest,
        bare,
        reader,
    }
}

fn put_wire(key: &str, value: &[u8]) -> Vec<u8> {
    let mut wire = u32::try_from(key.len())
        .unwrap_or(u32::MAX)
        .to_le_bytes()
        .to_vec();
    wire.extend(key.as_bytes());
    wire.extend(value);
    wire
}

fn names(answer: &[u8]) -> Vec<String> {
    let mut reader = Reader::new(answer, "list");
    let mut names = Vec::new();
    while !reader.is_empty() {
        names.push(reader.text().unwrap_or_else(|error| panic!("{error:?}")));
    }
    names
}

fn effect_of(answer: &[u8]) -> EffectId {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(answer);
    EffectId(u64::from_le_bytes(bytes))
}

impl Rig {
    async fn call(&self, peer: u64, op: &str, payload: Vec<u8>) -> Result<Vec<u8>, ErrorCode> {
        self.broker
            .dispatch(peer, KEYSTORE_CONTRACT, op, payload)
            .await
            .map_err(|error| error.code)
    }

    async fn ok(&self, op: &str, payload: Vec<u8>) -> Vec<u8> {
        self.call(self.guest, op, payload)
            .await
            .unwrap_or_else(|code| panic!("{op} answers: {code:?}"))
    }

    /// Every byte on disk under the store, and every ledger record's
    /// debug rendering: where a value must never appear.
    fn on_the_record(&self) -> (Vec<u8>, String) {
        fn walk(dir: &std::path::Path, into: &mut Vec<u8>) {
            for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
                let path = entry.path();
                if path.is_dir() {
                    walk(&path, into);
                } else {
                    into.extend(std::fs::read(&path).unwrap_or_default());
                }
            }
        }
        let mut disk = Vec::new();
        walk(&self.home.0, &mut disk);
        (disk, format!("{:?}", self.ledger.kinds()))
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[tokio::test]
async fn the_bundle_round_trips_under_a_prefix_grant_and_never_records_a_value() {
    let rig = rig("bundle");
    rig.ok("put", put_wire("engines/openai", SECRET)).await;
    assert_eq!(
        rig.ok("get", b"engines/openai".to_vec()).await,
        SECRET.to_vec()
    );
    assert_eq!(
        names(&rig.ok("list", Vec::new()).await),
        vec!["engines/openai".to_owned()]
    );
    assert_eq!(
        rig.call(rig.guest, "put", put_wire("smtp/password", OTHER))
            .await,
        Err(ErrorCode::EffectFailed),
        "a key beside every granted prefix refuses"
    );
    assert_eq!(
        rig.call(rig.guest, "get", b"smtp/password".to_vec()).await,
        Err(ErrorCode::EffectFailed)
    );
    assert_eq!(rig.ledger.scope_refusals(FiberId(7)), 2);
    rig.ok("delete", b"engines/openai".to_vec()).await;
    assert_eq!(
        rig.call(rig.guest, "get", b"engines/openai".to_vec()).await,
        Err(ErrorCode::NotFound),
        "absence is the typed not-found"
    );
    assert_eq!(
        rig.call(rig.guest, "delete", b"engines/openai".to_vec())
            .await,
        Err(ErrorCode::NotFound),
        "deleting an absent key is not-found and registers nothing"
    );
    assert_eq!(rig.keystore.effects().len(), 2, "put and delete");

    // Law 2 + 02 §Redaction: the key NAME and the digest are on the
    // record; the value is nowhere — not in a ledger payload, a label, a
    // refusal detail, nor anywhere on disk (sealed store and inverses).
    let (disk, ledger) = rig.on_the_record();
    assert!(!contains(&disk, SECRET), "the value is sealed at rest");
    assert!(!ledger.contains(std::str::from_utf8(SECRET).unwrap_or("")));
    let digest = crate::sha256::hex_digest(SECRET);
    let accessed: Vec<(String, Option<String>)> = rig
        .ledger
        .kinds()
        .into_iter()
        .filter_map(|(kind, _)| match kind {
            LedgerEventKind::KeystoreAccessed {
                operation,
                key,
                digest,
            } if key == "engines/openai" => Some((operation, digest)),
            _ => None,
        })
        .collect();
    assert_eq!(
        accessed,
        vec![
            ("put".to_owned(), Some(digest.clone())),
            ("get".to_owned(), Some(digest)),
            ("delete".to_owned(), None),
            ("get".to_owned(), None),
        ],
        "name and digest per crossing, never the value"
    );
}

#[tokio::test]
async fn a_bare_grant_admits_no_key() {
    let rig = rig("bare");
    assert_eq!(
        rig.call(rig.bare, "put", put_wire("anything", SECRET))
            .await,
        Err(ErrorCode::EffectFailed)
    );
    assert_eq!(
        rig.call(rig.bare, "get", b"anything".to_vec()).await,
        Err(ErrorCode::EffectFailed)
    );
    assert!(
        names(
            &rig.call(rig.bare, "list", Vec::new())
                .await
                .unwrap_or_default()
        )
        .is_empty()
    );
    assert_eq!(rig.ledger.scope_refusals(FiberId(8)), 2);
}

#[tokio::test]
async fn a_read_only_attenuation_refuses_put_and_delete() {
    let rig = rig("read-only");
    rig.ok("put", put_wire("engines/a", SECRET)).await;
    assert_eq!(
        rig.call(rig.reader, "get", b"engines/a".to_vec()).await,
        Ok(SECRET.to_vec())
    );
    assert_eq!(
        names(
            &rig.call(rig.reader, "list", Vec::new())
                .await
                .unwrap_or_default()
        ),
        vec!["engines/a".to_owned()]
    );
    for (op, payload) in [
        ("put", put_wire("engines/a", OTHER)),
        ("delete", b"engines/a".to_vec()),
    ] {
        assert_eq!(
            rig.call(rig.reader, op, payload).await,
            Err(ErrorCode::EffectFailed),
            "{op} under a read-only grant refuses"
        );
    }
    assert_eq!(rig.ledger.scope_refusals(FiberId(9)), 2);
    assert_eq!(rig.ok("get", b"engines/a".to_vec()).await, SECRET.to_vec());
}

#[tokio::test]
async fn withdrawal_restores_each_prior_lifo_and_the_witness_attests() {
    let rig = rig("withdraw");
    let first = effect_of(&rig.ok("put", put_wire("engines/k", SECRET)).await);
    let second = effect_of(&rig.ok("put", put_wire("engines/k", OTHER)).await);
    let third = effect_of(&rig.ok("delete", b"engines/k".to_vec()).await);
    assert_eq!(rig.keystore.effects().len(), 3);
    let (witness, inverse) = rig
        .keystore
        .undo_action(third)
        .unwrap_or_else(|| panic!("revertible"));
    assert!(!witness(), "before the inverse the key is not at its prior");
    inverse()
        .await
        .unwrap_or_else(|error| panic!("inverse: {error:?}"));
    assert!(witness(), "the deleted value is back");
    rig.keystore
        .reclaim(third)
        .await
        .unwrap_or_else(|error| panic!("reclaim: {error:?}"));
    assert_eq!(rig.ok("get", b"engines/k".to_vec()).await, OTHER.to_vec());
    rig.keystore
        .withdraw(second)
        .await
        .unwrap_or_else(|error| panic!("withdraw: {error:?}"));
    assert_eq!(rig.ok("get", b"engines/k".to_vec()).await, SECRET.to_vec());
    rig.keystore
        .withdraw(first)
        .await
        .unwrap_or_else(|error| panic!("withdraw: {error:?}"));
    assert_eq!(
        rig.call(rig.guest, "get", b"engines/k".to_vec()).await,
        Err(ErrorCode::NotFound),
        "the trail withdrawn LIFO leaves prior absence"
    );
    assert!(rig.keystore.effects().is_empty());
    assert!(
        rig.keystore.withdraw(third).await.is_ok(),
        "an already-consumed effect withdraws clean"
    );
    assert!(rig.keystore.withdraw(EffectId(999)).await.is_err());
}

#[tokio::test]
async fn the_store_reopens_from_disk_with_its_journal() {
    let home = home("reopen");
    let ledger = Arc::new(Recording(Mutex::new(Vec::new())));
    {
        let broker = Arc::new(Broker::new(Arc::clone(&ledger) as Arc<dyn LedgerSink>));
        let keystore = open(&home, &ledger);
        keystore
            .register(&broker)
            .unwrap_or_else(|error| panic!("{error:?}"));
        let guest = broker.register_peer(Some(FiberId(7)));
        broker.attribute_entry(guest, "holder");
        broker.grant_with(
            guest,
            KEYSTORE_CONTRACT,
            GrantScope::Keys(vec!["e/".to_owned()]),
        );
        broker
            .dispatch(guest, KEYSTORE_CONTRACT, "put", put_wire("e/k", SECRET))
            .await
            .unwrap_or_else(|error| panic!("{error:?}"));
    }
    let reopened = open(&home, &ledger);
    let broker = Arc::new(Broker::new(Arc::clone(&ledger) as Arc<dyn LedgerSink>));
    reopened
        .register(&broker)
        .unwrap_or_else(|error| panic!("{error:?}"));
    let guest = broker.register_peer(Some(FiberId(11)));
    broker.grant_with(
        guest,
        KEYSTORE_CONTRACT,
        GrantScope::Keys(vec!["e/".to_owned()]),
    );
    assert_eq!(
        broker
            .dispatch(guest, KEYSTORE_CONTRACT, "get", b"e/k".to_vec())
            .await
            .map_err(|error| error.code),
        Ok(SECRET.to_vec()),
        "the sealed document reopens under the same master key"
    );
    let journals = reopened.journals();
    assert_eq!(journals.len(), 1);
    assert_eq!(journals[0].0, "holder");
    assert!(journals[0].1[0].label.starts_with("keystore put e/k"));
    // A tampered master key refuses the whole store (fail-closed), never
    // serves it empty.
    std::fs::write(home.0.join("keystore/master.key"), [7u8; 32])
        .unwrap_or_else(|error| panic!("{error}"));
    assert!(
        HostKeystore::open(
            home.0.join("keystore"),
            Arc::clone(&ledger) as Arc<dyn LedgerSink>
        )
        .is_err()
    );
}

#[tokio::test]
async fn malformed_key_names_are_the_typed_invalid() {
    let rig = rig("invalid");
    for key in ["", "engines/\0nul", &"x".repeat(513)] {
        assert_eq!(
            rig.call(rig.guest, "put", put_wire(key, SECRET)).await,
            Err(ErrorCode::InvalidProfile),
            "{key:?} refuses typed"
        );
    }
    assert_eq!(rig.ledger.scope_refusals(FiberId(7)), 0);
    assert!(rig.keystore.effects().is_empty());
}
