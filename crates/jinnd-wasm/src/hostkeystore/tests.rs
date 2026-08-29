//! Provider-seam pins for M2-K8 (`jinn:keystore`): the rig, and the
//! bundle's four operations under a prefix grant with the
//! value-never-on-record law (ledger, labels, errors, disk). Split by
//! seam (test-file cap soft): `authority` (bare grant, attenuation),
//! `retention` (LIFO withdrawal, witness, reopen, key names), `vault`
//! (the master key is never on the data root).

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use jinnd_api::{EffectId, ErrorCode, FiberId, LedgerEventKind, RefusalReason};

mod authority;
mod retention;
mod vault;

use super::{HostKeystore, KEYSTORE_CONTRACT, MasterKeySource};
use crate::broker::Broker;
use crate::grants::GrantScope;
use crate::hostwire::Reader;
use crate::peer::LedgerSink;

const SECRET: &[u8] = b"sk-live-0xDEADBEEF-never-on-the-record";
const OTHER: &[u8] = b"rotated-0xCAFEBABE-value";
/// The rig's operator passphrase: supplied to the provider, never under
/// its home.
const PASSPHRASE: &[u8] = b"rig-passphrase-0xFEEDFACE";

fn passphrase() -> MasterKeySource {
    MasterKeySource::Passphrase(PASSPHRASE.to_vec())
}

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
    open_with(home, ledger, passphrase()).unwrap_or_else(|error| panic!("open: {error:?}"))
}

fn open_with(
    home: &Home,
    ledger: &Arc<Recording>,
    master: MasterKeySource,
) -> Result<Arc<HostKeystore>, jinnd_api::KernelError> {
    HostKeystore::open(
        home.0.join("keystore"),
        master,
        Arc::clone(ledger) as Arc<dyn LedgerSink>,
    )
    .map(Arc::new)
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
