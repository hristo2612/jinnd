//! The `jinn:keystore` at-rest store (M2-K8): ONE sealed document holding
//! the whole name→value map, a master key from OS entropy (mode 0600), and
//! the seal/unseal the retained inverses share. The honest boundary is
//! the bundle README's: as confidential as the master key's file
//! permissions; values are plaintext only in the daemon's memory.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use jinnd_api::{ErrorCode, KernelError};

use crate::broker_state::refusal;
use crate::hostfs::retention::commit_atomic;
use crate::hostwire::{Reader, put_segment};

const MASTER_FILE: &str = "master.key";
const STORE_FILE: &str = "secrets.bin";
/// The sealed-document magic: format 1, ChaCha20-Poly1305, 12-byte nonce.
const MAGIC: &[u8; 4] = b"JKS1";
const NONCE_LEN: usize = 12;
/// The longest key name the bundle admits (README).
pub(crate) const KEY_NAME_CAP: usize = 512;

fn failed(detail: &str, error: &std::io::Error) -> KernelError {
    refusal(
        ErrorCode::EffectFailed,
        format!("keystore {detail}: {error}"),
    )
}

fn corrupt(detail: &str) -> KernelError {
    refusal(
        ErrorCode::EffectFailed,
        format!("keystore store unreadable: {detail}"),
    )
}

/// Reads the master key, or creates it from OS entropy on first boot —
/// created mode 0600, never widened.
fn master_key(path: &Path) -> Result<[u8; 32], KernelError> {
    let mut key = [0u8; 32];
    match std::fs::read(path) {
        Ok(bytes) if bytes.len() == key.len() => {
            key.copy_from_slice(&bytes);
            return Ok(key);
        }
        Ok(_) => return Err(corrupt("master key is not 32 bytes")),
        Err(absent) if absent.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(failed("master key", &error)),
    }
    getrandom::fill(&mut key).map_err(|error| {
        refusal(
            ErrorCode::EffectFailed,
            format!("keystore entropy: {error}"),
        )
    })?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|error| failed("master key", &error))?;
    file.write_all(&key)
        .and_then(|()| file.sync_all())
        .map_err(|error| failed("master key", &error))?;
    Ok(key)
}

/// The map's plain encoding: u32-LE count, then per entry a prefixed name
/// and a prefixed value.
fn encode(entries: &BTreeMap<String, Vec<u8>>) -> Vec<u8> {
    let mut wire = u32::try_from(entries.len())
        .unwrap_or(u32::MAX)
        .to_le_bytes()
        .to_vec();
    for (name, value) in entries {
        put_segment(&mut wire, name.as_bytes());
        put_segment(&mut wire, value);
    }
    wire
}

fn decode(plain: &[u8]) -> Result<BTreeMap<String, Vec<u8>>, KernelError> {
    let mut reader = Reader::new(plain, "keystore document");
    let count = reader.u32().map_err(|_| corrupt("count"))?;
    let mut entries = BTreeMap::new();
    for _ in 0..count {
        let name = reader.text().map_err(|_| corrupt("name"))?;
        let value = reader.segment().map_err(|_| corrupt("value"))?;
        entries.insert(name, value.to_vec());
    }
    Ok(entries)
}

/// The in-memory map and its cipher; the document on disk is one sealed
/// encoding of it.
pub(crate) struct Vault {
    store: PathBuf,
    cipher: ChaCha20Poly1305,
    entries: BTreeMap<String, Vec<u8>>,
}

impl Vault {
    /// Opens (creating) the store under `dir`. Blocking — construction only.
    ///
    /// # Errors
    ///
    /// The directory or master key cannot be created or read, or the
    /// sealed document does not authenticate (fail-closed: a store that
    /// cannot be trusted is refused, never served empty).
    pub(crate) fn open(dir: &Path) -> Result<Self, KernelError> {
        std::fs::create_dir_all(dir).map_err(|error| failed("create", &error))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
        }
        let cipher = ChaCha20Poly1305::new(Key::from_slice(&master_key(&dir.join(MASTER_FILE))?));
        let store = dir.join(STORE_FILE);
        let entries = match std::fs::read(&store) {
            Ok(sealed) => decode(&unseal_with(&cipher, &sealed)?)?,
            Err(absent) if absent.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(error) => return Err(failed("open", &error)),
        };
        Ok(Self {
            store,
            cipher,
            entries,
        })
    }

    pub(crate) fn get(&self, key: &str) -> Option<&[u8]> {
        self.entries.get(key).map(Vec::as_slice)
    }

    /// Every key name, sorted.
    pub(crate) fn names(&self) -> impl Iterator<Item = &str> {
        self.entries.keys().map(String::as_str)
    }

    /// Binds `key` to `value` (or unbinds it) in memory, answering the
    /// prior binding. Nothing is on disk until [`Vault::sealed`] commits.
    pub(crate) fn set(&mut self, key: &str, value: Option<Vec<u8>>) -> Option<Vec<u8>> {
        match value {
            Some(value) => self.entries.insert(key.to_owned(), value),
            None => self.entries.remove(key),
        }
    }

    /// Seals `plain` under a fresh random nonce: magic, nonce, ciphertext.
    pub(crate) fn seal(&self, plain: &[u8]) -> Result<Vec<u8>, KernelError> {
        let mut nonce = [0u8; NONCE_LEN];
        getrandom::fill(&mut nonce).map_err(|error| {
            refusal(
                ErrorCode::EffectFailed,
                format!("keystore entropy: {error}"),
            )
        })?;
        let sealed = self
            .cipher
            .encrypt(Nonce::from_slice(&nonce), plain)
            .map_err(|_| corrupt("seal"))?;
        let mut wire = MAGIC.to_vec();
        wire.extend(nonce);
        wire.extend(sealed);
        Ok(wire)
    }

    pub(crate) fn unseal(&self, sealed: &[u8]) -> Result<Vec<u8>, KernelError> {
        unseal_with(&self.cipher, sealed)
    }

    /// The sealed document to commit, and where.
    pub(crate) fn sealed(&self) -> Result<(PathBuf, Vec<u8>), KernelError> {
        Ok((self.store.clone(), self.seal(&encode(&self.entries))?))
    }
}

fn unseal_with(cipher: &ChaCha20Poly1305, sealed: &[u8]) -> Result<Vec<u8>, KernelError> {
    let body = sealed
        .strip_prefix(MAGIC.as_slice())
        .ok_or_else(|| corrupt("magic"))?;
    let (nonce, ciphertext) = body
        .split_at_checked(NONCE_LEN)
        .ok_or_else(|| corrupt("nonce"))?;
    cipher
        .decrypt(Nonce::from_slice(nonce), ciphertext)
        .map_err(|_| corrupt("authentication failed"))
}

/// Commits one sealed document atomically (stage + fsync + rename).
pub(crate) async fn commit(target: &Path, sealed: &[u8]) -> Result<(), KernelError> {
    commit_atomic(target, sealed)
        .await
        .map_err(|error| failed("commit", &error))
}
