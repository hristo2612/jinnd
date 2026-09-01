//! The `jinn:keystore` at-rest store (M2-K8): ONE sealed document holding
//! the whole name→value map under a master key that is NEVER on the data
//! root (see [`super::master`]), and the seal/unseal the retained inverses
//! share. Values are plaintext only in the daemon's memory.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use jinnd_api::{ErrorCode, KernelError};

use super::master::{MasterKeySource, entropy};
use crate::broker_state::refusal;
use crate::hostfs::retention::{commit_atomic, sweep_staged};
use crate::hostwire::{Reader, put_segment};

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
/// encoding of it. The cipher is absent until the key is first needed —
/// at open when a document exists, else at the first mutation.
pub(crate) struct Vault {
    dir: PathBuf,
    source: MasterKeySource,
    cipher: Option<ChaCha20Poly1305>,
    entries: BTreeMap<String, Vec<u8>>,
}

impl Vault {
    /// Opens (creating) the store under `dir`. Blocking — construction only.
    ///
    /// # Errors
    ///
    /// The directory cannot be created or read; or a sealed document
    /// exists and the key cannot be resolved or does not authenticate it
    /// (fail-closed: a store that cannot be trusted is refused, never
    /// served empty).
    pub(crate) fn open(dir: &Path, source: MasterKeySource) -> Result<Self, KernelError> {
        std::fs::create_dir_all(dir).map_err(|error| failed("create", &error))?;
        // The sealed document commits through the same protocol, so the
        // same crash leaves the same orphan here (M2-K19; I4).
        sweep_staged(dir);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700));
        }
        let (cipher, entries) = match std::fs::read(dir.join(STORE_FILE)) {
            Ok(sealed) => {
                let cipher = ChaCha20Poly1305::new(Key::from_slice(&source.resolve(dir)?));
                let entries = decode(&unseal_with(&cipher, &sealed)?)?;
                (Some(cipher), entries)
            }
            Err(absent) if absent.kind() == std::io::ErrorKind::NotFound => (None, BTreeMap::new()),
            Err(error) => return Err(failed("open", &error)),
        };
        Ok(Self {
            dir: dir.to_path_buf(),
            source,
            cipher,
            entries,
        })
    }

    /// Whether the key is still unresolved.
    pub(crate) fn locked(&self) -> bool {
        self.cipher.is_none()
    }

    /// What resolving the key needs, for a resolution off the lock.
    pub(crate) fn pending(&self) -> (MasterKeySource, PathBuf) {
        (self.source.clone(), self.dir.clone())
    }

    pub(crate) fn install(&mut self, key: [u8; 32]) {
        self.cipher = Some(ChaCha20Poly1305::new(Key::from_slice(&key)));
    }

    fn cipher(&self) -> Result<&ChaCha20Poly1305, KernelError> {
        self.cipher
            .as_ref()
            .ok_or_else(|| corrupt("master key not resolved"))
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
        entropy(&mut nonce)?;
        let sealed = self
            .cipher()?
            .encrypt(Nonce::from_slice(&nonce), plain)
            .map_err(|_| corrupt("seal"))?;
        let mut wire = MAGIC.to_vec();
        wire.extend(nonce);
        wire.extend(sealed);
        Ok(wire)
    }

    pub(crate) fn unseal(&self, sealed: &[u8]) -> Result<Vec<u8>, KernelError> {
        unseal_with(self.cipher()?, sealed)
    }

    /// The sealed document to commit, and where.
    pub(crate) fn sealed(&self) -> Result<(PathBuf, Vec<u8>), KernelError> {
        Ok((
            self.dir.join(STORE_FILE),
            self.seal(&encode(&self.entries))?,
        ))
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
