# jinn:keystore 0.1.0

The base secret store (M2-K8; harness finding 5's remainder): `get`, `put`,
`delete`, `list` behind the same broker choke point as every provider —
grant check → ledger append → dispatch — under a `key-prefix` scope that
admits nothing on a bare grant, and an optional `ops` attenuation
(`["get", "list"]` is a read-only consumer).

## What the ledger holds

Every call is a `ContractCall` line, and the provider appends
`KeystoreAccessed { operation, key, digest }`: the key NAME and the SHA-256
of the value that crossed (`get` found, `put`), `digest` absent otherwise.
`put`/`delete` register a revertible effect labelled
`keystore <op> <key> [effect N]`. No payload, label, refusal detail, or
error message ever carries a value (02 §Redaction, class `secret`).

## The honest security boundary (v0.1)

The store is an **encrypted file** whose master key is **never under the
data root** (round-2 ruling): possession of the data root yields
ciphertext only.

- `<data>.keystore/secrets.bin` — the whole name→value map, sealed with
  ChaCha20-Poly1305 under a fresh random nonce on every commit, written by
  stage + fsync + rename (whole or absent, never torn).
- `<data>.keystore/salt` — 16 public bytes for the passphrase derivation
  below. Not a secret; useless without the passphrase.
- Retained inverses (`<data>.keystore/inverses/`) hold prior values sealed
  under the same key; a completed revert or withdrawal reclaims them.
- Values are plaintext **in the daemon's memory** while it runs, as any
  provider that answers them must be; they are never logged.

The master key comes from ONE of two sources, chosen at daemon start:

- **Passphrase** (`JINND_KEYSTORE_PASSPHRASE`, or a file named by
  `JINND_KEYSTORE_PASSPHRASE_FILE`; trailing newline ignored). A CONFIGURED
  source that cannot be read FAILS CLOSED: a variable set empty, or a file
  that is missing, unreadable, or empty, refuses daemon start with a typed
  error naming the variable and the path — it never degrades to "no source"
  or to the keychain, because a silent fall-through would seal the next
  secret under a key the operator did not choose.
  The key is derived with scrypt (N = 2^15, r = 8, p = 1) over the store's
  salt. The store is exactly as confidential as the passphrase's keeping;
  a lost passphrase loses every secret (no recovery, by design). This is
  the only source on non-macOS platforms and the operator's choice for a
  headless macOS daemon.
- **Platform keychain** (macOS default when no passphrase is set): one
  generic-password item per store path (service `jinnd keystore`), 32
  bytes from OS entropy created on first need. The item carries the
  keychain's own ACL: the same binary reads it silently; a rebuilt or
  different binary is prompted by the OS, and a locked keychain with no
  operator present REFUSES — the daemon then refuses the mutation (or the
  boot, when a document already exists) typed, never serves an empty
  store. A launchd-hosted daemon should set the passphrase instead.

The key is resolved at boot when a sealed document exists, else at the
first mutation; reads of an absent store need no key. With neither source
the first `put` is a typed `EffectFailed` naming the variables.

Out of scope (the card): rotation policies, remote stores, TLS.

## Wire (wit/plugin.wit 0.10.0)

- `get`: payload = key UTF-8; answer = the value bytes.
- `put`: payload = u32-LE key length + key + value; answer = 8-byte LE
  effect id (journaled by the calling seat, withdrawn LIFO with its trail).
- `delete`: payload = key UTF-8; answer as `put`.
- `list`: empty payload; answer = u32-LE-length-prefixed names, sorted.

Key names: non-empty UTF-8, no NUL, at most 512 bytes; anything else is
`invalid`.
