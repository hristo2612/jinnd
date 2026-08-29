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

The v0.1 backend is an **encrypted file**, on every platform:

- `<data>.keystore/secrets.bin` — the whole name→value map, sealed with
  ChaCha20-Poly1305 under a fresh random nonce on every commit, written by
  stage + fsync + rename (whole or absent, never torn).
- `<data>.keystore/master.key` — 32 bytes from OS entropy, created mode
  0600 on first boot. This is the "platform secret" the card names: the
  store is exactly as confidential as the file permissions on this key.
  An operator who moves the data root moves both files together; losing
  `master.key` loses every secret (there is no recovery, by design).
- Retained inverses (`<data>.keystore/inverses/`) hold prior values sealed
  under the same key; a completed revert or withdrawal reclaims them.
- Values are plaintext **in the daemon's memory** while it runs, as any
  provider that answers them must be; they are never logged.

The platform keychain (macOS Security framework) is **deferred**: a
launchd-hosted daemon hits keychain ACL prompts and locked-keychain
refusals with no operator present, which would make the provider's
availability depend on a GUI session — the soak's exact shape. When a
headless-safe keychain path exists it lands as a second backend behind the
same contract; nothing in this bundle changes.

Out of scope (the card): rotation policies, remote stores, TLS.

## Wire (wit/plugin.wit 0.5.0)

- `get`: payload = key UTF-8; answer = the value bytes.
- `put`: payload = u32-LE key length + key + value; answer = 8-byte LE
  effect id (journaled by the calling seat, withdrawn LIFO with its trail).
- `delete`: payload = key UTF-8; answer as `put`.
- `list`: empty payload; answer = u32-LE-length-prefixed names, sorted.

Key names: non-empty UTF-8, no NUL, at most 512 bytes; anything else is
`invalid`.
