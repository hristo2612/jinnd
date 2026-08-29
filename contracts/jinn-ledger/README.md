# jinn:ledger 0.1.0

The ledger read contract (constitution 02; finalized at M2-K7, harness
finding 20): a plugin pages the append-only stream and learns the
high-water sequence, on the record.

## Grant

A bare `"jinn:ledger"` grant; no scope type (whole-ledger read in v0.1).

## Wire

`services.resolve("jinn:ledger")`, then `services.call(handle,
"read-range", from-id u64-LE ++ limit u32-LE)` answering the JSON `page`,
or `services.call(handle, "last-seq", [])` answering u64-LE.

## Receipts

Every non-empty delivery appends `LedgerConsumed { first, last, count }`
under the reader's entry and fiber. The reader never sees its own
receipts; every other reader does. `next-from` advances past dropped rows.

## Redaction

Each event carries `sensitivity`: `public` for kernel bookkeeping,
`personal` where a payload may name a path, a command, or an operator's
text. Secret-class values are never stored (02), so nothing here needs
redacting for a device-local reader; an exporter redacts by the tag.
