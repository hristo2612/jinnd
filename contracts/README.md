# Capability Contracts

WIT interface files — the constitution's teeth. Every service a plugin provides or
consumes is declared here; the kernel enforces exactly what is written and the ledger
records every crossing (Laws 1–2).

Format, versioning, equality semantics, and irreversibility declarations are governed
by `docs/constitution/01-capability-contracts.md`.

Rules (R12): contracts are versioned; never breaking within a major version; designed
to outlive any particular kernel implementation or host OS.

Status: empty — seeded by Constitution v0.1 (M0).
