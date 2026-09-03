//! M2-K24 acceptance through the real daemon (harness FINDINGS #7, #45,
//! #46): a wasm entry's `injects` declaration carries the typed lane's
//! semantics to the string lane. (a) A declared consumer rests `Pending`
//! until every declared provider is `Active` — both boot orders FORCED,
//! never waited for; (b) replacing a declared provider reloads its
//! consumer exactly once under `DependencyChanged`, siblings untouched;
//! (c) a `Failed` consumer re-arms when a declared provider moves and
//! never before (ruling 2, 2026-08-25); a provider withdrawn without a
//! successor parks its consumer `Pending`; an undeclared entry is
//! byte-for-byte today's; a declaration without a grant is a contained
//! entry fault; mutual declarations rest `Pending` and SAY SO on
//! `jinn:introspect` (Laws 1/2, I1/I3/I4, R1, R9, R10, R11, R12).

mod activation;
mod harness;
mod ledger;
mod replacement;
mod visibility;
