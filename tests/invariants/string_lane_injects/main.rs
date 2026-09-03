//! Verifier-owned M2-K24 acceptance cases. These drive the production
//! daemon and two real Tier A guests; neither the harness facade nor a
//! scheduler coin toss stands in for the string lane.

mod activation;
mod fixture;
mod harness;
mod ledger;
mod replacement;
mod visibility;
