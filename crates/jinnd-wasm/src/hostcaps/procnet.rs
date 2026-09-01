//! The guest-facing `jinn:process` and `jinn:net` imports (M2-K6): each
//! call is one handle-less broker crossing — grant check → ledger append
//! → dispatch to the live provider — under the wire `wit/plugin.wit`
//! declares. A spawn, a listen, and an accept mint KERNEL REGISTRATIONS:
//! the answered handle joins THIS instance's journal so suspend and
//! dispose release it through the provider, LIFO with the rest (R5;
//! M2-K4 lifecycle class). Every answer crosses as the bundle's own
//! error variant (round 4, R3): a guest matches, never parses.
//!
//! This file is the seam the two imports SHARE — the contract names, the
//! registration label, and the two crossing shapes. Each contract's own
//! `Host` impl lives beside it in `procnet/` (`process`, `net`), split by
//! responsibility when the combined file passed R10's 300-line cap.

use crate::handle::{HostRecord, Registration};
use crate::hostwire::{Reader, decode_handle};
use crate::instance::HostState;
use jinnd_api::KernelError;

mod net;
mod process;

/// The process provider's contract name.
pub const PROCESS_CONTRACT: &str = "jinn:process";
/// The net provider's contract name.
pub const NET_CONTRACT: &str = "jinn:net";

/// The Law-2 label one kernel registration is journaled and released
/// under, shared by the provider's ledger line and the seat's withdrawal.
#[must_use]
pub fn registration_label(contract: &str, what: &str, handle: u64) -> String {
    format!("{contract} {what} [handle {handle}]")
}

/// One crossing, facade-typed: each import converts at its boundary into
/// its bundle's error (`From` in `bindings`), so the guest sees the variant.
async fn crossing(
    state: &HostState,
    contract: &str,
    operation: &str,
    payload: Vec<u8>,
) -> Result<Vec<u8>, KernelError> {
    state
        .seat
        .broker
        .dispatch(state.seat.peer, contract, operation, payload)
        .await
}

/// One registering crossing: admitted into the journal first (a sealed
/// seat refuses on the record), dispatched, then the answered handle
/// joins the journal under its label.
async fn registering(
    state: &mut HostState,
    contract: &str,
    operation: &str,
    what: &str,
    payload: Vec<u8>,
) -> Result<u64, KernelError> {
    state.admit(&format!("{contract} {operation}"))?;
    let answer = crossing(state, contract, operation, payload).await?;
    let handle = decode_handle(&answer)?;
    state
        .outcome
        .registrations
        .push(Registration::Kernel(HostRecord {
            contract: contract.to_owned(),
            label: registration_label(contract, what, handle),
            effect: handle,
        }));
    Ok(handle)
}

fn handle_payload(handle: u64, tail: &[u8]) -> Vec<u8> {
    let mut wire = handle.to_le_bytes().to_vec();
    wire.extend(tail);
    wire
}

fn read_answer(answer: &[u8]) -> Result<(u8, Vec<u8>), KernelError> {
    let mut reader = Reader::new(answer, "read answer");
    let tag = reader.u8()?;
    Ok((tag, reader.rest().to_vec()))
}

fn count_answer(answer: &[u8]) -> Result<u32, KernelError> {
    Reader::new(answer, "count answer").u32()
}
