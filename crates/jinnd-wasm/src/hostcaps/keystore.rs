//! The guest-facing `jinn:keystore` import (M2-K8; split from
//! `hostcaps.rs` by responsibility, R10): each call is one handle-less
//! broker crossing under the wire `wit/plugin.wit` declares; `put` and
//! `delete` journal their answered effect on this instance so dispose
//! withdraws it LIFO with the rest (R5). Every answer crosses as the
//! bundle's own `keystore-error` (R3, R12).

use crate::bindings;
use crate::handle::{HostRecord, Registration};
use crate::hostkeystore::{KEYSTORE_CONTRACT, keystore_label};
use crate::instance::HostState;

use super::prefixed;

/// One `jinn:keystore` revertible effect (M2-K8): the crossing, then the
/// answered effect id JOINS THIS INSTANCE'S JOURNAL under the provider's
/// label — withdrawn LIFO with the rest, through the broker's current
/// provider (R5). The label names the key, never the value.
async fn keystore_effect(
    state: &mut HostState,
    operation: &str,
    key: &str,
    payload: Vec<u8>,
) -> Result<(), bindings::keystore::KeystoreError> {
    state.admit(&format!("keystore {operation}"))?;
    let answer = state
        .seat
        .broker
        .dispatch(state.seat.peer, KEYSTORE_CONTRACT, operation, payload)
        .await?;
    let effect = crate::hostwire::decode_handle(&answer)?;
    state
        .outcome
        .registrations
        .push(Registration::Host(HostRecord {
            contract: KEYSTORE_CONTRACT.to_owned(),
            label: keystore_label(operation, key, effect),
            effect,
        }));
    Ok(())
}

impl bindings::keystore::Host for HostState {
    async fn get(&mut self, key: String) -> Result<Vec<u8>, bindings::keystore::KeystoreError> {
        Ok(self
            .seat
            .broker
            .dispatch(self.seat.peer, KEYSTORE_CONTRACT, "get", key.into_bytes())
            .await?)
    }

    async fn put(
        &mut self,
        key: String,
        value: Vec<u8>,
    ) -> Result<(), bindings::keystore::KeystoreError> {
        let payload = prefixed(&[key.as_bytes()], &value);
        keystore_effect(self, "put", &key, payload).await
    }

    async fn delete(&mut self, key: String) -> Result<(), bindings::keystore::KeystoreError> {
        keystore_effect(self, "delete", &key, key.clone().into_bytes()).await
    }

    async fn list(&mut self) -> Result<Vec<String>, bindings::keystore::KeystoreError> {
        let answer = self
            .seat
            .broker
            .dispatch(self.seat.peer, KEYSTORE_CONTRACT, "list", Vec::new())
            .await?;
        let mut reader = crate::hostwire::Reader::new(&answer, "keystore list answer");
        let mut names = Vec::new();
        while !reader.is_empty() {
            names.push(reader.text()?);
        }
        Ok(names)
    }
}
