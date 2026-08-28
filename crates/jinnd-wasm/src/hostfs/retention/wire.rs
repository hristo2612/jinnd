//! The spill file's wire format: tag byte, prefixed header fields, body.
//! Split from `retention.rs` by responsibility (R10 file hygiene).

use jinnd_api::{ErrorCode, KernelError};

use crate::broker_state::refusal;

use super::{Header, Prior, Record};

const TAG_ABSENT: u8 = 0;
const TAG_CONTENT: u8 = 1;
const TAG_LENGTH: u8 = 2;
/// Tag flag (M2-K4, additive): the header carries entry and operation after
/// the owner. Records without it (M2-K3) still decode, unattributed.
const TAG_ENTRY: u8 = 0x80;

pub(super) fn corrupt(id: u64, detail: &str) -> KernelError {
    refusal(
        ErrorCode::EffectFailed,
        format!("inverse record {id} unreadable: {detail}"),
    )
}

fn push_prefixed(bytes: &mut Vec<u8>, segment: &[u8]) {
    bytes.extend(
        u32::try_from(segment.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    bytes.extend(segment);
}

fn take_prefixed(bytes: &[u8]) -> Option<(String, &[u8])> {
    let (length, rest) = bytes.split_at_checked(4)?;
    let mut prefix = [0u8; 4];
    prefix.copy_from_slice(length);
    let (segment, rest) = rest.split_at_checked(u32::from_le_bytes(prefix) as usize)?;
    Some((String::from_utf8(segment.to_vec()).ok()?, rest))
}

impl Header {
    /// Wire: one tag byte, then u32-LE-prefixed label and key, then the
    /// u64-LE owner. The tag names the body that follows.
    fn encode(&self, tag: u8) -> Vec<u8> {
        let mut bytes = vec![tag | TAG_ENTRY];
        push_prefixed(&mut bytes, self.label.as_bytes());
        push_prefixed(&mut bytes, self.key.as_bytes());
        bytes.extend(self.owner.to_le_bytes());
        push_prefixed(&mut bytes, self.entry.as_bytes());
        push_prefixed(&mut bytes, self.operation.as_bytes());
        bytes
    }

    pub(super) fn decode(id: u64, bytes: &[u8]) -> Result<(u8, Self, &[u8]), KernelError> {
        let (&tag, rest) = bytes.split_first().ok_or_else(|| corrupt(id, "empty"))?;
        let (label, rest) = take_prefixed(rest).ok_or_else(|| corrupt(id, "label"))?;
        let (key, rest) = take_prefixed(rest).ok_or_else(|| corrupt(id, "key"))?;
        let (owner, mut body) = rest
            .split_at_checked(8)
            .ok_or_else(|| corrupt(id, "owner"))?;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(owner);
        let (mut entry, mut operation) = (String::new(), String::new());
        if tag & TAG_ENTRY != 0 {
            let (read_entry, rest) = take_prefixed(body).ok_or_else(|| corrupt(id, "entry"))?;
            let (read_op, rest) = take_prefixed(rest).ok_or_else(|| corrupt(id, "operation"))?;
            (entry, operation, body) = (read_entry, read_op, rest);
        }
        let header = Self {
            label,
            key,
            owner: u64::from_le_bytes(bytes),
            entry,
            operation,
        };
        Ok((tag & !TAG_ENTRY, header, body))
    }
}

impl Record {
    /// Wire: the header, then the body (nothing / the content / an 8-byte
    /// LE length).
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut bytes = self.header.encode(match self.prior {
            Prior::Absent => TAG_ABSENT,
            Prior::Content(_) => TAG_CONTENT,
            Prior::Length(_) => TAG_LENGTH,
        });
        match &self.prior {
            Prior::Absent => {}
            Prior::Content(content) => bytes.extend(content),
            Prior::Length(length) => bytes.extend(length.to_le_bytes()),
        }
        bytes
    }

    pub(crate) fn decode(id: u64, bytes: &[u8]) -> Result<Self, KernelError> {
        let (tag, header, body) = Header::decode(id, bytes)?;
        let prior = match tag {
            TAG_ABSENT => Prior::Absent,
            TAG_CONTENT => Prior::Content(body.to_vec()),
            TAG_LENGTH => {
                let mut length = [0u8; 8];
                let taken = body.get(..8).ok_or_else(|| corrupt(id, "length"))?;
                length.copy_from_slice(taken);
                Prior::Length(u64::from_le_bytes(length))
            }
            _ => return Err(corrupt(id, "tag")),
        };
        Ok(Self { header, prior })
    }
}
