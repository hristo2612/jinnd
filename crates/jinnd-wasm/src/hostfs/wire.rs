//! The `jinn:fs` broker wire (wit/plugin.wit `interface fs`; M2-K3): the
//! request encodings the host glue writes and the provider reads, and the
//! `file-meta` answer encoding both sides share. One codec, two callers —
//! the contract files declare the shapes, this module merely binds them
//! (R12).

use jinnd_api::{ErrorCode, KernelError};

use crate::broker_state::refusal;

/// One `file-meta` record as the contract bundle declares it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileMeta {
    pub path: String,
    pub size: u64,
    pub modified_ms: u64,
    pub is_dir: bool,
}

fn malformed(what: &str) -> KernelError {
    refusal(
        ErrorCode::PluginFailed,
        format!("malformed fs {what} payload"),
    )
}

/// One u32-LE length-prefixed segment off the front of `wire`.
fn take_prefixed<'a>(wire: &'a [u8], what: &str) -> Result<(&'a [u8], &'a [u8]), KernelError> {
    let (length, rest) = wire.split_at_checked(4).ok_or_else(|| malformed(what))?;
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(length);
    let length = u32::from_le_bytes(bytes) as usize;
    rest.split_at_checked(length).ok_or_else(|| malformed(what))
}

fn take_u64<'a>(wire: &'a [u8], what: &str) -> Result<(u64, &'a [u8]), KernelError> {
    let (value, rest) = wire.split_at_checked(8).ok_or_else(|| malformed(what))?;
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(value);
    Ok((u64::from_le_bytes(bytes), rest))
}

fn utf8(bytes: &[u8], what: &str) -> Result<String, KernelError> {
    String::from_utf8(bytes.to_vec()).map_err(|_| malformed(what))
}

/// Decodes a keyed effect request (`write`/`append`/`remove`): u32-LE path
/// length, path bytes, u32-LE key length, key bytes, then the data
/// (`remove` carries none).
pub fn split_keyed(payload: &[u8]) -> Result<(String, String, Vec<u8>), KernelError> {
    let (path, rest) = take_prefixed(payload, "effect")?;
    let (key, data) = take_prefixed(rest, "effect")?;
    Ok((utf8(path, "effect")?, utf8(key, "effect")?, data.to_vec()))
}

/// Decodes a path-only request (`read`, `list`, `meta`): the path's UTF-8
/// bytes.
pub fn path_payload(payload: Vec<u8>, what: &str) -> Result<String, KernelError> {
    String::from_utf8(payload).map_err(|_| malformed(what))
}

/// Encodes `file-meta` records: per record, u32-LE path length, path
/// bytes, u64-LE size, u64-LE modified-ms, then one `is-dir` byte.
pub fn encode_metas(metas: &[FileMeta]) -> Vec<u8> {
    let mut wire = Vec::new();
    for meta in metas {
        wire.extend(
            u32::try_from(meta.path.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        wire.extend(meta.path.as_bytes());
        wire.extend(meta.size.to_le_bytes());
        wire.extend(meta.modified_ms.to_le_bytes());
        wire.push(u8::from(meta.is_dir));
    }
    wire
}

/// Decodes what [`encode_metas`] wrote.
pub fn decode_metas(mut wire: &[u8]) -> Result<Vec<FileMeta>, KernelError> {
    let mut metas = Vec::new();
    while !wire.is_empty() {
        let (path, rest) = take_prefixed(wire, "meta")?;
        let path = utf8(path, "meta")?;
        let (size, rest) = take_u64(rest, "meta")?;
        let (modified_ms, rest) = take_u64(rest, "meta")?;
        let (&is_dir, rest) = rest.split_first().ok_or_else(|| malformed("meta"))?;
        metas.push(FileMeta {
            path,
            size,
            modified_ms,
            is_dir: is_dir != 0,
        });
        wire = rest;
    }
    Ok(metas)
}

#[cfg(test)]
mod tests {
    use super::{FileMeta, decode_metas, encode_metas};

    #[test]
    fn metas_round_trip_the_wire() {
        let metas = vec![
            FileMeta {
                path: "a.txt".into(),
                size: 8,
                modified_ms: 1_700_000_000_000,
                is_dir: false,
            },
            FileMeta {
                path: "nested".into(),
                size: 0,
                modified_ms: 1,
                is_dir: true,
            },
        ];
        let decoded = decode_metas(&encode_metas(&metas))
            .unwrap_or_else(|error| panic!("round trip: {error:?}"));
        assert_eq!(decoded, metas);
        assert!(
            decode_metas(&[3, 0, 0, 0, b'a']).is_err(),
            "truncation refuses"
        );
    }
}
