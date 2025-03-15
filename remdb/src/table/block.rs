#![allow(unused)]

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    error::{Error, Result},
    format::{
        key::{Key, KeyBytes},
        value::{Value, ValueMeta},
    },
};

struct Header {
    seq: u64,
    overlap: u32,
    diff_len: u32,
    value_len: u32,
    value_meta: ValueMeta, // u8
}

impl Header {
    pub fn emit_size(&self) -> usize {
        Self::header_size()
            + (self.overlap as usize + self.diff_len as usize + self.value_len as usize)
    }

    pub fn header_size() -> usize {
        8 + 4 + 4 + 4 + 1
    }

    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u64_le(self.seq);
        buf.put_u32_le(self.overlap);
        buf.put_u32_le(self.diff_len);
        buf.put_u32_le(self.value_len);
        buf.put_u8(self.value_meta as u8);
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self> {
        if buf.len() < Self::header_size() {
            return Err(Error::Decode("header decode failed, buf too short".into()));
        }

        let seq = buf.get_u32_le();
        let overlap = buf.get_u32_le();
        let diff_len = buf.get_u32_le();
        let value_len = buf.get_u32_le();
        let value_meta = buf.get_u8();

        Ok(Self {
            seq: seq as u64,
            overlap,
            diff_len,
            value_len,
            value_meta: ValueMeta::from(value_meta),
        })
    }
}

/// ```text
///     +---------------------+
///     | seq: u64            |
///     +---------------------+
///     | overlap len: u32    |
///     +---------------------+
///     | diff len: u32       |
///     +---------------------+
///     | value len: u32      |
///     +---------------------+
///     | value meta: u8      |
///     +---------------------+
///     | diff key            |
///     +---------------------+
///     | value or value ptr  |
///     +---------------------+
/// ```
pub struct Entry {
    header: Header,
    diff_key: Bytes,
    value_or_ptr: Bytes,
}

pub struct BlockBuilder {
    entries: Vec<Entry>,
    base_key: Bytes,
    entires_bytes: usize,
}

impl Default for BlockBuilder {
    fn default() -> Self {
        Self {
            entries: Default::default(),
            base_key: Default::default(),
            entires_bytes: 0,
        }
    }
}

impl BlockBuilder {
    pub fn add_entry(&mut self, key: KeyBytes, value: Value) {
        let (overlap, diff, diff_key) = if self.base_key.is_empty() {
            self.base_key = key.real_key.clone();
            (0, key.key_len() as u32, key.real_key.clone())
        } else {
            let (overlap, diff_key) = key_diff(&self.base_key, &key.real_key);
            (overlap, diff_key.len() as u32, diff_key)
        };

        let header = Header {
            seq: key.seq(),
            overlap,
            diff_len: diff,
            value_len: value.value.len() as u32,
            value_meta: value.meta,
        };
        let entry_size = header.emit_size();
        let entry = Entry {
            header,
            diff_key,
            value_or_ptr: value.value,
        };

        self.entries.push(entry);
        self.entires_bytes += entry_size;
    }

    pub fn block_size(&self) -> usize {
        self.entires_bytes + self.entries.len() * 4 + 4 + 4
    }

    pub fn finish(self) -> Block {
        let mut buf = BytesMut::with_capacity(self.entires_bytes);
        let mut offsets = Vec::with_capacity(self.entries.len());

        for entry in self.entries.iter() {
            offsets.push(buf.len() as u32);
            entry.header.encode(&mut buf);
            buf.extend_from_slice(&entry.diff_key);
            buf.extend_from_slice(&entry.value_or_ptr);
        }
        assert_eq!(buf.len(), self.entires_bytes);

        for offset in offsets.iter() {
            buf.put_u32_le(*offset);
        }
        buf.put_u32_le(offsets.len() as u32);

        let crc32 = crc32fast::hash(&buf);
        buf.put_u32_le(crc32);

        let data = buf.freeze();
        Block { data }
    }
}

/// ```text
///     +---------------------+
///     | entry 1             |
///     +---------------------+
///     | entry 2             |
///     +---------------------+
///     | ...                 |
///     +---------------------+
///     | entry n             |
///     +---------------------+
///     | entry 1 offset: u32 |
///     +---------------------+
///     | entry 2 offset: u32 |
///     +---------------------+
///     | ...                 |
///     +---------------------+
///     | entry n offset: u32 |
///     +---------------------+
///     | entry count: u32    |
///     +---------------------+
///     | check sum: u32      |
///     +---------------------+
/// ```
pub struct Block {
    pub(crate) data: Bytes, // all data
}

impl Block {
    pub fn is_valid(&self) -> bool {
        if self.data.len() < 8 {
            return false;
        }

        let n = self.data.len();
        let check_sum = self.data[n - 4..].as_ref().get_u32_le();

        let data = self.data[..n - 4].as_ref();
        let crc32 = crc32fast::hash(data);
        crc32 == check_sum
    }

    // entry count
    pub fn len(&self) -> usize {
        let n = self.data.len();
        self.data[n - 8..].as_ref().get_u32_le() as usize
    }

    fn entry_offsets(&self) -> &[u8] {
        let n = self.data.len();
        let count = self.len();
        &self.data[n - 8 - count * 4..n - 8]
    }

    fn get_entry_offset(&self, idx: usize) -> usize {
        let offsets = self.entry_offsets();
        let offset = offsets[idx * 4..(idx * 4) + 4].as_ref().get_u32_le();
        offset as usize
    }

    fn get_entry(&self, idx: usize) -> Entry {
        assert!(idx < self.len());
        let offset = self.get_entry_offset(idx);
        let header = Header::decode(&self.data[offset..]).unwrap();

        let diff_key = self.data.slice(
            offset + Header::header_size()
                ..offset + Header::header_size() + header.diff_len as usize,
        );
        let value = self.data.slice(
            offset + Header::header_size() + header.diff_len as usize
                ..offset
                    + Header::header_size()
                    + header.diff_len as usize
                    + header.value_len as usize,
        );

        Entry {
            header,
            diff_key,
            value_or_ptr: value,
        }
    }
}

// 记得第一个 key 为 base_key，返回 (overlap, diff_key)
fn key_diff(base_key: &Bytes, key: &Bytes) -> (u32, Bytes) {
    let mut overlap = base_key.len();
    // TODO: faseter
    for (i, (a, b)) in base_key.iter().zip(key.iter()).enumerate() {
        if a != b {
            overlap = i;
            break;
        }
    }

    (overlap as u32, key.slice(overlap..))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::key_diff;

    #[test]
    fn test_key_diff() {
        let base_key = Bytes::copy_from_slice(b"hello");
        let key = Bytes::copy_from_slice(b"hello world");
        let (overlap, diff_key) = key_diff(&base_key, &key);
        assert_eq!(overlap, 5);
        assert_eq!(diff_key, Bytes::copy_from_slice(b" world"));

        let base_key = Bytes::copy_from_slice(b"hello1");
        let key = Bytes::copy_from_slice(b"hello world");
        let (overlap, diff_key) = key_diff(&base_key, &key);
        assert_eq!(overlap, 5);
        assert_eq!(diff_key, Bytes::copy_from_slice(b" world"));
    }
}
