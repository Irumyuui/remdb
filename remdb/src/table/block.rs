#![allow(unused)]

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    error::{Error, Result},
    format::{
        key::KeyBytes,
        value::{Value, ValueMeta},
    },
};

use super::block_iter::BlockIter;

#[derive(Debug, Clone)]
pub(crate) struct Header {
    pub(crate) seq: u64,
    pub(crate) overlap: u32,
    pub(crate) diff_len: u32,
    pub(crate) value_len: u32,
    pub(crate) value_meta: ValueMeta, // u8
}

impl Header {
    pub fn emit_size(&self) -> usize {
        Self::header_size() + (self.diff_len as usize + self.value_len as usize)
    }

    pub fn header_size() -> usize {
        8 // seq 
        + 4 // overlap
        + 4 // diff_len
        + 4 // value_len
        + 1 // value_meta
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

        let seq = buf.get_u64_le();
        let overlap = buf.get_u32_le();
        let diff_len = buf.get_u32_le();
        let value_len = buf.get_u32_le();
        let value_meta = buf.get_u8();

        Ok(Self {
            seq,
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
    pub(crate) header: Header,
    pub(crate) diff_key: Bytes,
    pub(crate) value_or_ptr: Bytes,
}

impl Entry {
    pub fn value(&self) -> Value {
        Value {
            meta: self.header.value_meta,
            value_or_ptr: self.value_or_ptr.clone(),
        }
    }
}

#[derive(Default)]
pub struct BlockBuilder {
    entries: Vec<Entry>,
    base_key: Bytes,
    entires_bytes: usize,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self::default()
    }

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
            value_len: value.value_or_ptr.len() as u32,
            value_meta: value.meta,
        };
        let entry_size = header.emit_size();
        let entry = Entry {
            header,
            diff_key,
            value_or_ptr: value.value_or_ptr,
        };

        self.entries.push(entry);
        self.entires_bytes += entry_size;
    }

    pub fn block_size(&self) -> usize {
        self.entires_bytes // entries's size
        + self.entries.len() * 4 // offset's size
        + 4 // offset len
        + 4 // check sum
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

        for offset in offsets.iter() {
            buf.put_u32_le(*offset);
        }
        buf.put_u32_le(offsets.len() as u32);

        let crc32 = crc32fast::hash(&buf);
        buf.put_u32_le(crc32);

        assert_eq!(buf.len(), self.block_size());
        let data = buf.freeze();

        Block::from_raw_data(data)
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
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Block {
    pub(crate) data: Bytes, // all data
    base_key: Bytes,
}

impl Block {
    pub fn from_raw_data(data: Bytes) -> Self {
        let mut this = Self {
            data,
            base_key: Default::default(),
        };
        this.init_base_key();
        this
    }

    pub fn into_raw_data(self) -> Bytes {
        self.data
    }

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

    pub fn check_valid(&self) -> Result<()> {
        if self.data.len() < 8 {
            return Err(Error::Corruption("block data too short".into()));
        }

        let n = self.data.len();
        let check_sum = self.data[n - 4..].as_ref().get_u32_le();
        let data = self.data[..n - 4].as_ref();
        let crc32 = crc32fast::hash(data);

        if crc32 != check_sum {
            return Err(Error::Corruption("block check sum failed".into()));
        }

        Ok(())
    }

    fn init_base_key(&mut self) {
        assert!(self.entry_count() > 0);
        self.base_key = self.get_entry(0).diff_key;
    }

    pub fn base_key(&self) -> Bytes {
        self.base_key.clone()
    }

    // entry count
    pub fn entry_count(&self) -> usize {
        let n = self.data.len();
        self.data[n - 8..].as_ref().get_u32_le() as usize
    }

    fn entry_offsets(&self) -> &[u8] {
        let n = self.data.len();
        let count = self.entry_count();
        &self.data[n - 8 - count * 4..n - 8]
    }

    fn get_entry_offset(&self, idx: usize) -> usize {
        let offsets = self.entry_offsets();
        let offset = offsets[idx * 4..(idx * 4) + 4].as_ref().get_u32_le();
        offset as usize
    }

    pub fn get_entry(&self, idx: usize) -> Entry {
        assert!(idx < self.entry_count());

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

    /// create iter, but seek to first
    pub fn iter(&self) -> BlockIter {
        let mut iter = BlockIter::new(self.clone());
        iter.seek_to_first();
        iter
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockInfo {
    pub(crate) block_offset: u64,
    pub(crate) filter_offset: u64,
    pub(crate) first_key: KeyBytes,
}

impl BlockInfo {
    pub fn encode_len(&self) -> usize {
        8 + 8 + 8 + self.first_key.real_key.len()
    }

    pub fn encode(&self, buf: &mut BytesMut, hasher: Option<&mut crc32fast::Hasher>) {
        let l = buf.len();
        buf.put_u64_le(self.block_offset);
        buf.put_u64_le(self.filter_offset);
        buf.put_u64_le(self.first_key.seq());
        buf.extend_from_slice(self.first_key.key());

        if let Some(hasher) = hasher {
            hasher.update(&buf[l..]);
        }
    }

    /// required slice len
    pub fn decode(slice: Bytes) -> Self {
        let block_offset = slice[0..8].as_ref().get_u64_le();
        let filter_offset = slice[8..16].as_ref().get_u64_le();
        let seq = slice[16..24].as_ref().get_u64_le();
        let key = slice.slice(24..);

        Self {
            block_offset,
            filter_offset,
            first_key: KeyBytes::new(key, seq),
        }
    }
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
