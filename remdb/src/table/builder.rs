#![allow(unused)]

use std::mem;

use bytes::{Buf, BufMut};

use crate::{
    error::{Error, Result},
    value::Value,
};

struct Header {
    overlap: u16,
    diff: u16,
}

pub const HEADER_SIZE: usize = mem::size_of::<u16>() * 2;

impl Header {
    fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u16_le(self.overlap);
        buf.put_u16_le(self.diff);
    }

    fn decode(mut buf: &[u8]) -> Self {
        let overlap = buf.get_u16_le();
        let diff = buf.get_u16_le();
        Self { overlap, diff }
    }
}

/// Block format like this:
///
/// ```text
///     +---------+---------+-----+---------+---------------+-------------+-----------+
///     | Entry 1 | Entry 2 | ... | Entry n | Entry Offsets | Entries Len | Check Sum |
///     +---------+---------+-----+---------+---------------+-------------+-----------+
/// ```
///
/// Entry format like this:
///
/// ```text
///     +-------------+----------+----------+-------+
///     | overlap len | diff len | diff key | value |
///     +-------------+----------+----------+-------+
/// ```

#[derive(Debug, Default)]
struct BlockBuilder {
    data: Vec<u8>,
    base_key: Vec<u8>,
    entry_offsets: Vec<u32>,
}

impl BlockBuilder {
    fn is_empty(&self) -> bool {
        self.entry_offsets.is_empty()
    }

    fn block_size(&self) -> usize {
        self.data.len() + self.entry_offsets.len() * 4
         + 4 // len of entries
         + 4 // check sum
    }

    fn encode(&self, buf: &mut Vec<u8>) {
        let l = buf.len();

        buf.extend_from_slice(&self.data);
        for offset in &self.entry_offsets {
            buf.put_u32_le(*offset);
        }
        buf.put_u32_le(self.entry_offsets.len() as u32);

        let r = buf.len();
        let check_sum = crc32fast::hash(&buf[l..r]);
        buf.put_u32_le(check_sum);
    }

    fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < 8 {
            return Err(Error::Corruption("buf too short".into()));
        }

        let len = buf.len();
        let check_sum = buf[len - 4..].as_ref().get_u32_le();
        let entries_len = buf[len - 8..].as_ref().get_u32_le() as usize;
        if len >= 8 + entries_len * 4 + 4 {
            return Err(Error::Corruption("buf too short for entries".into()));
        }

        let mut entry_offsets = Vec::with_capacity(entries_len);
        let mut entry_offsets_buf = &buf[len - 8 - entries_len * 4..len - 8];
        for _ in 0..entries_len {
            entry_offsets.push(entry_offsets_buf.get_u32_le());
        }
        let data = buf[..len - 8 - entries_len * 4].to_vec();

        let first_key_header = Header::decode(&data[..4]);
        let base_key = data[4..4 + first_key_header.diff as usize].to_vec();

        Ok(Self {
            data,
            entry_offsets,
            base_key,
        })
    }

    fn add_entry(&mut self, header: Header, key: &[u8], value: &Value) {
        self.entry_offsets.push(self.data.len() as u32);
        let diff_key = &key[header.overlap as usize..];
        header.encode(&mut self.data);
        self.data.extend_from_slice(diff_key);
        value.encode(&mut self.data);
    }
}

/// Table format like this:
///
/// ```text
///     +---------------+
///     | Blocks        |
///     +---------------+
///     | Block offsets |
///     +---------------+
///     | Block len     |
///     +---------------+
///     | boff checksum |
///     +---------------+
/// ```
pub struct TableBuilder {
    cur_block: BlockBuilder,
    blocks: Vec<u8>,

    block_offsets: Vec<u32>,

    block_size_limit: usize, // from opitons
}

impl TableBuilder {
    fn key_diff(&self, key: &[u8]) -> (u16, u16) {
        let mut overlap = 0;
        let mut diff = 0;
        for (i, (a, b)) in self.cur_block.base_key.iter().zip(key).enumerate() {
            if a == b {
                overlap += 1;
            } else {
                diff = i as u16;
                break;
            }
        }
        (overlap, diff)
    }

    pub fn add(&mut self, key: &[u8], value: &Value) {
        if self.should_finish_block() {
            self.finish_block();
        }

        self.add_internal(key, value);
    }

    fn add_internal(&mut self, key: &[u8], value: &Value) {
        let header = if self.cur_block.entry_offsets.is_empty() {
            self.cur_block.base_key.extend_from_slice(key);
            Header {
                overlap: 0,
                diff: key.len() as u16,
            }
        } else {
            let (overlap, diff) = self.key_diff(key);
            let header = Header { overlap, diff };
            header
        };

        self.cur_block.add_entry(header, key, value);
    }

    fn should_finish_block(&self) -> bool {
        let cur_block_size = self.cur_block.block_size();
        cur_block_size >= self.block_size_limit
    }

    fn finish_block(&mut self) {
        self.block_offsets.push(self.blocks.len() as u32);
        let block = mem::replace(&mut self.cur_block, BlockBuilder::default());
        block.encode(&mut self.blocks);
    }

    pub fn finish(&mut self) -> Vec<u8> {
        if !self.cur_block.is_empty() {
            self.finish_block();
        }

        // | blocks | block_offsets | block_offsets_len | block_offsets + block_offsets_len chechsum |
        let mut table_data = self.blocks.clone();
        let l = table_data.len();

        for block_offset in &self.block_offsets {
            table_data.put_u32_le(*block_offset);
        }
        table_data.put_u32_le(self.block_offsets.len() as u32);

        let check_sum = crc32fast::hash(&table_data[l..]);
        table_data.put_u32_le(check_sum);

        table_data
    }
}
