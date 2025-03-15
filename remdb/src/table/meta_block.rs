#![allow(unused)]

use bytes::{Buf, BufMut, BytesMut};

use crate::{
    error::{Error, Result},
    format::key::Seq,
};

pub const MAGIC_NUMBER: u64 = 0x1145141919810;

pub const META_SIZE: usize = 8 + 8 + 8 + 8 + 8 + 1 + 4 + 8;

#[derive(Debug, Clone)]
pub struct MetaBlock {
    pub(crate) blocks_start: u64, // TODO: is it needed?
    pub(crate) filters_start: u64,
    pub(crate) offsets_start: u64,
    pub(crate) block_count: u64,
    pub(crate) max_seq: Seq,
    pub(crate) compress_type: u8,
    // crc: u32,
    // magic: u64,
}

impl MetaBlock {
    pub fn encode(&self, buf: &mut BytesMut) {
        let l = buf.len();
        buf.put_u64_le(self.blocks_start);
        buf.put_u64_le(self.filters_start);
        buf.put_u64_le(self.offsets_start);
        buf.put_u64_le(self.block_count);
        buf.put_u64_le(self.max_seq);
        buf.put_u8(self.compress_type);

        // crc32
        let crc32 = crc32fast::hash(&buf[l..]);
        buf.put_u32_le(crc32);
        buf.put_u64_le(MAGIC_NUMBER);
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self> {
        if buf.len() < META_SIZE {
            return Err(Error::Decode("meta decode failed, buf too short".into()));
        }

        let prev_buf = buf;

        let blocks_start = buf.get_u64_le();
        let filters_start = buf.get_u64_le();
        let offsets_start = buf.get_u64_le();
        let block_count = buf.get_u64_le();
        let max_seq = buf.get_u64_le();
        let compress_type = buf.get_u8();

        let crc = buf.get_u32_le();

        let calc_crc = crc32fast::hash(&prev_buf[..META_SIZE - 4 - 8]);
        if calc_crc != crc {
            return Err(Error::Corruption(
                "meta decode failed, crc not match".into(),
            ));
        }

        let magic = buf.get_u64_le();
        if magic != MAGIC_NUMBER {
            return Err(Error::Corruption(
                "meta decode failed, magic number not match".into(),
            ));
        }

        Ok(Self {
            blocks_start,
            filters_start,
            offsets_start,
            block_count,
            max_seq,
            compress_type,
        })
    }
}
