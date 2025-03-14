#![allow(unused)]

use bytes::{Buf, BufMut, Bytes};

use crate::error::{Error, Result};

pub const VALUE_POINTER_SIZE: usize = 12;

#[derive(Debug, Clone, Default)]
pub struct ValuePtr {
    fid: u32,
    len: u32,
    offset: u32,
}

impl ValuePtr {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u32_le(self.fid);
        buf.put_u32_le(self.len);
        buf.put_u32_le(self.offset);
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self> {
        if buf.len() < VALUE_POINTER_SIZE {
            return Err(Error::Decode(
                "value ptr decode failed, buf too short".into(),
            ));
        }

        let fid = buf.get_u32_le();
        let len = buf.get_u32_le();
        let offset = buf.get_u32_le();

        Ok(Self { fid, len, offset })
    }
}

#[derive(Debug, Clone)]
pub struct Value {
    pub(crate) meta: u8, // mark it is value or value pointer
    pub(crate) value: Bytes,
}

impl Value {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u8(self.meta);
        buf.extend_from_slice(&self.value);
    }

    pub fn decode(buf: &[u8]) -> Self {
        let meta = buf[0];
        let value = Bytes::copy_from_slice(&buf[1..]);
        Self { meta, value }
    }
}
