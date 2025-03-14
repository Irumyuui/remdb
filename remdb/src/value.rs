#![allow(unused)]

use bytes::{Buf, BufMut};

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
