#![allow(unused)]

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};

pub const VALUE_POINTER_SIZE: usize = 4 + 4 + 8;

#[derive(Debug, Clone, Default)]
pub struct ValuePtr {
    pub(crate) fid: u32,
    pub(crate) len: u32, // value log entry length (with crc32)
    pub(crate) offset: u64,
}

impl ValuePtr {
    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u32_le(self.fid);
        buf.put_u32_le(self.len);
        buf.put_u64_le(self.offset);
    }

    pub fn encode_to_slice(&self, mut slice: &mut [u8]) {
        slice.put_u32_le(self.fid);
        slice.put_u32_le(self.len);
        slice.put_u64_le(self.offset);
    }

    pub fn decode(mut buf: &[u8]) -> Result<Self> {
        if buf.len() < VALUE_POINTER_SIZE {
            return Err(Error::Decode(
                "value ptr decode failed, buf too short".into(),
            ));
        }

        let fid = buf.get_u32_le();
        let len = buf.get_u32_le();
        let offset = buf.get_u64_le();

        Ok(Self { fid, len, offset })
    }

    pub fn fid(&self) -> u32 {
        self.fid
    }

    pub fn len(&self) -> u32 {
        self.len
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub const fn encode_len() -> usize {
        VALUE_POINTER_SIZE
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Value {
    pub(crate) meta: ValueMeta, // mark it is value or value pointer
    pub(crate) value_or_ptr: Bytes,
}

impl Value {
    pub fn from_raw_value(value: Bytes) -> Self {
        Self {
            meta: ValueMeta::Value,
            value_or_ptr: value,
        }
    }

    pub fn from_ptr(ptr: &ValuePtr) -> Self {
        let mut buf = BytesMut::zeroed(VALUE_POINTER_SIZE);
        ptr.encode(&mut buf);
        Self {
            meta: ValueMeta::Pointer,
            value_or_ptr: buf.freeze(),
        }
    }

    pub fn from_raw_slice(value: &[u8]) -> Self {
        Self::from_raw_value(Bytes::copy_from_slice(value))
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u8(self.meta as u8);
        buf.extend_from_slice(&self.value_or_ptr);
    }

    pub fn decode(buf: &[u8]) -> Self {
        let meta = ValueMeta::from(buf[0]);
        let value = Bytes::copy_from_slice(&buf[1..]);
        Self {
            meta,
            value_or_ptr: value,
        }
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueMeta {
    Value = 0,
    Pointer = 1,
}

impl From<u8> for ValueMeta {
    fn from(value: u8) -> Self {
        match value {
            0 => ValueMeta::Value,
            1 => ValueMeta::Pointer,
            _ => unreachable!(),
        }
    }
}

impl ValueMeta {
    pub fn is_value(&self) -> bool {
        matches!(self, ValueMeta::Value)
    }

    pub fn is_ptr(&self) -> bool {
        matches!(self, ValueMeta::Pointer)
    }
}
