#![allow(unused)]

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};

pub const VALUE_POINTER_SIZE: usize = 4 + 4 + 8;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
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

    pub fn encode_to_array(&self) -> [u8; 16] {
        let mut buf = [0_u8; Self::encode_len()];
        self.encode_to_slice(&mut buf);
        buf
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value {
    RawValue(Bytes),
    Pointer(ValuePtr),
}

impl Value {
    pub fn is_raw_value(&self) -> bool {
        matches!(self, Value::RawValue(_))
    }

    pub fn is_pointer(&self) -> bool {
        matches!(self, Value::Pointer(_))
    }

    /// if `self` is a pointer, always return false
    pub fn is_empty(&self) -> bool {
        match self {
            Value::RawValue(bytes) => bytes.is_empty(),
            Value::Pointer(value_ptr) => false,
        }
    }

    pub fn from_raw_value(value: Bytes) -> Self {
        Self::RawValue(value)
    }

    pub fn from_ptr(ptr: ValuePtr) -> Self {
        Self::Pointer(ptr)
    }

    pub fn as_raw_value(&self) -> &Bytes {
        match self {
            Value::RawValue(bytes) => bytes,
            Value::Pointer(_) => panic!("is not a raw value"),
        }
    }

    pub fn as_value_ptr(&self) -> &ValuePtr {
        match self {
            Value::RawValue(_) => panic!("is not a pointer"),
            Value::Pointer(value_ptr) => value_ptr,
        }
    }

    pub fn from_raw_slice(value: &[u8]) -> Self {
        Self::from_raw_value(Bytes::copy_from_slice(value))
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Value::RawValue(bytes) => {
                buf.put_u8(ValueMeta::Value as u8);
                buf.extend_from_slice(bytes);
            }
            Value::Pointer(value_ptr) => {
                buf.put_u8(ValueMeta::Pointer as u8);
                value_ptr.encode(buf);
            }
        }
    }

    pub fn decode(buf: &[u8]) -> Self {
        let meta = ValueMeta::from(buf[0]);
        match meta {
            ValueMeta::Value => {
                let value = Bytes::copy_from_slice(&buf[1..]);
                Value::RawValue(value)
            }
            ValueMeta::Pointer => {
                let value_ptr = ValuePtr::decode(&buf[1..]).unwrap();
                Value::Pointer(value_ptr)
            }
        }
    }

    pub fn meta(&self) -> ValueMeta {
        match self {
            Value::RawValue(_) => ValueMeta::Value,
            Value::Pointer(_) => ValueMeta::Pointer,
        }
    }

    pub fn value_len(&self) -> usize {
        match self {
            Value::RawValue(bytes) => bytes.len(),
            Value::Pointer(value_ptr) => value_ptr.len() as usize,
        }
    }

    pub fn content_to_bytes(&self) -> Bytes {
        match self {
            Value::RawValue(bytes) => bytes.clone(),
            Value::Pointer(value_ptr) => {
                let mut buf = BytesMut::with_capacity(VALUE_POINTER_SIZE + 1);
                buf.put_u8(ValueMeta::Pointer as u8);
                value_ptr.encode(&mut buf);
                buf.freeze()
            }
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
