#![allow(unused)]

use bytes::{BufMut, Bytes};

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
