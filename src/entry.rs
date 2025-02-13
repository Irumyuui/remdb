#![allow(unused)]

use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct Entry {
    pub key: Bytes,
    pub value: Bytes,
    pub(crate) meta: u8,
    pub(crate) timestamp: u64,
}
