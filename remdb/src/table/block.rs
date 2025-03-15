use bytes::Bytes;

use crate::format::value::Value;

pub struct Entry {
    overlap: u16,
    diff: u16,
    diff_key: Bytes,
    value: Value,
}

pub struct Block {
    entries: Vec<Entry>,
}
