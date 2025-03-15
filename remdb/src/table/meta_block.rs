#![allow(unused)]

pub const MAGIC_NUMBER: u64 = 0x1145141919810;

#[derive(Debug, Clone)]
pub struct MetaBlock {
    blocks_start: u64,
    filters_start: u64,
    offsets_start: u64,
    block_count: u64,
    // crc: u32,
    // magic: u64,
}
