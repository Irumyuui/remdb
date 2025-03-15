#![allow(unused)]

use std::mem;

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    format::{
        key::{KeyBytes, Seq},
        value::Value,
    },
    table::meta_block::MetaBlock,
};

use super::{
    block::{Block, BlockBuilder},
    bloom::Bloom,
};

/// ```text
///     +----------------+
///     | blocks         |
///     +----------------+
///     | blooms         |
///     +----------------+
///     | block offsets  | // u32
///     +----------------+
///     | filter offsets | // u32
///     +----------------+
///     | meta           |
///     +----------------+
/// ```
pub struct TableBuilder {
    current_block: BlockBuilder,
    entry_blocks: Vec<Block>,

    // bloom: Bloom,
    key_hashs: Vec<u32>,
    filter_blocks: Vec<u8>,
    filter_offsets: Vec<u8>,

    max_seq: Seq,

    block_size_limit: usize, // from options
                             // TODO: is it need `block_count_limit`?
}

impl TableBuilder {
    pub fn new(block_size_limit: usize) -> Self {
        Self {
            current_block: BlockBuilder::default(),
            entry_blocks: Vec::new(),

            key_hashs: Vec::new(),
            filter_blocks: Vec::new(),
            filter_offsets: Vec::new(),

            max_seq: 0,

            block_size_limit,
        }
    }

    pub fn add(&mut self, key: KeyBytes, value: Value) {
        if self.should_finish_block() {
            self.finish_block();
        }
        self.add_internal(key, value);
    }

    fn should_finish_block(&self) -> bool {
        let current_block_size = self.current_block.block_size();
        current_block_size >= self.block_size_limit
    }

    fn finish_block(&mut self) {
        let block_builder = mem::replace(&mut self.current_block, BlockBuilder::default());

        let block = block_builder.finish();
        self.entry_blocks.push(block);

        self.finish_filter_entry();
    }

    fn finish_filter_entry(&mut self) {
        let bloom = Bloom::with_size_and_false_rate(self.key_hashs.len(), 0.01);
        let filter = bloom.build_from_hashs(&self.key_hashs);
        self.key_hashs.clear();
        self.filter_offsets
            .put_u32_le(self.filter_blocks.len() as u32);

        let crc32 = crc32fast::hash(&filter);
        self.filter_blocks.extend_from_slice(&filter); // TODO: no more memory copy
        self.filter_blocks.put_u32_le(crc32);
    }

    fn add_internal(&mut self, key: KeyBytes, value: Value) {
        let key_hash = Bloom::hash(&key.key());
        self.key_hashs.push(key_hash);
        self.max_seq = self.max_seq.max(key.seq());
        self.current_block.add_entry(key, value);
    }

    // TODO: is it use file?
    pub fn finish(mut self) -> Bytes {
        if !self.key_hashs.is_empty() {
            self.finish_block();
        }

        // TODO: no more memory copy
        let mut buf = BytesMut::new();
        let mut block_offsets: Vec<u8> = Vec::with_capacity(self.entry_blocks.len() * 4);

        let block_count = self.entry_blocks.len();
        let blocks_start = buf.len();

        // Block and Filter
        for block in self.entry_blocks.iter() {
            block_offsets.put_u32_le(buf.len() as u32);
            // TODO: compress block
            buf.extend_from_slice(&block.data);
        }
        let filters_start = buf.len();
        buf.extend_from_slice(&self.filter_blocks);

        // Offsets
        let offsets_start = buf.len();
        buf.extend_from_slice(&block_offsets);
        buf.extend_from_slice(&self.filter_offsets);

        // Meta
        let max_seq = self.max_seq;
        let compress_type = 0; // TODO: compress type, current is None

        let meta = MetaBlock {
            blocks_start: blocks_start as u64,
            filters_start: filters_start as u64,
            offsets_start: offsets_start as u64,
            block_count: block_count as u64,
            max_seq,
            compress_type,
        };
        meta.encode(&mut buf);

        buf.freeze()
    }
}
