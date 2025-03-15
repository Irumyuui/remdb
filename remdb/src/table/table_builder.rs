#![allow(unused)]

use std::mem;

use crate::format::{key::KeyBytes, value::Value};

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

    bloom: Bloom,
    key_hashs: Vec<u32>,
    filter_blocks: Vec<u8>,
    filter_offsets: Vec<u32>,

    block_size_limit: usize,
    // sst_size: usize,
}

impl TableBuilder {
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

        self.finish_filter();
    }

    fn finish_filter(&mut self) {
        let filter = self.bloom.build_from_hashs(&self.key_hashs);
        self.key_hashs.clear();
        self.filter_offsets.push(self.filter_blocks.len() as u32);
        self.filter_blocks.extend_from_slice(&filter);
    }

    fn add_internal(&mut self, key: KeyBytes, value: Value) {
        let key_hash = Bloom::hash(&key.key());

        self.key_hashs.push(key_hash);
        self.current_block.add_entry(key, value);
    }

    // TODO: is it use file?
    pub fn finish(self) {
        todo!()
    }
}
