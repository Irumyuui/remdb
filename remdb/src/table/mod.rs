#![allow(unused)]

use std::sync::Arc;

use block::Block;
use meta_block::MetaBlock;

use crate::{fs::File, options::DBOptions};

pub mod block;
pub mod bloom;
pub mod meta_block;
pub mod table_builder;

pub type BlockCache = Arc<dyn remdb_utils::caches::Cache<BlockCacheKey, Block>>;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct BlockCacheKey {
    table_id: u32,
    block_offset: u32,
}

pub struct Table {
    id: u32,
    file: File,

    block_offsets: Vec<u64>,
    filter_offsets: Vec<u64>,
    table_meta: MetaBlock,

    options: Arc<DBOptions>,
}

impl Table {
    fn read_block_from_cache(&self) {

    }
}
