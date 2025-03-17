#![allow(unused)]

use std::sync::Arc;

use block::Block;
use bytes::{Bytes, BytesMut};
use filter_block::FilterBlock;
use meta_block::MetaBlock;

use crate::{
    error::{Error, Result},
    fs::File,
    options::DBOptions,
};

pub mod block;
pub mod block_iter;
pub mod bloom;
pub mod filter_block;
pub mod meta_block;
pub mod table_builder;
pub mod table_iter;

pub type BlockCache = Arc<dyn remdb_utils::caches::Cache<BlockCacheKey, Bytes>>; // one block is a Bytes

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct BlockCacheKey {
    table_id: u32,
    block_offset: u64,
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
    /// maybe read a `Blcok`, or `FilterBlock`
    fn read_bytes_from_cache(&self, offset: u64) -> Option<Bytes> {
        self.options
            .table_cache
            .as_ref()
            .map(|c| {
                c.get(&BlockCacheKey {
                    table_id: self.id,
                    block_offset: offset,
                })
            })
            .flatten()
            .map(Clone::clone)
    }

    fn write_bytes_to_cache(&self, offset: u64, data: Bytes) {
        let memory_size = data.len();

        self.options.table_cache.as_ref().map(|c| {
            c.insert(
                BlockCacheKey {
                    table_id: self.id,
                    block_offset: offset,
                },
                data,
                memory_size,
            )
        });
    }

    /// return `block` and `from_cache_flag`
    async fn read_block_inner(
        &self,
        offset_idx: usize,
        start_offset: u64,
    ) -> Result<(Block, bool)> {
        if let Some(data) = self.read_bytes_from_cache(start_offset) {
            Ok((Block::from_raw_data(data), true))
        } else {
            let end_offset = self
                .block_offsets
                .get(offset_idx + 1)
                .unwrap_or(&self.table_meta.filters_start);

            let len = (end_offset - start_offset) as usize;
            let mut buf = BytesMut::zeroed(len);
            self.file.read_exact_at(&mut buf, start_offset).await?;
            let data = buf.freeze();
            assert_eq!(data.len(), len);

            let block = Block::from_raw_data(data.clone());
            if !block.is_valid() {
                return Err(Error::Corruption(
                    format!(
                        "block at offset {} is corrupted, table id: {}",
                        start_offset, self.id
                    )
                    .into(),
                ));
            }
            self.write_bytes_to_cache(start_offset, data);

            Ok((block, false))
        }
    }

    async fn read_filter_block_inner(
        &self,
        offset_idx: usize,
        start_offset: u64,
    ) -> Result<(FilterBlock, bool)> {
        if let Some(data) = self.read_bytes_from_cache(start_offset) {
            Ok((FilterBlock::from_raw_data(data), true))
        } else {
            let end_offset = self
                .block_offsets
                .get(offset_idx + 1)
                .unwrap_or(&self.table_meta.filters_start);

            let len = (end_offset - start_offset) as usize;
            let mut buf = BytesMut::zeroed(len);
            self.file.read_exact_at(&mut buf, start_offset).await?;
            let data = buf.freeze();
            assert_eq!(data.len(), len);

            let filter_block = FilterBlock::from_raw_data(data.clone());
            if !filter_block.is_valid() {
                return Err(Error::Corruption(
                    format!(
                        "block at offset {} is corrupted, table id: {}",
                        start_offset, self.id
                    )
                    .into(),
                ));
            }
            self.write_bytes_to_cache(start_offset, data);

            Ok((filter_block, false))
        }
    }

    pub async fn read_block(&self, idx: usize) -> Result<Block> {
        if let Some(&start_offset) = self.block_offsets.get(idx) {
            let (block, _from_cache) = self.read_block_inner(idx, start_offset).await?;
            Ok(block)
        } else {
            Err(Error::Corruption(
                format!(
                    "read_block invalid idx: {}, tabel id: {} max len: {}",
                    idx,
                    self.id,
                    self.block_count()
                )
                .into(),
            ))
        }
    }

    pub async fn read_filter_block(&self, idx: usize) -> Result<FilterBlock> {
        if let Some(&start_offset) = self.filter_offsets.get(idx) {
            let (filter_block, _from_cache) =
                self.read_filter_block_inner(idx, start_offset).await?;
            Ok(filter_block)
        } else {
            Err(Error::Corruption(
                format!(
                    "read_filter_block invalid idx: {}, tabel id: {} max len: {}",
                    idx,
                    self.id,
                    self.block_count()
                )
                .into(),
            ))
        }
    }

    pub fn block_count(&self) -> usize {
        self.block_offsets.len()
    }
}
