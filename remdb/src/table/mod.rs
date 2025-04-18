#![allow(unused)]

use std::sync::Arc;

use block::{Block, BlockInfo};
use bytes::{Bytes, BytesMut};
use filter_block::FilterBlock;
use meta_block::MetaBlock;
use table_iter::TableIter;

use crate::{
    error::{KvError, KvResult},
    format::{
        key::{KeyBytes, KeySlice},
        value::ValuePtr,
    },
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
pub mod table_reader;

pub type BlockCache = Arc<dyn remdb_utils::caches::Cache<BlockCacheKey, Bytes>>; // one block is a Bytes

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct BlockCacheKey {
    table_id: u32,
    block_offset: u64,
}

pub struct Table {
    id: u32,
    file: File,

    block_infos: Vec<BlockInfo>,
    table_meta: MetaBlock,

    size: u64,

    pub(crate) first_key: KeyBytes,
    pub(crate) last_key: KeyBytes,

    options: Arc<DBOptions>,
}

impl Table {
    /// maybe read a `Blcok`, or `FilterBlock`
    fn read_bytes_from_cache(&self, offset: u64) -> Option<Bytes> {
        self.options
            .table_cache
            .as_ref()
            .and_then(|c| {
                c.get(&BlockCacheKey {
                    table_id: self.id,
                    block_offset: offset,
                })
            })
            .cloned()
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
        force_file_read: bool,
    ) -> KvResult<(Block, bool)> {
        if !force_file_read && let Some(data) = self.read_bytes_from_cache(start_offset) {
            Ok((Block::from_raw_data(data), true))
        } else {
            let end_offset = self
                .block_infos
                .get(offset_idx + 1)
                .map(|info| &info.block_offset)
                .unwrap_or(&self.table_meta.filters_start);

            let len = (end_offset - start_offset) as usize;
            let mut buf = BytesMut::zeroed(len);
            self.file.read_exact_at(&mut buf, start_offset).await?;
            let data = buf.freeze();
            assert_eq!(data.len(), len);

            let block = Block::from_raw_data(data.clone());
            if let Err(e) = block.check_valid() {
                return Err(KvError::Corruption(
                    format!(
                        "block at offset {} is corrupted, table id: {}, error: {:?}",
                        start_offset, self.id, e
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
        force_file_read: bool,
    ) -> KvResult<(FilterBlock, bool)> {
        tracing::debug!(
            "read_filter_block_inner, offset_idx: {}, start_offset: {}",
            offset_idx,
            start_offset
        );

        if !force_file_read && let Some(data) = self.read_bytes_from_cache(start_offset) {
            Ok((FilterBlock::from_raw_data(data), true))
        } else {
            let end_offset = self
                .block_infos
                .get(offset_idx + 1)
                .map(|info| &info.filter_offset)
                .unwrap_or(&self.table_meta.block_info_start);

            tracing::debug!(
                "read_filter_block_inner, end_offset: {}, start_offset: {}",
                end_offset,
                start_offset
            );

            let len = (end_offset - start_offset) as usize;
            let mut buf = BytesMut::zeroed(len);
            self.file.read_exact_at(&mut buf, start_offset).await?;
            let data = buf.freeze();
            assert_eq!(data.len(), len);

            let filter_block = FilterBlock::from_raw_data(data.clone());
            tracing::debug!(
                "filter block at offset: {}, data len: {:?}",
                start_offset,
                data.len()
            );
            if let Err(e) = filter_block.check_valid() {
                return Err(KvError::Corruption(
                    format!(
                        "filter block at offset {} is corrupted, table id: {}, err: {}",
                        start_offset, self.id, e
                    )
                    .into(),
                ));
            }
            self.write_bytes_to_cache(start_offset, data);

            Ok((filter_block, false))
        }
    }

    pub fn get_block_offset(&self, idx: usize) -> u64 {
        self.block_infos.get(idx).map(|i| i.block_offset).unwrap()
    }

    pub async fn read_block(&self, idx: usize, force_file_read: bool) -> KvResult<Block> {
        if let Some(&start_offset) = self.block_infos.get(idx).map(|i| &i.block_offset) {
            let (block, _from_cache) = self
                .read_block_inner(idx, start_offset, force_file_read)
                .await?;
            Ok(block)
        } else {
            Err(KvError::Corruption(
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

    pub async fn read_filter_block(
        &self,
        idx: usize,
        force_file_read: bool,
    ) -> KvResult<FilterBlock> {
        if let Some(&start_offset_in_fiter_block) =
            self.block_infos.get(idx).map(|i| &i.filter_offset)
        {
            let start_offset = start_offset_in_fiter_block + self.table_meta.filters_start;

            let (filter_block, _from_cache) = self
                .read_filter_block_inner(idx, start_offset, force_file_read)
                .await?;
            Ok(filter_block)
        } else {
            Err(KvError::Corruption(
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
        // tracing::debug!(
        //     "block id: {}, block infos: {:?}",
        //     self.id(),
        //     self.block_infos
        // );
        self.block_infos.len()
    }

    pub fn find_key_in_block_index(&self, key: KeySlice) -> usize {
        let res = self
            .block_infos
            .partition_point(|info| info.first_key.as_key_slice() <= key)
            .saturating_sub(1);

        tracing::debug!(
            "find_key_in_block_index, key: {:?}, res: {}, len: {}",
            key,
            res,
            self.block_count()
        );
        res
    }

    /// create a `TableIter`, will seek to first
    pub async fn iter(self: &Arc<Self>) -> KvResult<TableIter> {
        let mut iter = TableIter::new(self.clone()).await?;
        iter.seek_to_first().await?;
        Ok(iter)
    }

    pub async fn iter_seek_target_key(self: &Arc<Self>, key: KeySlice<'_>) -> KvResult<TableIter> {
        let mut iter = TableIter::new(self.clone()).await?;
        iter.seek_to_key(key).await?;
        Ok(iter)
    }

    pub async fn check_bloom_idx(self: &Arc<Self>, key: KeySlice<'_>) -> KvResult<Option<usize>> {
        for i in 0..self.block_count() {
            let filter_block = self.read_filter_block(i, false).await?;
            if filter_block.may_contains(key.key()) {
                return Ok(Some(i));
            }
        }
        Ok(None)
    }

    pub fn id(&self) -> u32 {
        self.id
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn first_key(&self) -> KeyBytes {
        self.first_key.clone()
    }

    pub fn last_key(&self) -> KeyBytes {
        self.last_key.clone()
    }

    pub async fn rewrite_value_pointer(&self, offset: u64, ptr: ValuePtr) -> KvResult<()> {
        let mut buf = [0_u8; ValuePtr::encode_len()];
        ptr.encode_to_slice(&mut buf);
        // TODO: check the offset is value ptr?
        self.file.write_all_at(&buf, offset).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;

    use crate::{
        // iterator::Iter,
        options::DBOpenOptions,
        table::block::BlockBuilder,
        test_utils::{gen_key_value, run_async_test},
    };

    use super::table_builder::TableBuilder;

    #[test]
    fn test_table_read_block() -> anyhow::Result<()> {
        run_async_test(async || {
            let tempdir = tempfile::tempdir()?;

            let options = DBOpenOptions::new()
                .block_size_threshold(100000)
                .db_path(tempdir.path())
                .build()?;
            let mut table_builder = TableBuilder::new(options);

            const ONE_BLOCK_COUNT: usize = 3;
            const COUNT: usize = ONE_BLOCK_COUNT * 1000;

            let block_data = (0..COUNT).map(|n| gen_key_value(n as u64, n)).collect_vec();
            let mut blocks = Vec::with_capacity(COUNT / ONE_BLOCK_COUNT);

            for items in block_data.chunks(ONE_BLOCK_COUNT) {
                let mut block_builder = BlockBuilder::new();
                for (key, value) in items {
                    table_builder.add(key.clone(), value.clone());
                    block_builder.add_entry(key.clone(), value.clone());
                }
                table_builder.finish_block();
                blocks.push(block_builder.finish());
            }

            let table = Arc::new(table_builder.finish(0).await?);
            assert_eq!(table.block_count(), COUNT / ONE_BLOCK_COUNT);

            let mut result = Vec::with_capacity(COUNT / ONE_BLOCK_COUNT);
            for i in 0..table.block_count() {
                let block = table.read_block(i, true).await?;
                result.push(block);
            }

            for (block, expected_block) in result.iter().zip(blocks.iter()) {
                assert_eq!(block, expected_block);
            }

            Ok(())
        })
    }
}
