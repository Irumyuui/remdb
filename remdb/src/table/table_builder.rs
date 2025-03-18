#![allow(unused)]

use std::{fs::OpenOptions, io, mem, sync::Arc};

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    error::Result,
    format::{
        key::{KeyBytes, Seq},
        sst_format_path,
        value::Value,
    },
    options::DBOptions,
    table::{block::BlockInfo, meta_block::MetaBlock},
};

use super::{
    Table,
    block::{Block, BlockBuilder},
    bloom::Bloom,
};

/// ```text
///     +----------------+
///     | blocks         |
///     +----------------+
///     | blooms         |
///     +----------------+
///     | block infos    |
///     +----------------+
///     | infos checksum |
///     +----------------+
///     | info offsets   | // u64
///     +----------------+
///     | ioffs checksum |
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
    filter_offsets: Vec<u64>,

    max_seq: Seq,

    options: Arc<DBOptions>,
    // block_size_limit: usize, // from options
    // TODO: is it need `block_count_limit`?
}

impl TableBuilder {
    pub fn new(opts: Arc<DBOptions>) -> Self {
        Self {
            current_block: BlockBuilder::default(),
            entry_blocks: Vec::new(),

            key_hashs: Vec::new(),
            filter_blocks: Vec::new(),
            filter_offsets: Vec::new(),

            max_seq: 0,
            options: opts,
        }
    }

    pub fn add(&mut self, key: KeyBytes, value: Value) {
        tracing::debug!("add key: {:?}, value: {:?}", key, value);

        if self.should_finish_block() {
            self.finish_block();
        }
        self.add_internal(key, value);
    }

    fn should_finish_block(&self) -> bool {
        let current_block_size = self.current_block.block_size();
        current_block_size >= self.options.block_size_threshold as usize
    }

    pub(crate) fn finish_block(&mut self) {
        let block_builder = std::mem::take(&mut self.current_block);

        let block = block_builder.finish();
        self.entry_blocks.push(block);

        self.finish_filter_entry();

        tracing::debug!("finish block, block count: {}", self.entry_blocks.len());
    }

    fn finish_filter_entry(&mut self) {
        let bloom = Bloom::with_size_and_false_rate(self.key_hashs.len(), 0.01);
        let filter = bloom.build_from_hashs(&self.key_hashs);
        self.key_hashs.clear();

        tracing::debug!(
            "finish filter entry, filter len: {}, start offset: {}",
            filter.len(),
            self.filter_blocks.len() as u64
        );
        self.filter_offsets.push(self.filter_blocks.len() as u64);

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

    pub fn current_block_count(&self) -> usize {
        self.entry_blocks.len() + if self.key_hashs.is_empty() { 0 } else { 1 }
    }

    fn finish_table_data(&self) -> (Bytes, Vec<BlockInfo>, MetaBlock) {
        // TODO: no more memory copy
        let mut buf = BytesMut::new();
        let mut block_offsets: Vec<u64> = Vec::with_capacity(self.entry_blocks.len());

        let block_count = self.entry_blocks.len();
        let blocks_start = buf.len();

        // Block and Filter
        for block in self.entry_blocks.iter() {
            block_offsets.push(buf.len() as u64);
            // TODO: compress block
            buf.extend_from_slice(&block.data);
        }
        let filters_start = buf.len();
        buf.extend_from_slice(&self.filter_blocks);

        // Block info
        assert_eq!(block_offsets.len(), self.filter_offsets.len());

        let block_info_start = buf.len();
        let mut hasher = crc32fast::Hasher::new();
        let mut block_infos = Vec::with_capacity(self.entry_blocks.len());
        let mut block_info_offsets: Vec<u64> = Vec::with_capacity(self.entry_blocks.len());
        for (&block_off, &fiter_off, first_key) in block_offsets
            .iter()
            .zip(self.filter_offsets.iter())
            .zip(self.entry_blocks.iter().map(|b| {
                let first_entry = b.get_entry(0);
                KeyBytes::new(first_entry.diff_key.clone(), first_entry.header.seq)
            }))
            .map(|((a, b), c)| (a, b, c))
        {
            let block_info = BlockInfo {
                block_offset: block_off,
                filter_offset: fiter_off,
                first_key,
            };
            block_info_offsets.push(buf.len() as u64);
            block_info.encode(&mut buf, Some(&mut hasher));
            block_infos.push(block_info);
        }
        let crc32 = hasher.finalize();
        buf.put_u32_le(crc32);

        // Block info offsets
        let mut hasher = crc32fast::Hasher::new();
        for offset in block_info_offsets.iter() {
            buf.put_u64_le(*offset);
            hasher.update(&buf[buf.len() - 8..]);
        }
        let crc32 = hasher.finalize();
        buf.put_u32_le(crc32);

        // Meta
        let max_seq = self.max_seq;
        let compress_type = 0; // TODO: compress type, current is None

        let meta = MetaBlock {
            blocks_start: blocks_start as u64,
            filters_start: filters_start as u64,
            block_info_start: block_info_start as u64,
            block_count: block_count as u64,
            max_seq,
            compress_type,
        };
        meta.encode(&mut buf);

        (buf.freeze(), block_infos, meta)
    }

    pub async fn finish(mut self, id: u32) -> Result<Table> {
        if !self.key_hashs.is_empty() {
            self.finish_block();
        }

        let (data, block_infos, meta) = self.finish_table_data();
        let path = sst_format_path(&self.options.main_db_dir, id);

        if path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("sst file {} already exists", path.display()),
            )
            .into());
        }

        let mut open_options = OpenOptions::new();
        open_options.read(true).write(true).create(true);
        let file = self.options.io_manager.open_file(path, open_options)?;
        file.write_all_at(&data, 0).await?;

        // dbg!(&block_infos);

        tracing::info!("table {} finished, fd: {:?}", id, file.fd);

        let table = Table {
            id,
            file,
            block_infos,
            table_meta: meta,

            options: self.options,
        };

        Ok(table)
    }
}
