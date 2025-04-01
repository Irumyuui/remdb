use std::{default, sync::Arc};

use bytes::{Buf, Bytes, BytesMut};
use itertools::Itertools;

use crate::{
    error::{Error, Result},
    format::key::{KeyBuf, KeyBytes},
    fs::File,
    options::DBOptions,
    table::{
        block::BlockInfo,
        meta_block::{META_BLOCK_SIZE, MetaBlock},
    },
};

use super::Table;

pub struct TableReader {
    id: u32,
    file: File,
    opts: Arc<DBOptions>,
}

impl TableReader {
    pub fn new(id: u32, file: File, opts: Arc<DBOptions>) -> Self {
        Self { id, file, opts }
    }

    pub async fn build(self) -> Result<Table> {
        let mut buf = BytesMut::new();

        // Meta
        let file_size = self.file.len().await?;
        if file_size < META_BLOCK_SIZE as u64 {
            return Error::table_recover(format!(
                "table file too small: {}, read table meta failed, table id: {}",
                file_size, self.id
            ));
        }

        tracing::debug!("table file size: {}", file_size);

        buf.resize(META_BLOCK_SIZE, 0);
        self.file
            .read_exact_at(&mut buf, file_size - META_BLOCK_SIZE as u64)
            .await?;
        let meta = MetaBlock::decode(&buf[..])?;

        tracing::debug!("table meta: {:?}", meta);

        // Block info offsets
        let block_info_offsets_size = meta.block_count * 8 + 4;
        let block_info_offsets_end = file_size - META_BLOCK_SIZE as u64;
        let block_info_offsets_start = block_info_offsets_end - block_info_offsets_size;
        if block_info_offsets_size < 4 {
            return Error::table_recover(format!(
                "table file too small: {}, read block info offsets failed, table id: {}",
                file_size, self.id
            ));
        }

        buf.resize(block_info_offsets_size as usize, 0);
        self.file
            .read_exact_at(&mut buf, block_info_offsets_start)
            .await?;
        let expected_checksum = crc32fast::hash(&buf[..buf.len() - 4]);
        let actual_checksum = buf[buf.len() - 4..].as_ref().get_u32_le();
        if expected_checksum != actual_checksum {
            return Error::table_recover(format!(
                "block info offsets checksum not match, expected: {}, actual: {}, table id: {}",
                expected_checksum, actual_checksum, self.id
            ));
        }

        let block_info_offsets = buf[..buf.len() - 4]
            .chunks_exact(8)
            .map(|mut chunk| chunk.get_u64_le())
            .collect_vec();
        if block_info_offsets.is_empty() {
            return Error::table_recover(format!(
                "table file too small: {}, read block info offsets failed, tabel id: {}",
                file_size, self.id
            ));
        }

        // Block infos
        let block_info_end = block_info_offsets_start;
        let block_infos_start = block_info_offsets[0];
        let block_infos_size = block_info_end - block_infos_start;
        if block_infos_size < 4 {
            return Error::table_recover(format!(
                "table file too small: {}, read block infos failed, table id: {}",
                file_size, self.id
            ));
        }

        buf.resize(block_infos_size as usize, 0);
        self.file.read_exact_at(&mut buf, block_infos_start).await?;

        let expected_checksum = crc32fast::hash(&buf[..buf.len() - 4]);
        let actual_checksum = buf[buf.len() - 4..].as_ref().get_u32_le();
        if expected_checksum != actual_checksum {
            return Error::table_recover(format!(
                "block infos checksum not match, expected: {}, actual: {}, table id: {}",
                expected_checksum, actual_checksum, self.id
            ));
        }

        let buf = buf.freeze();
        let mut block_infos = Vec::with_capacity(meta.block_count as usize);
        for i in 0..block_info_offsets.len() {
            let start = block_info_offsets[i] - block_infos_start;
            let end = block_info_offsets
                .get(i + 1)
                .copied()
                .unwrap_or(block_info_end - 4)
                - block_infos_start;

            if start >= end {
                return Error::table_recover(format!(
                    "table file block info invalid, start: {}, end: {}, table id: {}",
                    start, end, self.id
                ));
            }

            tracing::debug!("block info start: {}, end: {}", start, end);

            let bytes = buf.slice(start as usize..end as usize);
            let block_info = BlockInfo::decode(bytes); // TODO: error handle
            block_infos.push(block_info);
        }

        let size = self.file.len().await?;
        let first_key = block_infos[0].first_key.clone();

        let mut table = Table {
            id: self.id,
            file: self.file,
            table_meta: meta,
            block_infos,
            options: self.opts,
            size: size as u64,
            first_key,
            last_key: KeyBytes::default(),
        };

        let block = table.read_block(table.block_count() - 1, false).await?;
        table.last_key = block.last_key();

        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, sync::Arc};

    use itertools::Itertools;

    use crate::{
        format::sst_format_path,
        kv_iter::KvIter,
        options::DBOpenOptions,
        table::{block::BlockBuilder, table_builder::TableBuilder, table_reader::TableReader},
        test_utils::{gen_key_value, run_async_test},
    };

    #[test]
    fn test_build_and_read() -> anyhow::Result<()> {
        run_async_test(async || {
            let tempdir = tempfile::tempdir()?;
            let opts = DBOpenOptions::new().db_path(tempdir.path()).build()?;
            let mut builder = TableBuilder::new(opts.clone());

            const ONE_BLOCK_COUNT: usize = 3;
            const COUNT: usize = ONE_BLOCK_COUNT * 1000;

            let mut block_data = (0..COUNT).map(|n| gen_key_value(n as u64, n)).collect_vec();

            for items in block_data.chunks(ONE_BLOCK_COUNT) {
                let mut block_builder = BlockBuilder::new();
                for (key, value) in items {
                    builder.add(key.clone(), value.clone());
                    block_builder.add_entry(key.clone(), value.clone());
                }
                builder.finish_block();
            }

            let table = Arc::new(builder.finish(0).await?);
            assert_eq!(table.block_count(), COUNT / ONE_BLOCK_COUNT);

            tracing::debug!("expected table meta: {:?}", table.table_meta);

            let mut fd_opts = OpenOptions::new();
            fd_opts.read(true).write(false).create(false);
            let file = opts
                .io_manager
                .open_file(sst_format_path(&opts.main_db_dir, 0), fd_opts)?;
            let read_table = Arc::new(TableReader::new(0, file, opts.clone()).build().await?);

            assert_eq!(table.id(), read_table.id());
            assert_eq!(table.block_count(), read_table.block_count());
            assert_eq!(table.table_meta, read_table.table_meta);
            for (expected_info, actual_info) in
                table.block_infos.iter().zip(read_table.block_infos.iter())
            {
                assert_eq!(expected_info, actual_info);
            }

            let mut expected_iter = table.iter().await?;
            let mut actual_iter = read_table.iter().await?;

            while let Some(e_item) = expected_iter.next().await? {
                let a_item = actual_iter.next().await?.unwrap();
                assert_eq!(e_item.key, a_item.key);
                assert_eq!(e_item.value.value_or_ptr, a_item.value.value_or_ptr);
            }

            Ok(())
        })
    }
}
