use std::sync::Arc;

use crate::{
    error::Result,
    format::key::KeySlice,
    kv_iter::{KvItem, KvIter, Peekable},
};

use super::{Table, block_iter::BlockIter};

pub struct TableIter {
    table: Arc<Table>,
    pub(crate) block_idx: usize,
    pub(crate) block_iter: Option<BlockIter>,

    current: Option<KvItem>,
}

impl TableIter {
    /// create a table iter, but is not valid until `seek_to_first` or `seek_to_key` is called
    pub async fn new(table: Arc<Table>) -> Result<Self> {
        Ok(Self {
            table,
            block_idx: 0,
            block_iter: None,
            current: None,
        })
    }

    fn load_current(&mut self) {
        if let Some(block_iter) = self.block_iter.as_ref()
            && block_iter.is_valid()
        {
            self.current = Some(KvItem {
                key: block_iter.key(),
                value: block_iter.value(),
            });
        } else {
            self.current = None;
        }

        tracing::debug!("current: {:?}", self.current);
    }

    pub async fn seek_to_first(&mut self) -> Result<()> {
        let iter = if self.table.block_count() > 0 {
            Some(self.table.read_block(0, false).await?.iter())
        } else {
            None
        };
        self.block_idx = 0;
        self.block_iter = iter;
        self.load_current();
        Ok(())
    }

    pub async fn seek_to_key(&mut self, key: KeySlice<'_>) -> Result<()> {
        let block_idx = self.table.find_key_in_block_index(key);
        if self.table.block_count() <= block_idx {
            self.block_idx = self.table.block_count();
            self.block_iter = None;
            return Ok(());
        }
        let mut iter = self.table.read_block(block_idx, false).await?.iter();
        iter.seek_to_key(key);
        if iter.is_valid() {
            self.block_idx = block_idx;
            self.block_iter.replace(iter);
        } else {
            self.block_idx = block_idx + 1;
            if block_idx + 1 < self.table.block_count() {
                self.block_iter
                    .replace(self.table.read_block(block_idx + 1, false).await?.iter());
            } else {
                self.block_iter = None;
            }
        }
        self.load_current();
        Ok(())
    }

    pub fn current_value_offset(&self) -> u64 {
        let block_offset = self.table.get_block_offset(self.block_idx);
        let in_block_offset = self.block_iter.as_ref().unwrap().value_offset();
        block_offset + in_block_offset
    }

    pub fn table_id(&self) -> u32 {
        self.table.id()
    }

    pub fn table(&self) -> Arc<Table> {
        self.table.clone()
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.block_idx < self.table.block_count()
            && self.block_iter.as_ref().is_some_and(|iter| iter.is_valid())
    }
}

impl KvIter for TableIter {
    async fn next(&mut self) -> Result<Option<KvItem>> {
        tracing::debug!("call table iter next, current: {:?}", self.current);

        let current = match self.current.take() {
            Some(item) => item,
            None => return Ok(None),
        };

        let block_iter = self.block_iter.as_mut().unwrap();
        block_iter.next();
        if !block_iter.is_valid() {
            self.block_idx += 1;
            if self.block_idx < self.table.block_count() {
                self.block_iter
                    .replace(self.table.read_block(self.block_idx, false).await?.iter());
            }
        }
        self.load_current();

        Ok(Some(current))
    }
}

impl Peekable for TableIter {
    fn peek(&self) -> Option<&KvItem> {
        self.current.as_ref()
    }
}

pub struct TableConcatIter {
    current: Option<TableIter>,
    next_sst_index: usize,
    sstables: Vec<Arc<Table>>,
}

impl TableConcatIter {
    /// should call `seek_to_first` or `seek_to_key` after create
    ///
    /// required table is sorted..
    pub fn new(tables: Vec<Arc<Table>>) -> Self {
        Self::check_tables_valid(&tables);

        Self {
            current: None,
            next_sst_index: 0,
            sstables: tables,
        }
    }

    pub fn value_offset_with_table(&self) -> (u64, Arc<Table>) {
        let cur = self.current.as_ref().unwrap();
        (cur.current_value_offset(), cur.table.clone())
    }

    async fn check_tables_valid(tables: &[Arc<Table>]) {
        for table in tables {
            assert!(table.first_key() <= table.last_key());
        }
        for i in 1..tables.len() {
            assert!(tables[i - 1].last_key() < tables[i].first_key());
        }
    }

    pub async fn seek_to_first(&mut self) -> Result<()> {
        if let Some(table) = self.sstables.first() {
            let iter = table.iter().await?;
            self.current.replace(iter);
            self.next_sst_index = 1;
            self.move_until_table_iter_valid().await?;
            Ok(())
        } else {
            self.current = None;
            self.next_sst_index = self.sstables.len();
            Ok(())
        }
    }

    pub async fn seek_to_key(&mut self, key: KeySlice<'_>) -> Result<()> {
        let idx = self
            .sstables
            .partition_point(|t| t.first_key().as_key_slice() <= key)
            .saturating_sub(1);

        if let Some(table) = self.sstables.get(idx) {
            self.current.replace(table.iter().await?);
            self.next_sst_index = idx + 1;
            self.move_until_table_iter_valid().await?;
        } else {
            self.current = None;
            self.next_sst_index = self.sstables.len();
        }

        Ok(())
    }

    async fn move_until_table_iter_valid(&mut self) -> Result<()> {
        while let Some(iter) = self.current.as_ref() {
            if iter.is_valid() {
                break;
            }

            if let Some(table) = self.sstables.get(self.next_sst_index) {
                self.current.replace(table.iter().await?);
                self.next_sst_index += 1;
            } else {
                self.current = None;
                self.next_sst_index = self.sstables.len();
                break;
            }
        }

        Ok(())
    }
}

impl KvIter for TableConcatIter {
    async fn next(&mut self) -> Result<Option<KvItem>> {
        self.move_until_table_iter_valid().await?;
        let item = match self.current.as_mut() {
            Some(iter) => KvIter::next(iter).await?,
            None => None,
        };

        Ok(item)
    }
}

impl Peekable for TableConcatIter {
    fn peek(&self) -> Option<&KvItem> {
        self.current.as_ref().and_then(|iter| iter.peek())
    }
}

// For testing the 'KvIter'
#[cfg(test)]
mod tests2 {
    use std::sync::Arc;

    use itertools::Itertools;

    use crate::{
        format::key::{KeyBytes, KeySlice},
        kv_iter::KvIter,
        options::DBOpenOptions,
        table::table_builder::TableBuilder,
        test_utils::{gen_key_value, run_async_test},
    };

    use super::TableConcatIter;

    #[test]
    fn test_iter_foreach() -> anyhow::Result<()> {
        run_async_test(async || {
            let tempdir = tempfile::tempdir()?;

            let options = DBOpenOptions::new()
                .block_size_threshold(100000)
                .db_path(tempdir.path())
                .build()?;
            let mut table_builder = TableBuilder::new(options);

            const ONE_BLOCK_COUNT: usize = 3;
            const COUNT: usize = ONE_BLOCK_COUNT * 1000;

            let mut block_data = (0..COUNT).map(|n| gen_key_value(n as u64, n)).collect_vec();
            for items in block_data.chunks(ONE_BLOCK_COUNT) {
                for (key, value) in items {
                    table_builder.add(key.clone(), value.clone());
                }
                table_builder.finish_block();
            }

            let table = Arc::new(table_builder.finish(0).await?);
            assert_eq!(table.block_count(), COUNT / ONE_BLOCK_COUNT);

            let mut iter = table.iter().await?;
            let mut result = vec![];
            while let Some(item) = iter.next().await? {
                result.push((item.key.clone(), item.value.clone()));
            }
            assert_eq!(block_data.len(), result.len());
            for (excepted, actual) in block_data.iter().zip(result.iter()) {
                assert_eq!(excepted, actual);
            }

            Ok(())
        })
    }

    #[test]
    fn test_flush_one_block_and_read() -> anyhow::Result<()> {
        run_async_test(async || {
            let tempdir = tempfile::tempdir()?;

            let options = DBOpenOptions::new()
                .block_size_threshold(1000000000)
                .db_path(tempdir.path())
                .build()?;
            let mut table_builder = TableBuilder::new(options);

            const COUNT: usize = 5;

            let mut block_data = (0..COUNT).map(|n| gen_key_value(n as u64, n)).collect_vec();
            for (key, value) in block_data.iter() {
                table_builder.add(key.clone(), value.clone());
            }

            let table = Arc::new(table_builder.finish(0).await?);
            assert_eq!(table.block_count(), 1);

            let mut iter = table.iter().await?;
            let mut result = vec![];
            while let Some(item) = iter.next().await? {
                result.push((item.key.clone(), item.value.clone()));
            }

            assert_eq!(block_data.len(), result.len());
            for (excepted, actual) in block_data.iter().zip(result.iter()) {
                assert_eq!(excepted, actual);
            }

            Ok(())
        })
    }

    #[test]
    fn test_iter_with_target_key() -> anyhow::Result<()> {
        run_async_test(async || {
            let tempdir = tempfile::tempdir()?;

            let options = DBOpenOptions::new()
                .block_size_threshold(100000)
                .db_path(tempdir.path())
                .build()?;
            let mut table_builder = TableBuilder::new(options.clone());

            const ONE_BLOCK_COUNT: usize = 10;
            const COUNT: usize = ONE_BLOCK_COUNT * 10; // 100 items in 10 blocks

            let block_data = (0..COUNT).map(|n| gen_key_value(n as u64, n)).collect_vec();
            for items in block_data.chunks(ONE_BLOCK_COUNT) {
                for (key, value) in items {
                    table_builder.add(key.clone(), value.clone());
                }
                table_builder.finish_block();
            }

            let table = Arc::new(table_builder.finish(0).await?);

            // Test seek to a key in the middle
            let target_idx = 55;
            let (target_key, _) = &block_data[target_idx];
            let mut iter = table
                .iter_seek_target_key(target_key.as_key_slice())
                .await?;

            let item = iter.next().await?;
            assert!(item.is_some());
            assert_eq!(item.unwrap().key, *target_key);

            // Test seeking to a key that doesn't exist but would be in a block
            let non_existent_key = KeyBytes::new(format!("key{:05}", 57).into(), 57);
            let mut iter = table
                .iter_seek_target_key(non_existent_key.as_key_slice())
                .await?;
            assert!(iter.next().await?.is_none());

            // Test seeking to a key after all existing keys
            let after_last_key = KeyBytes::new(format!("key{:05}", 1000).into(), 1919810);
            let mut iter = table
                .iter_seek_target_key(after_last_key.as_key_slice())
                .await?;
            assert!(iter.next().await?.is_none());

            Ok(())
        })
    }

    #[test]
    #[should_panic]
    fn test_build_empty() {
        run_async_test(async || -> anyhow::Result<()> {
            let tempdir = tempfile::tempdir()?;

            let options = DBOpenOptions::new()
                .block_size_threshold(100000)
                .db_path(tempdir.path())
                .build()?;

            let empty_table_builder = TableBuilder::new(options);
            let empty_table = Arc::new(empty_table_builder.finish(1).await?);
            let mut iter = empty_table
                .iter_seek_target_key(KeySlice::new(b"1233", 1))
                .await?;

            assert!(iter.next().await?.is_none());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_table_concat_iters() -> anyhow::Result<()> {
        run_async_test(async || -> anyhow::Result<()> {
            let tempdir = tempfile::tempdir()?;
            let options = DBOpenOptions::new()
                .block_size_threshold(100000)
                .db_path(tempdir.path())
                .build()?;

            let expected = (0..9).map(|i| gen_key_value(i, i as _)).collect_vec();
            let mut builder1 = TableBuilder::new(options.clone());
            let mut builder2 = TableBuilder::new(options.clone());
            let mut builder3 = TableBuilder::new(options.clone());

            for (i, (k, v)) in expected.iter().enumerate() {
                if i < 3 {
                    builder1.add(k.clone(), v.clone());
                } else if i < 6 {
                    builder2.add(k.clone(), v.clone());
                } else {
                    builder3.add(k.clone(), v.clone());
                }
            }

            let tables = vec![
                Arc::new(builder1.finish(0).await?),
                Arc::new(builder2.finish(1).await?),
                Arc::new(builder3.finish(2).await?),
            ];
            let mut citer = TableConcatIter::new(tables);
            citer.seek_to_first().await?;

            for (ek, ev) in expected.iter() {
                let item = citer.next().await?.unwrap();
                assert_eq!(item.key, *ek);
                assert_eq!(&item.value, ev);
            }

            Ok(())
        })
    }
}
