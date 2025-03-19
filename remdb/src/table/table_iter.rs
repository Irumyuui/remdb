use std::sync::Arc;

use crate::{error::Result, format::key::KeySlice};

use super::{Table, block_iter::BlockIter};

pub struct TableIter {
    table: Arc<Table>,
    pub(crate) block_idx: usize,
    pub(crate) block_iter: Option<BlockIter>,
}

impl TableIter {
    /// create a table iter, but is not valid until `seek_to_first` or `seek_to_key` is called
    pub async fn new(table: Arc<Table>) -> Result<Self> {
        Ok(Self {
            table,
            block_idx: 0,
            block_iter: None,
        })
    }

    pub async fn seek_to_first(&mut self) -> Result<()> {
        let iter = if self.table.block_count() > 0 {
            Some(self.table.read_block(0).await?.iter())
        } else {
            None
        };
        self.block_idx = 0;
        self.block_iter = iter;
        Ok(())
    }

    pub async fn seek_to_key(&mut self, key: KeySlice<'_>) -> Result<()> {
        let block_idx = self.table.find_key_in_block_index(key);
        if self.table.block_count() <= block_idx {
            self.block_idx = self.table.block_count();
            self.block_iter = None;
            return Ok(());
        }
        let mut iter = self.table.read_block(block_idx).await?.iter();
        iter.seek_to_key(key);
        if iter.is_valid() {
            self.block_idx = block_idx;
            self.block_iter.replace(iter);
        } else {
            self.block_idx = block_idx + 1;
            if block_idx + 1 < self.table.block_count() {
                self.block_iter
                    .replace(self.table.read_block(block_idx + 1).await?.iter());
            } else {
                self.block_iter = None;
            }
        }

        Ok(())
    }
}

impl crate::iterator::Iter for TableIter {
    type KeyType<'a> = KeySlice<'a>;

    async fn is_valid(&self) -> bool {
        self.block_idx < self.table.block_count()
            && self.block_iter.as_ref().is_some_and(|iter| iter.is_valid())
    }

    async fn key(&self) -> Self::KeyType<'_> {
        assert!(self.is_valid().await);
        self.block_iter.as_ref().unwrap().key()
    }

    async fn value(&self) -> crate::format::value::Value {
        assert!(self.is_valid().await);
        self.block_iter.as_ref().unwrap().value()
    }

    async fn next(&mut self) -> crate::error::Result<()> {
        assert!(self.is_valid().await);

        let block_iter = self.block_iter.as_mut().unwrap();
        block_iter.next();
        if !block_iter.is_valid() {
            self.block_idx += 1;
            if self.block_idx < self.table.block_count() {
                self.block_iter
                    .replace(self.table.read_block(self.block_idx).await?.iter());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use itertools::Itertools;

    use crate::{
        format::key::KeyBytes,
        iterator::Iter,
        options::DBOpenOptions,
        table::table_builder::TableBuilder,
        test_utils::{gen_key_value, run_async_test},
    };

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
            while iter.is_valid().await {
                result.push((iter.key().await.into_key_bytes(), iter.value().await));
                iter.next().await?;
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

            const COUNT: usize = 100;

            let mut block_data = (0..COUNT).map(|n| gen_key_value(n as u64, n)).collect_vec();
            for (key, value) in block_data.iter() {
                table_builder.add(key.clone(), value.clone());
            }

            let table = Arc::new(table_builder.finish(0).await?);
            assert_eq!(table.block_count(), 1);

            let mut iter = table.iter().await?;
            let mut result = vec![];
            while iter.is_valid().await {
                result.push((iter.key().await.into_key_bytes(), iter.value().await));
                iter.next().await?;
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

            assert!(iter.is_valid().await);
            assert_eq!(iter.key().await.into_key_bytes(), *target_key);

            // Test seeking to a key that doesn't exist but would be in a block
            let non_existent_key = KeyBytes::new(format!("key{:05}", 57).into(), 57);
            let mut iter = table
                .iter_seek_target_key(non_existent_key.as_key_slice())
                .await?;

            assert!(!iter.is_valid().await);

            // Test seeking to a key after all existing keys
            let after_last_key = KeyBytes::new(format!("key{:05}", 1000).into(), 1919810);
            let mut iter = table
                .iter_seek_target_key(after_last_key.as_key_slice())
                .await?;

            assert!(!iter.is_valid().await);

            // Test seeking in empty table
            let empty_table_builder = TableBuilder::new(options);
            let empty_table = Arc::new(empty_table_builder.finish(1).await?);
            let mut iter = empty_table
                .iter_seek_target_key(target_key.as_key_slice())
                .await?;

            assert!(!iter.is_valid().await);

            Ok(())
        })
    }
}
